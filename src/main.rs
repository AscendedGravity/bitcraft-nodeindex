mod config;
mod subscription;
mod database;
mod sse;
use crate::{config::*, subscription::*, sse::*};

use std::sync::Arc;
use bindings::{sdk::DbContext, region::*, ext::ctx::*};
use anyhow::Result;
use axum::{Router, Json, routing::get, http::StatusCode, extract::{Path, State}};
use axum::http::{HeaderValue, Method};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, mpsc::unbounded_channel};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};
use chrono;

#[tokio::main]
async fn main() {
    let config = match AppConfig::from("config.json") {
        Ok(config) => config,
        Err(err) => {
            error!("could not set config: {:#}", err);
            return;
        }
    };

    // Configure logging with levels from config.json or environment
    let logging_level = &config.server.logging_level;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    // Use config-based log level with database module at warn to suppress debug noise
                    let filter_string = format!("{},nodeindex::database=warn", logging_level);
                    tracing_subscriber::EnvFilter::new(filter_string)
                })
        )
        .init();

    let (state, db_config, server_config) = config.build();

    let mut queries = Vec::new();
    if !state.enemy.is_empty() { queries.push(Query::ENEMY) }
    for id in state.resource.keys() { queries.push(Query::RESOURCE(*id)) }
    if !state.player.is_empty() { queries.push(Query::PLAYER) }

    // Create SSE manager with default configuration
    let sse_manager = SseManager::default();
    let app_state_with_sse = AppStateWithSse {
        app_state: state.clone(),
        sse_manager: sse_manager.clone(),
    };

    let sub = QueueSub::with(queries)
        .on_success(|| {
            info!("active!");
        })
        .on_error(|ctx, err| {
            error!("db error while subscribing: {:?}", err);
            ctx.disconnect().unwrap();
        });

    let (tx, rx) = unbounded_channel();
    let (tx_sig, rx_sig) = oneshot::channel();

    let con = DbConnection::builder()
        .configure(&db_config)
        .on_connect(|ctx, _, _| {
            info!("connected!");
            ctx.subscribe(sub);
        })
        .on_disconnect(move |_, _| {
            info!("disconnected!");
            tx_sig.send(()).unwrap();
        })
        .with_light_mode(true)
        .with_channel(tx)
        .build()
        .unwrap();

    tokio::spawn(database::consume_with_sse(rx, state.clone(), sse_manager.clone()));

    let (con, server) = tokio::join!(
        tokio::spawn(con.run_until(tokio::signal::ctrl_c())),
        tokio::spawn(server(rx_sig, server_config, Arc::new(app_state_with_sse))),
    );

    if let Ok(Err(e)) = con { error!("db error: {:#}", e); }
    if let Ok(Err(e)) = server { error!("server error: {:#}", e); }
}

async fn server(rx: oneshot::Receiver<()>, config: ServerConfig, state: Arc<AppStateWithSse<AppState>>) -> Result<()> {
    let mut app = Router::new()
        .route("/resource/{id}", get(route_resource_id))
        .route("/enemy/{id}", get(route_enemy_id))
        .route("/player/{id}", get(route_player_id))
        .route("/health", get(route_health))
        .route("/resources", get(route_resources))
        .route("/enemies", get(route_enemies))
        .route("/players", get(route_players))
        .merge(create_sse_router())  // Add SSE routes via the SSE module
        .layer(CompressionLayer::new().gzip(true).zstd(true))
        .with_state(state);

    if !config.cors_origin.is_empty() {
        let cors = CorsLayer::new()
            .allow_origin([HeaderValue::from_str(&config.cors_origin)?])
            .allow_methods([Method::GET, Method::OPTIONS])
            .allow_headers(Any);

        app = app.layer(cors);
    }

    let addr = config.socket_addr;
    let listener = TcpListener::bind(addr).await?;

    info!("server listening on {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async { rx.await.unwrap(); })
        .await?;

    Ok(())
}

async fn route_resource_id(
    Path(id): Path<i32>,
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Some(resource) = state.app_state.resource.get(&id) else {
        return Err((StatusCode::NOT_FOUND, format!("Resource ID not found: {}", id)))
    };
    let nodes = resource.nodes.read().await;

    Ok(Json(serde_json::json!({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": resource.properties,
            "geometry": { "type": "MultiPoint", "coordinates": nodes.values().collect::<Vec<_>>() }
        }]
    })))
}

async fn route_enemy_id(
    Path(id): Path<i32>,
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Some(enemy) = state.app_state.enemy.get(&id) else {
        return Err((StatusCode::NOT_FOUND, format!("Enemy ID not found: {}", id)))
    };
    let nodes = enemy.nodes.read().await;
    
    // Keep MultiPoint format for enemies (unlike players which use individual Point features)
    let coordinates: Vec<[f64; 2]> = nodes
        .values()
        .map(|coords| [coords[0] as f64 / 1_000f64, coords[1] as f64 / 1_000f64])
        .collect();

    Ok(Json(serde_json::json!({
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": enemy.properties,
                "geometry": {
                    "type": "MultiPoint",
                    "coordinates": coordinates
                }
            }
        ]
    })))
}

async fn route_player_id(
    Path(id): Path<i32>,
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    // Check if player tracking is configured
    let Some(player) = state.app_state.player.get(&id) else {
        return Err((StatusCode::NOT_FOUND, format!("Player ID not found: {}", id)))
    };
    let nodes = player.nodes.read().await;
    let player_names = player.player_names.read().await;
    
    // Create individual Point features with entity IDs and player names in properties
    let features: Vec<serde_json::Value> = nodes
        .iter()
        .map(|(entity_id, coords)| {
            // Resolve player name: check active player_names, then shared last_known_names, then default
            let player_name = match player_names.get(entity_id) {
                Some(name) => name.clone(),
                None => {
                    // try shared last_known_names in state
                    let mut restored: Option<String> = None;
                    if let Some(player_group) = state.app_state.player.get(&1) {
                        // read last_known_names and check TTL
                        if let Ok(mut last_known) = player_group.last_known_names.try_write() {
                            if let Some((name, ts)) = last_known.get_mut(entity_id) {
                                // refresh timestamp and use name
                                *ts = chrono::Utc::now().timestamp_millis() as u64;
                                restored = Some(name.clone());
                                tracing::info!("route_player_id: restored cached username for entity_id={} -> {}", entity_id, name);
                            }
                        } else {
                            // Fall back to read lock if write not available
                            let last_known = player_group.last_known_names.blocking_read();
                            if let Some((name, _)) = last_known.get(entity_id) {
                                restored = Some(name.clone());
                                tracing::info!("route_player_id: found cached username for entity_id={} -> {}", entity_id, name);
                            }
                        }
                    } else {
                        tracing::warn!("route_player_id: could not get player_group for player ID 1");
                    }

                    if let Some(name) = restored {
                        name
                    } else {
                        let def = format!("Player_{}", entity_id);
                        tracing::warn!("route_player_id: no cached username found for entity_id={}, using placeholder: {}", entity_id, def);
                        def
                    }
                }
            };
            
            serde_json::json!({
                "type": "Feature",
                "properties": {
                    "entity_id": entity_id,
                    "player_name": player_name,
                    "makeCanvas": player.properties.get("makeCanvas").unwrap_or(&serde_json::json!("10"))
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [coords[0] as f64 / 1_000f64, coords[1] as f64 / 1_000f64]
                }
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "type": "FeatureCollection",
        "features": features
    })))
}

async fn route_resources(
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Json<Value> {
    Json(serde_json::json!(state.app_state.resources_list))
}

async fn route_enemies(
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Json<Value> {
    Json(serde_json::json!(state.app_state.enemies_list))
}

async fn route_players(
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Json<Value> {
    Json(serde_json::json!(state.app_state.players_list))
}

async fn route_health() -> Json<Value> {
    Json(serde_json::json!({
        "status": "ok",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}