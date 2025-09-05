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
    if !state.dungeon.is_empty() { queries.push(Query::DUNGEONS) }
    // Config-based chat subscription activation
    if state.chat.config.enabled {
        queries.push(Query::CHAT);
        info!("Chat subscription enabled via config");
    }

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
        .route("/dungeon/{id}", get(route_dungeon_id))
        .route("/health", get(route_health))
        .route("/resources", get(route_resources))
        .route("/enemies", get(route_enemies))
        .route("/players", get(route_players))
        .route("/dungeons", get(route_dungeons))
    // Chat endpoints
    .route("/chat/recent", get(route_chat_recent))
    .route("/chat/channels", get(route_chat_channels))
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
    // Instead of returning static config list, return aggregated live player data
    // This prevents duplicate entity IDs between /players and /player/{id} endpoints
    
    let mut all_features = Vec::new();
    
    // Collect live player data from all configured player groups
    for (player_id, player_group) in &state.app_state.player {
        let nodes = player_group.nodes.read().await;
        let player_names = player_group.player_names.read().await;
        
        // Create individual Point features with entity IDs and player names
        for (entity_id, coords) in nodes.iter() {
            // Resolve player name (same logic as route_player_id)
            let player_name = match player_names.get(entity_id) {
                Some(name) => name.clone(),
                None => {
                    // Try shared last_known_names
                    let mut restored: Option<String> = None;
                    if let Some(player_group_1) = state.app_state.player.get(&1) {
                        if let Ok(mut last_known) = player_group_1.last_known_names.try_write() {
                            if let Some((name, ts)) = last_known.get_mut(entity_id) {
                                *ts = chrono::Utc::now().timestamp_millis() as u64;
                                restored = Some(name.clone());
                            }
                        } else {
                            let last_known = player_group_1.last_known_names.blocking_read();
                            if let Some((name, _)) = last_known.get(entity_id) {
                                restored = Some(name.clone());
                            }
                        }
                    }
                    
                    restored.unwrap_or_else(|| format!("Player_{}", entity_id))
                }
            };
            
            all_features.push(serde_json::json!({
                "type": "Feature",
                "properties": {
                    "entity_id": entity_id,
                    "player_name": player_name,
                    "player_group_id": player_id, // Include which group this came from
                    "makeCanvas": player_group.properties.get("makeCanvas").unwrap_or(&serde_json::json!("10"))
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [coords[0] as f64 / 1_000f64, coords[1] as f64 / 1_000f64]
                }
            }));
        }
    }
    
    Json(serde_json::json!({
        "type": "FeatureCollection",
        "features": all_features,
        "metadata": {
            "endpoint": "players",
            "aggregated": true,
            "player_groups": state.app_state.player.keys().collect::<Vec<_>>(),
            "feature_count": all_features.len()
        }
    }))
}

async fn route_health() -> Json<Value> {
    Json(serde_json::json!({
        "status": "ok",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

// === Chat API Endpoints ===
async fn route_chat_recent(
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    if !state.app_state.chat.config.enabled {
        return Err((StatusCode::NOT_FOUND, "Chat tracking is disabled".to_string()));
    }

    // If configured not to serve the recent buffer, return an empty collection.
    // This avoids returning a large batch of potentially outdated messages on initial load.
    if !state.app_state.chat.config.serve_recent_on_fetch {
        return Ok(Json(serde_json::json!({
            "type": "ChatCollection",
            "messages": [],
            "count": 0,
            "timestamp": chrono::Utc::now().to_rfc3339()
        })));
    }

    let recent = state.app_state.chat.recent_messages.read().await;
    let mut messages: Vec<&crate::config::ChatMessage> = recent.iter().collect();
    
    // Ensure messages are sorted by timestamp descending (most recent first)
    // The buffer should already be sorted, but this ensures correct order for the API
    messages.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    Ok(Json(serde_json::json!({
        "type": "ChatCollection",
        "messages": messages,
        "count": messages.len(),
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

async fn route_chat_channels(
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Json<Value> {
    Json(serde_json::json!({
        "channels": [
            {"id": 2, "name": "Empire Internal", "description": "Private empire communication"},
            {"id": 3, "name": "Region", "description": "Regional chat"},
            {"id": 4, "name": "Claim", "description": "Claim-specific chat"},
            {"id": 5, "name": "Empire Public", "description": "Public empire communication"}
        ],
    "enabled": state.app_state.chat.config.enabled,
    "configured_channels": state.app_state.chat.config.channels,
    // Expose whether the server will serve recent messages on a fetch.
    // Clients should open an SSE connection if this is false so they still
    // receive live chat messages.
    "serve_recent_on_fetch": state.app_state.chat.config.serve_recent_on_fetch
    }))
}

async fn route_dungeon_id(
    Path(id): Path<u64>,
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Some(dungeon) = state.app_state.dungeon.get(&id) else {
        return Err((StatusCode::NOT_FOUND, format!("Dungeon ID not found: {}", id)))
    };
    let nodes = dungeon.nodes.read().await;

    // Create individual Point features with entity IDs and portal states in properties
    let features: Vec<serde_json::Value> = nodes
        .iter()
        .map(|(entity_id, coords)| {
            serde_json::json!({
                "type": "Feature",
                "properties": {
                    "entity_id": entity_id,
                    "makeCanvas": dungeon.properties.get("makeCanvas").unwrap_or(&serde_json::json!("10")),
                    "dungeon_type": id
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

async fn route_dungeons(
    state: State<Arc<AppStateWithSse<AppState>>>,
) -> Json<Value> {
    // Create enhanced dungeons list with portal state information
    let mut dungeons_with_portal_state = Vec::new();
    
    for dungeon_config in &state.app_state.dungeons_list {
        let mut dungeon_info = serde_json::json!({
            "id": dungeon_config.id,
            "name": dungeon_config.name,
            "properties": dungeon_config.properties
        });
        
        // Check if this dungeon has any active portals
        if let Some(dungeon_group) = state.app_state.dungeon.get(&dungeon_config.id) {
            let nodes = dungeon_group.nodes.read().await;
            let active_portals: Vec<serde_json::Value> = nodes
                .iter()
                .map(|(entity_id, coords)| {
                    serde_json::json!({
                        "entity_id": entity_id,
                        "coordinates": [coords[0] as f64 / 1_000f64, coords[1] as f64 / 1_000f64],
                        "portal_active": true
                    })
                })
                .collect();
            
            dungeon_info["portal_state"] = serde_json::json!({
                "has_active_portals": !active_portals.is_empty(),
                "active_portal_count": active_portals.len(),
                "active_portals": active_portals
            });
        } else {
            // No dungeon group found, no active portals
            dungeon_info["portal_state"] = serde_json::json!({
                "has_active_portals": false,
                "active_portal_count": 0,
                "active_portals": []
            });
        }
        
        dungeons_with_portal_state.push(dungeon_info);
    }
    
    Json(serde_json::json!({
        "dungeons": dungeons_with_portal_state,
        "metadata": {
            "endpoint": "dungeons",
            "total_dungeons": dungeons_with_portal_state.len(),
            "includes_portal_state": true
        }
    }))
}