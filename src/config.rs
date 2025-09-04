use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use std::collections::VecDeque;
use anyhow::{Result, anyhow};
use std::path::Path;
use bindings::sdk::{DbConnectionBuilder, __codegen::SpacetimeModule};

#[derive(Serialize, Deserialize, Clone)]
pub struct Entity {
    pub id: i32,
    #[serde(default = "Default::default")]
    pub name: String,
    #[serde(default = "default_properties")]
    pub properties: Value,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DungeonEntity {
    pub id: u64,
    #[serde(default = "Default::default")]
    pub name: String,
    #[serde(default = "default_properties")]
    pub properties: Value,
}

fn default_properties() -> Value { json!({ "makeCanvas": "10" }) }
fn default_socket_addr() -> SocketAddr { ([127, 0, 0, 1], 3300).into() }
fn default_logging_level() -> String { "info".to_string() }

#[derive(Serialize, Deserialize)]
pub struct DbConfig {
    #[serde(default = "Default::default")]
    pub token: String,
    pub region: i32,
}

#[derive(Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_socket_addr")]
    pub socket_addr: SocketAddr,
    #[serde(default = "Default::default")]
    pub cors_origin: String,
    #[serde(default = "default_logging_level")]
    pub logging_level: String,
}

#[derive(Serialize, Deserialize)]
pub struct AppConfig {
    pub db: DbConfig,
    pub server: ServerConfig,
    #[serde(default)]
    pub chat: ChatConfig,
    pub resources: Vec<Entity>,
    pub enemies: Vec<Entity>,
    pub players: Vec<Entity>,
    #[serde(default)]
    pub dungeons: Vec<DungeonEntity>,
}

// === Chat Configuration & State ===

fn default_chat_enabled() -> bool { false }
fn default_chat_channels() -> Vec<i32> { vec![3, 4, 5] } // Region, Claim, (Reserved/Other)
fn default_chat_include_context() -> bool { true }
fn default_chat_max_length() -> usize { 500 }
fn default_chat_throttle_ms() -> u64 { 100 }
fn default_chat_serve_recent() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatConfig {
    #[serde(default = "default_chat_enabled")]
    pub enabled: bool,
    #[serde(default = "default_chat_channels")]
    pub channels: Vec<i32>,
    #[serde(default = "default_chat_include_context")]
    pub include_context: bool,
    #[serde(default = "default_chat_max_length")]
    pub max_message_length: usize,
    #[serde(default = "default_chat_throttle_ms")]
    pub throttle_ms: u64,
    /// When true the `/chat/recent` endpoint will return the buffered recent messages.
    /// When false the endpoint will return an empty collection and clients should wait for SSE.
    #[serde(default = "default_chat_serve_recent")]
    pub serve_recent_on_fetch: bool,
}

impl Default for ChatConfig {
    fn default() -> Self {
        Self {
            enabled: default_chat_enabled(),
            channels: default_chat_channels(),
            include_context: default_chat_include_context(),
            max_message_length: default_chat_max_length(),
            throttle_ms: default_chat_throttle_ms(),
            serve_recent_on_fetch: default_chat_serve_recent(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub entity_id: u64,
    pub channel_id: i32,
    pub channel_name: String,
    pub target_id: u64,
    pub username: String,
    pub text: String,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

#[derive(Debug)]
pub struct ChatState {
    pub config: ChatConfig,
    pub recent_messages: Arc<RwLock<VecDeque<ChatMessage>>>,
    pub claim_names: Arc<RwLock<HashMap<u64, String>>>,
    pub empire_names: Arc<RwLock<HashMap<u64, String>>>,
}

pub struct EntityGroup {
    pub nodes: RwLock<HashMap<u64, [i32; 2]>>,
    pub player_names: RwLock<HashMap<u64, String>>, // Track player names by entity_id
    pub last_known_names: RwLock<HashMap<u64, (String, u64)>>, // (name, last-seen ms)
    pub properties: Value,
}

pub struct AppState {
    pub resource: HashMap<i32, EntityGroup>,
    pub enemy: HashMap<i32, EntityGroup>,
    pub player: HashMap<i32, EntityGroup>,
    pub dungeon: HashMap<u64, EntityGroup>,
    pub resources_list: Vec<Entity>,
    pub enemies_list: Vec<Entity>,
    pub players_list: Vec<Entity>,
    pub dungeons_list: Vec<DungeonEntity>,
    pub chat: ChatState,
}


impl AppConfig {
    pub fn from(path: &str) -> Result<Self> {
        let path = Path::new(path);
        let content = std::fs::read(path)?;
        let mut config: AppConfig = serde_json::from_slice(&content)?;

        if let Ok(token) = std::env::var("TOKEN") {
            config.db.token = token;
        }
        if let Ok(region) = std::env::var("REGION") {
            config.db.region = region.parse()
                .map_err(|_| anyhow!("invalid region, needs to be a number (1-9)"))?;
        }
        if let Ok(socket_addr) = std::env::var("SOCKET_ADDR") {
            config.server.socket_addr = socket_addr.parse()?;
        }
        if let Ok(cors_origin) = std::env::var("CORS_ORIGIN") {
            config.server.cors_origin = cors_origin;
        }
        if let Ok(logging_level) = std::env::var("LOGGING_LEVEL") {
            config.server.logging_level = logging_level;
        }

        if config.db.token.is_empty() {
            return Err(anyhow!("token is empty"));
        }
        if config.server.cors_origin == "*" {
            return Err(anyhow!("CORS origin may not be any, unset to disable"));
        }

        Ok(config)
    }

    pub fn build(self) -> (Arc<AppState>, DbConfig, ServerConfig) {
        let mut state = AppState {
            resource: HashMap::with_capacity(self.resources.len()),
            enemy: HashMap::with_capacity(self.enemies.len()),
            player: HashMap::with_capacity(self.players.len()),
            dungeon: HashMap::with_capacity(self.dungeons.len()),
            resources_list: self.resources.clone(),
            enemies_list: self.enemies.clone(),
            players_list: self.players.clone(),
            dungeons_list: self.dungeons.clone(),
            chat: ChatState {
                config: self.chat.clone(),
                recent_messages: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
                claim_names: Arc::new(RwLock::new(HashMap::new())),
                empire_names: Arc::new(RwLock::new(HashMap::new())),
            },
        };

        for Entity { id, name: _, properties } in self.resources {
            state.resource.insert(id, EntityGroup { 
                nodes: RwLock::new(HashMap::new()), 
                player_names: RwLock::new(HashMap::new()),
                last_known_names: RwLock::new(HashMap::new()),
                properties 
            });
        }
        for Entity { id, name: _, properties } in self.enemies {
            state.enemy.insert(id, EntityGroup { 
                nodes: RwLock::new(HashMap::new()), 
                player_names: RwLock::new(HashMap::new()),
                last_known_names: RwLock::new(HashMap::new()),
                properties 
            });
        }
        for Entity { id, name: _, properties } in self.players {
            state.player.insert(id, EntityGroup { 
                nodes: RwLock::new(HashMap::new()), 
                player_names: RwLock::new(HashMap::new()),
                last_known_names: RwLock::new(HashMap::new()),
                properties 
            });
        }
        for DungeonEntity { id, name: _, properties } in self.dungeons {
            state.dungeon.insert(id, EntityGroup { 
                nodes: RwLock::new(HashMap::new()), 
                player_names: RwLock::new(HashMap::new()),
                last_known_names: RwLock::new(HashMap::new()),
                properties 
            });
        }

        (Arc::new(state), self.db, self.server)
    }
}


pub trait WithDbConfig { fn configure(self, config: &DbConfig) -> Self; }
impl<M: SpacetimeModule> WithDbConfig for DbConnectionBuilder<M>
{
    fn configure(self, config: &DbConfig) -> Self {
        self.with_uri("https://bitcraft-early-access.spacetimedb.com")
            .with_module_name(format!("bitcraft-{}", config.region))
            .with_token(Some(&config.token))
    }
}

