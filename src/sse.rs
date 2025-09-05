use std::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{broadcast, RwLock};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use axum::{
    extract::State,
    response::sse::{Event, Sse},
    routing::get,
    Router,
};
use tokio_stream::{wrappers::BroadcastStream, StreamExt, Stream};

use crate::config::ChatMessage;

/// Configuration for SSE functionality
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseConfig {
    /// Channel capacity for broadcast channel
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    
    /// Keep-alive interval in seconds
    #[serde(default = "default_keep_alive_interval_secs")]
    pub keep_alive_interval_secs: u64,
    
    /// Keep-alive message text
    #[serde(default = "default_keep_alive_text")]
    pub keep_alive_text: String,
    
    /// Enable verbose logging of SSE events
    #[serde(default = "default_verbose_logging")]
    pub verbose_logging: bool,
}

impl Default for SseConfig {
    fn default() -> Self {
        Self {
            channel_capacity: default_channel_capacity(),
            keep_alive_interval_secs: default_keep_alive_interval_secs(),
            keep_alive_text: default_keep_alive_text(),
            verbose_logging: default_verbose_logging(),
        }
    }
}

fn default_channel_capacity() -> usize { 1000 }
fn default_keep_alive_interval_secs() -> u64 { 30 }
fn default_keep_alive_text() -> String { "keep-alive".to_string() }
fn default_verbose_logging() -> bool { false }

/// Event structure for SSE messages
#[derive(Clone, Debug)]
pub struct SseEvent {
    pub message: String,
    pub event_type: Option<String>,
}

impl SseEvent {
    /// Create a new SSE event with just a message
    pub fn new(message: String) -> Self {
        Self {
            message,
            event_type: None,
        }
    }
}

/// SSE-specific error types
#[derive(Debug, Error)]
pub enum SseError {
    #[error("Channel send failed: {0}")]
    ChannelSend(#[from] broadcast::error::SendError<SseEvent>),
    #[error("Failed to serialize chat message: {0}")]
    Serialization(String),
}

/// Core SSE manager for handling event broadcasting
#[derive(Clone)]
pub struct SseManager {
    tx: broadcast::Sender<SseEvent>,
    config: SseConfig,
    seq_counter: Arc<AtomicU64>,
}

impl SseManager {
    /// Create a new SSE manager with the given configuration
    pub fn new(config: SseConfig) -> Self {
        let (tx, _) = broadcast::channel(config.channel_capacity);
        Self { 
            tx, 
            config,
            seq_counter: Arc::new(AtomicU64::new(1)), // Start sequence at 1
        }
    }
    
    /// Create a new SSE manager with default configuration
    pub fn default() -> Self {
        Self::new(SseConfig::default())
    }
    
    /// Get the next sequence number
    pub fn next_seq(&self) -> u64 {
        self.seq_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Subscribe to SSE events
    pub fn subscribe(&self) -> broadcast::Receiver<SseEvent> {
        self.tx.subscribe()
    }
    
    /// Send an SSE event only if there are subscribers (no error if none)
    pub fn send_event_if_subscribers(&self, event: SseEvent) -> Result<bool, SseError> {
        if self.tx.receiver_count() > 0 {
            self.tx.send(event)?;
            Ok(true)
        } else {
            Ok(false) // No subscribers, but not an error
        }
    }
    
    /// Send a resource insert event
    pub fn send_resource_insert(&self, resource_id: i32) -> Result<bool, SseError> {
        let event = SseEvent::new(format!("insert:{}", resource_id));
        self.send_event_if_subscribers(event)
    }
    
    /// Send a resource delete event
    pub fn send_resource_delete(&self, resource_id: i32) -> Result<bool, SseError> {
        let event = SseEvent::new(format!("delete:{}", resource_id));
        self.send_event_if_subscribers(event)
    }
    
    /// Send an enemy insert event
    pub fn send_enemy_insert(&self, enemy_id: i32) -> Result<bool, SseError> {
        let event = SseEvent::new(format!("enemy_insert:{}", enemy_id));
        self.send_event_if_subscribers(event)
    }
    
    /// Send an enemy delete event
    pub fn send_enemy_delete(&self, enemy_id: i32) -> Result<bool, SseError> {
        let event = SseEvent::new(format!("enemy_delete:{}", enemy_id));
        self.send_event_if_subscribers(event)
    }
    
    /// Send a player insert event
    pub fn send_player_insert(&self, char_id: i32, entity_id: u64) -> Result<bool, SseError> {
        let event = SseEvent::new(format!("player_insert:{}:{}", char_id, entity_id));
        self.send_event_if_subscribers(event)
    }
    
    /// Send a player delete event
    pub fn send_player_delete(&self, char_id: i32, entity_id: u64) -> Result<bool, SseError> {
        let event = SseEvent::new(format!("player_delete:{}:{}", char_id, entity_id));
        self.send_event_if_subscribers(event)
    }
    
    /// Send a dungeon insert event (portal enabled)
    pub fn send_dungeon_insert(&self, dungeon_id: u64, entity_id: u64) -> Result<bool, SseError> {
        let event = SseEvent::new(format!("dungeon_insert:{}:{}", dungeon_id, entity_id));
        self.send_event_if_subscribers(event)
    }
    
    /// Send a dungeon delete event (portal disabled/removed)
    pub fn send_dungeon_delete(&self, dungeon_id: u64, entity_id: u64) -> Result<bool, SseError> {
        let event = SseEvent::new(format!("dungeon_delete:{}:{}", dungeon_id, entity_id));
        self.send_event_if_subscribers(event)
    }
    
    /// Send a portal state change event with entity_id and boolean state
    pub fn send_portal_state_change(&self, entity_id: u64, portal_active: bool) -> Result<bool, SseError> {
        let event_data = serde_json::json!({
            "event": "portal_state_change",
            "entity_id": entity_id,
            "portal_active": portal_active,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        });
        let event = SseEvent { 
            message: event_data.to_string(), 
            event_type: Some("portal_state_change".to_string()) 
        };
        self.send_event_if_subscribers(event)
    }
    
    /// Send a portal state change event with additional dungeon context
    pub fn send_portal_state_change_with_dungeon(&self, dungeon_id: u64, entity_id: u64, portal_active: bool, x: Option<i32>, z: Option<i32>) -> Result<bool, SseError> {
        let mut event_data = serde_json::json!({
            "event": "portal_state_change",
            "dungeon_id": dungeon_id,
            "entity_id": entity_id,
            "portal_active": portal_active,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        });
        
        // Add coordinates if provided
        if let (Some(x_coord), Some(z_coord)) = (x, z) {
            event_data["coordinates"] = serde_json::json!([x_coord, z_coord]);
        }
        
        let event = SseEvent { 
            message: event_data.to_string(), 
            event_type: Some("portal_state_change".to_string()) 
        };
        self.send_event_if_subscribers(event)
    }
    
    /// Get SSE configuration
    pub fn config(&self) -> &SseConfig {
        &self.config
    }
    
    /// Get keep-alive interval as Duration
    pub fn keep_alive_interval(&self) -> Duration {
        Duration::from_secs(self.config.keep_alive_interval_secs)
    }
    
    /// Log SSE error with context
    pub fn log_error(&self, operation: &str, error: &SseError) {
        if self.config.verbose_logging {
            eprintln!("SSE Error during {}: {:?}", operation, error);
        }
    }

    // === Chat-specific helpers ===

    /// Send a structured chat message event (event type: chat_message)
    pub fn send_chat_message(&self, message: &ChatMessage) -> Result<bool, SseError> {
        let event_data = serde_json::to_string(message)
            .map_err(|e| SseError::Serialization(e.to_string()))?;
        let event = SseEvent { message: event_data, event_type: Some("chat_message".to_string()) };
        self.send_event_if_subscribers(event)
    }

    /// Send a simplified formatted chat event (event type: chat) => "[Context] Username: Message"
    pub fn send_chat_event_formatted(&self, username: &str, text: &str, channel: &str, context: Option<&str>) -> Result<bool, SseError> {
        let formatted_message = match context {
            Some(ctx) => format!("[{}] {}: {}", ctx, username, text),
            None => format!("[{}] {}: {}", channel, username, text),
        };
        let event = SseEvent { message: formatted_message, event_type: Some("chat".to_string()) };
        self.send_event_if_subscribers(event)
    }

    // === Dungeon-specific SSE events ===

    /// Send a dungeon snapshot event (for initial client synchronization)
    pub fn send_dungeon_snapshot(&self, entity_id: u64, dungeon_data: &serde_json::Value, network_state: Option<&serde_json::Value>, derived_state: &str) -> Result<bool, SseError> {
        let seq = self.next_seq();
        let mut event_data = serde_json::json!({
            "type": "dungeon.snapshot",
            "entity_id": entity_id,
            "dungeon": dungeon_data,
            "derived_state": derived_state,
            "seq": seq,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        });
        
        if let Some(network) = network_state {
            event_data["network"] = network.clone();
        }
        
        let event = SseEvent { 
            message: event_data.to_string(), 
            event_type: Some("dungeon.snapshot".to_string()) 
        };
        self.send_event_if_subscribers(event)
    }

    /// Send a dungeon created event
    pub fn send_dungeon_created(&self, entity_id: u64, dungeon_data: &serde_json::Value) -> Result<bool, SseError> {
        let seq = self.next_seq();
        let event_data = serde_json::json!({
            "type": "dungeon.created",
            "entity_id": entity_id,
            "dungeon": dungeon_data,
            "seq": seq,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        });
        
        let event = SseEvent { 
            message: event_data.to_string(), 
            event_type: Some("dungeon.created".to_string()) 
        };
        self.send_event_if_subscribers(event)
    }

    /// Send a dungeon updated event
    pub fn send_dungeon_updated(&self, entity_id: u64, dungeon_data: &serde_json::Value) -> Result<bool, SseError> {
        let seq = self.next_seq();
        let event_data = serde_json::json!({
            "type": "dungeon.updated",
            "entity_id": entity_id,
            "dungeon": dungeon_data,
            "seq": seq,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        });
        
        let event = SseEvent { 
            message: event_data.to_string(), 
            event_type: Some("dungeon.updated".to_string()) 
        };
        self.send_event_if_subscribers(event)
    }

    /// Send a dungeon network updated event (collapse lifecycle changes)
    pub fn send_dungeon_network_updated(&self, entity_id: u64, network_state: &serde_json::Value, derived_state: &str) -> Result<bool, SseError> {
        let seq = self.next_seq();
        let event_data = serde_json::json!({
            "type": "dungeon.network.updated",
            "entity_id": entity_id,
            "network": network_state,
            "derived_state": derived_state,
            "seq": seq,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        });
        
        let event = SseEvent { 
            message: event_data.to_string(), 
            event_type: Some("dungeon.network.updated".to_string()) 
        };
        self.send_event_if_subscribers(event)
    }

    /// Send a dungeon network transition event (state change from/to)
    pub fn send_dungeon_network_transition(&self, entity_id: u64, from_state: &str, to_state: &str, at_timestamp: u64) -> Result<bool, SseError> {
        let seq = self.next_seq();
        let event_data = serde_json::json!({
            "type": "dungeon.network.transition",
            "entity_id": entity_id,
            "from": from_state,
            "to": to_state,
            "at_ts": at_timestamp,
            "seq": seq,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        });
        
        let event = SseEvent { 
            message: event_data.to_string(), 
            event_type: Some("dungeon.network.transition".to_string()) 
        };
        self.send_event_if_subscribers(event)
    }
}

/// Message processor for handling complex SSE event logic
pub struct SseMessageProcessor {
    manager: SseManager,
}

impl SseMessageProcessor {
    /// Create a new message processor with the given SSE manager
    pub fn new(manager: SseManager) -> Self {
        Self { manager }
    }
    
    /// Process a resource insert message and send appropriate SSE event
    pub fn process_resource_insert(&self, resource_id: i32, entity_id: u64, x: i32, z: i32) -> Result<(), SseError> {
        match self.manager.send_resource_insert(resource_id) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Resource insert event sent for resource {} (entity: {}, pos: {}, {})", 
                             resource_id, entity_id, x, z);
                }
                Ok(())
            }
            Ok(false) => {
                // No subscribers, but not an error
                Ok(())
            }
            Err(e) => {
                self.manager.log_error("resource insert", &e);
                Err(e)
            }
        }
    }
    
    /// Process a resource delete message and send appropriate SSE event
    pub fn process_resource_delete(&self, resource_id: i32, entity_id: u64) -> Result<(), SseError> {
        match self.manager.send_resource_delete(resource_id) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Resource delete event sent for resource {} (entity: {})", resource_id, entity_id);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("resource delete", &e);
                Err(e)
            }
        }
    }
    
    /// Process an enemy insert message and send appropriate SSE event
    pub fn process_enemy_insert(&self, enemy_id: i32, entity_id: u64, x: i32, z: i32) -> Result<(), SseError> {
        match self.manager.send_enemy_insert(enemy_id) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Enemy insert event sent for enemy {} (entity: {}, pos: {}, {})", 
                             enemy_id, entity_id, x, z);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("enemy insert", &e);
                Err(e)
            }
        }
    }
    
    /// Process an enemy delete message and send appropriate SSE event
    pub fn process_enemy_delete(&self, enemy_id: i32, entity_id: u64) -> Result<(), SseError> {
        match self.manager.send_enemy_delete(enemy_id) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Enemy delete event sent for enemy {} (entity: {})", enemy_id, entity_id);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("enemy delete", &e);
                Err(e)
            }
        }
    }
    
    /// Process a player insert message and send appropriate SSE event
    pub fn process_player_insert(&self, char_id: i32, entity_id: u64, x: i32, z: i32) -> Result<(), SseError> {
        match self.manager.send_player_insert(char_id, entity_id) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Player insert event sent for character {} (entity: {}, pos: {}, {})", 
                             char_id, entity_id, x, z);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("player insert", &e);
                Err(e)
            }
        }
    }
    
    /// Process a player delete message and send appropriate SSE event
    pub fn process_player_delete(&self, char_id: i32, entity_id: u64) -> Result<(), SseError> {
        match self.manager.send_player_delete(char_id, entity_id) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Player delete event sent for character {} (entity: {})", char_id, entity_id);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("player delete", &e);
                Err(e)
            }
        }
    }

    /// Process a dungeon insert message and send appropriate SSE event (portal enabled)
    pub fn process_dungeon_insert(&self, dungeon_id: u64, entity_id: u64, x: i32, z: i32) -> Result<(), SseError> {
        match self.manager.send_dungeon_insert(dungeon_id, entity_id) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Dungeon insert event sent for dungeon {} (entity: {}, pos: {}, {})", 
                             dungeon_id, entity_id, x, z);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("dungeon insert", &e);
                Err(e)
            }
        }
    }
    
    /// Process a dungeon delete message and send appropriate SSE event (portal disabled/removed)
    pub fn process_dungeon_delete(&self, dungeon_id: u64, entity_id: u64) -> Result<(), SseError> {
        match self.manager.send_dungeon_delete(dungeon_id, entity_id) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Dungeon delete event sent for dungeon {} (entity: {})", dungeon_id, entity_id);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("dungeon delete", &e);
                Err(e)
            }
        }
    }

    /// Process a portal state change and send appropriate SSE event
    pub fn process_portal_state_change(&self, entity_id: u64, portal_active: bool) -> Result<(), SseError> {
        match self.manager.send_portal_state_change(entity_id, portal_active) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Portal state change event sent for entity {} (active: {})", entity_id, portal_active);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("portal state change", &e);
                Err(e)
            }
        }
    }

    /// Process a portal state change with dungeon context and send appropriate SSE event
    pub fn process_portal_state_change_with_dungeon(&self, dungeon_id: u64, entity_id: u64, portal_active: bool, x: Option<i32>, z: Option<i32>) -> Result<(), SseError> {
        match self.manager.send_portal_state_change_with_dungeon(dungeon_id, entity_id, portal_active, x, z) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    let coords_str = match (x, z) {
                        (Some(x_coord), Some(z_coord)) => format!(" at ({}, {})", x_coord, z_coord),
                        _ => String::new(),
                    };
                    println!("SSE: Portal state change event sent for dungeon {} entity {} (active: {}){}", 
                             dungeon_id, entity_id, portal_active, coords_str);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("portal state change with dungeon", &e);
                Err(e)
            }
        }
    }

    /// Process a chat message with optional throttling predicate
    pub fn process_chat_message(&self, message: &ChatMessage, throttle_check: impl Fn() -> bool) -> Result<(), SseError> {
        if !throttle_check() {
            return Ok(()); // throttled, silently skip
        }
        match self.manager.send_chat_message(message) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Chat message event sent (channel={} user={} text={})", message.channel_name, message.username, message.text);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("chat message", &e);
                Err(e)
            }
        }
    }

    // === Dungeon-specific processor methods ===

    /// Process a dungeon created event and send appropriate SSE event
    pub fn process_dungeon_created(&self, entity_id: u64, dungeon_data: &serde_json::Value) -> Result<(), SseError> {
        match self.manager.send_dungeon_created(entity_id, dungeon_data) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Dungeon created event sent for entity {}", entity_id);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("dungeon created", &e);
                Err(e)
            }
        }
    }

    /// Process a dungeon updated event and send appropriate SSE event
    pub fn process_dungeon_updated(&self, entity_id: u64, dungeon_data: &serde_json::Value) -> Result<(), SseError> {
        match self.manager.send_dungeon_updated(entity_id, dungeon_data) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Dungeon updated event sent for entity {}", entity_id);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("dungeon updated", &e);
                Err(e)
            }
        }
    }

    /// Process a dungeon network updated event and send appropriate SSE event
    pub fn process_dungeon_network_updated(&self, entity_id: u64, network_state: &serde_json::Value, derived_state: &str) -> Result<(), SseError> {
        match self.manager.send_dungeon_network_updated(entity_id, network_state, derived_state) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Dungeon network updated event sent for entity {} (state: {})", entity_id, derived_state);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("dungeon network updated", &e);
                Err(e)
            }
        }
    }

    /// Process a dungeon network transition event and send appropriate SSE event
    pub fn process_dungeon_network_transition(&self, entity_id: u64, from_state: &str, to_state: &str, at_timestamp: u64) -> Result<(), SseError> {
        match self.manager.send_dungeon_network_transition(entity_id, from_state, to_state, at_timestamp) {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Dungeon network transition event sent for entity {} ({} -> {})", entity_id, from_state, to_state);
                }
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                self.manager.log_error("dungeon network transition", &e);
                Err(e)
            }
        }
    }
}

// === HTTP Route Integration ===

/// Application state wrapper that includes SSE functionality
pub struct AppStateWithSse<T> {
    pub app_state: Arc<T>,
    pub sse_manager: SseManager,
    pub dungeon_states: Arc<RwLock<HashMap<u64, crate::dungeon::DungeonState>>>,
}

/// SSE route handler function
async fn route_sse_events<T>(
    State(state): State<Arc<AppStateWithSse<T>>>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let rx = state.sse_manager.subscribe();
    let stream = BroadcastStream::new(rx)
        .map(|msg| {
            match msg {
                Ok(sse_event) => {
                    if let Some(event_type) = sse_event.event_type {
                        Ok(Event::default().event(event_type).data(sse_event.message))
                    } else {
                        Ok(Event::default().data(sse_event.message))
                    }
                }
                Err(err) => match err {
                    tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(_) => {
                        // Client missed messages; ask client to reconnect
                        Ok(Event::default().event("reconnect").data(""))
                    }
                },
            }
        });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(state.sse_manager.keep_alive_interval())
            .text(state.sse_manager.config().keep_alive_text.clone()),
    )
}

/// Dungeon-specific SSE route handler function (filters dungeon and portal events)
async fn route_sse_dungeons<T>(
    State(state): State<Arc<AppStateWithSse<T>>>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let rx = state.sse_manager.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter(|msg| {
            match msg {
                Ok(sse_event) => {
                    // Filter to dungeon-related events (including new dungeon.* event types)
                    let is_dungeon_event = sse_event.message.starts_with("dungeon_insert:") || 
                                         sse_event.message.starts_with("dungeon_delete:") ||
                                         sse_event.event_type.as_deref().map(|t| t.starts_with("dungeon.")).unwrap_or(false) ||
                                         sse_event.event_type.as_deref() == Some("portal_state_change");
                    is_dungeon_event
                }
                Err(_) => true, // Let reconnect messages through
            }
        })
        .map(|msg| {
            match msg {
                Ok(sse_event) => {
                    if let Some(event_type) = sse_event.event_type {
                        Ok(Event::default().event(event_type).data(sse_event.message))
                    } else {
                        Ok(Event::default().data(sse_event.message))
                    }
                }
                Err(err) => match err {
                    tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(_) => {
                        // Client missed messages; ask client to reconnect
                        Ok(Event::default().event("reconnect").data(""))
                    }
                },
            }
        });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(state.sse_manager.keep_alive_interval())
            .text(state.sse_manager.config().keep_alive_text.clone()),
    )
}

/// Create SSE router with the /events endpoint and /events/dungeons endpoint
pub fn create_sse_router<T>() -> Router<Arc<AppStateWithSse<T>>>
where
    T: Send + Sync + 'static,
{
    Router::new()
        .route("/events", get(route_sse_events))
        .route("/events/dungeons", get(route_sse_dungeons))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration as TokioDuration};
    
    #[test]
    fn test_sse_config_default() {
        let config = SseConfig::default();
        assert_eq!(config.channel_capacity, 1000);
        assert_eq!(config.keep_alive_interval_secs, 30);
        assert_eq!(config.keep_alive_text, "keep-alive");
        assert_eq!(config.verbose_logging, false); // Default is quiet
    }
    
    #[test]
    fn test_sse_config_verbose_logging() {
        let mut config = SseConfig::default();
        config.verbose_logging = true;
        let manager = SseManager::new(config);
        assert_eq!(manager.config().verbose_logging, true);
    }
    
    #[test]
    fn test_sse_manager_creation() {
        let config = SseConfig::default();
        let _manager = SseManager::new(config);
        // Test that manager can be created successfully
    }
    
    #[test]
    fn test_sse_event_creation() {
        let event = SseEvent::new("test message".to_string());
        assert_eq!(event.message, "test message");
        assert!(event.event_type.is_none());
    }
    
    #[tokio::test]
    async fn test_sse_manager_subscribe_and_send() {
        let manager = SseManager::default();
        let mut rx = manager.subscribe();
        
        // Test resource insert
        let result = manager.send_resource_insert(1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        
        let received = rx.recv().await.unwrap();
        assert_eq!(received.message, "insert:1");
        assert!(received.event_type.is_none());
    }
    
    #[tokio::test]
    async fn test_specialized_event_methods() {
        let manager = SseManager::default();
        let mut rx = manager.subscribe();
        
        // Test resource insert
        let result = manager.send_resource_insert(1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        
        let received = rx.recv().await.unwrap();
        assert_eq!(received.message, "insert:1");
        
        // Test enemy delete
        let result = manager.send_enemy_delete(2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        
        let received = rx.recv().await.unwrap();
        assert_eq!(received.message, "enemy_delete:2");
        
        // Test player insert
        let result = manager.send_player_insert(1, 123);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        
        let received = rx.recv().await.unwrap();
        assert_eq!(received.message, "player_insert:1:123");
    }
    
    #[test]
    fn test_no_subscribers_behavior() {
        let manager = SseManager::default();
        
        // Test that methods return Ok(false) when no subscribers
        let result = manager.send_resource_insert(1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }
    
    #[test]
    fn test_message_processor_creation() {
        let manager = SseManager::default();
        let _processor = SseMessageProcessor::new(manager);
        // Test that the processor can be created successfully
    }

    #[tokio::test]
    async fn test_chat_event_methods() {
        let manager = SseManager::default();
        let mut rx = manager.subscribe();

        let chat_msg = ChatMessage {
            entity_id: 1,
            channel_id: 3,
            channel_name: "Region".to_string(),
            target_id: 42,
            username: "Tester".to_string(),
            text: "Hello".to_string(),
            timestamp: 0,
            context: Some("EmpireName".to_string()),
        };

        // Structured event
        let sent = manager.send_chat_message(&chat_msg).unwrap();
        assert!(sent);
        let event = rx.recv().await.unwrap();
        assert_eq!(event.event_type.unwrap(), "chat_message");

        // Formatted event
        let sent2 = manager.send_chat_event_formatted(&chat_msg.username, &chat_msg.text, &chat_msg.channel_name, chat_msg.context.as_deref()).unwrap();
        assert!(sent2);
        let event2 = rx.recv().await.unwrap();
        assert_eq!(event2.event_type.unwrap(), "chat");
    }

    #[tokio::test]
    async fn test_chat_message_throttled_behavior() {
        let manager = SseManager::default();
        let mut rx = manager.subscribe();
        let processor = SseMessageProcessor::new(manager.clone());

        let chat_msg = ChatMessage {
            entity_id: 1,
            channel_id: 3,
            channel_name: "Region".to_string(),
            target_id: 0,
            username: "Tester".to_string(),
            text: "Hello".to_string(),
            timestamp: 0,
            context: None,
        };

        // First send allowed
        processor.process_chat_message(&chat_msg, || true).unwrap();
        let first = rx.recv().await.unwrap();
        assert_eq!(first.event_type.unwrap(), "chat_message");

        // Second send throttled (closure returns false)
        processor.process_chat_message(&chat_msg, || false).unwrap();
        let second = timeout(TokioDuration::from_millis(150), rx.recv()).await;
        assert!(second.is_err(), "Expected no second event due to throttling simulation");
    }
    
    #[tokio::test]
    async fn test_portal_state_change_events() {
        let manager = SseManager::default();
        let mut rx = manager.subscribe();
        
        // Test basic portal state change (active)
        let result = manager.send_portal_state_change(123, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        
        let received = rx.recv().await.unwrap();
        assert_eq!(received.event_type.unwrap(), "portal_state_change");
        
        // Parse the JSON message
        let event_data: serde_json::Value = serde_json::from_str(&received.message).unwrap();
        assert_eq!(event_data["event"], "portal_state_change");
        assert_eq!(event_data["entity_id"], 123);
        assert_eq!(event_data["portal_active"], true);
        assert!(event_data["timestamp"].is_number());
        
        // Test portal state change (inactive)
        let result = manager.send_portal_state_change(456, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        
        let received = rx.recv().await.unwrap();
        let event_data: serde_json::Value = serde_json::from_str(&received.message).unwrap();
        assert_eq!(event_data["portal_active"], false);
        assert_eq!(event_data["entity_id"], 456);
    }
    
    #[tokio::test]
    async fn test_portal_state_change_with_dungeon_context() {
        let manager = SseManager::default();
        let mut rx = manager.subscribe();
        
        // Test with dungeon context and coordinates
        let result = manager.send_portal_state_change_with_dungeon(789, 123, true, Some(100), Some(200));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        
        let received = rx.recv().await.unwrap();
        assert_eq!(received.event_type.unwrap(), "portal_state_change");
        
        // Parse the JSON message
        let event_data: serde_json::Value = serde_json::from_str(&received.message).unwrap();
        assert_eq!(event_data["event"], "portal_state_change");
        assert_eq!(event_data["dungeon_id"], 789);
        assert_eq!(event_data["entity_id"], 123);
        assert_eq!(event_data["portal_active"], true);
        assert_eq!(event_data["coordinates"][0], 100);
        assert_eq!(event_data["coordinates"][1], 200);
        
        // Test without coordinates
        let result = manager.send_portal_state_change_with_dungeon(321, 456, false, None, None);
        assert!(result.is_ok());
        
        let received = rx.recv().await.unwrap();
        let event_data: serde_json::Value = serde_json::from_str(&received.message).unwrap();
        assert_eq!(event_data["dungeon_id"], 321);
        assert_eq!(event_data["portal_active"], false);
        assert!(event_data["coordinates"].is_null());
    }
    
    #[tokio::test]
    async fn test_portal_state_change_processor() {
        let manager = SseManager::default();
        let mut rx = manager.subscribe();
        let processor = SseMessageProcessor::new(manager.clone());
        
        // Test basic processor method
        processor.process_portal_state_change(123, true).unwrap();
        let received = rx.recv().await.unwrap();
        assert_eq!(received.event_type.unwrap(), "portal_state_change");
        
        // Test processor with dungeon context
        processor.process_portal_state_change_with_dungeon(789, 456, false, Some(50), Some(75)).unwrap();
        let received = rx.recv().await.unwrap();
        let event_data: serde_json::Value = serde_json::from_str(&received.message).unwrap();
        assert_eq!(event_data["dungeon_id"], 789);
        assert_eq!(event_data["entity_id"], 456);
        assert_eq!(event_data["portal_active"], false);
        assert_eq!(event_data["coordinates"][0], 50);
        assert_eq!(event_data["coordinates"][1], 75);
    }
}