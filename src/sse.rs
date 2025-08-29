use std::time::Duration;
use tokio::sync::broadcast;
use serde::{Serialize, Deserialize};
use thiserror::Error;
use axum::{
    extract::State,
    response::sse::{Event, Sse},
    routing::get,
    Router,
};
use tokio_stream::{wrappers::BroadcastStream, StreamExt, Stream};
use std::sync::Arc;

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
    
    /// Create a new SSE event with a message and event type
    pub fn with_type(message: String, event_type: String) -> Self {
        Self {
            message,
            event_type: Some(event_type),
        }
    }
}

/// SSE-specific error types
#[derive(Debug, Error)]
pub enum SseError {
    #[error("Channel send failed: {0}")]
    ChannelSend(#[from] broadcast::error::SendError<SseEvent>),
    
    #[error("No subscribers available")]
    NoSubscribers,
    
    #[error("SSE manager not initialized")]
    NotInitialized,
    
    #[error("Configuration error: {0}")]
    Config(String),
}

/// Core SSE manager for handling event broadcasting
#[derive(Clone)]
pub struct SseManager {
    tx: broadcast::Sender<SseEvent>,
    config: SseConfig,
}

impl SseManager {
    /// Create a new SSE manager with the given configuration
    pub fn new(config: SseConfig) -> Self {
        let (tx, _) = broadcast::channel(config.channel_capacity);
        Self { tx, config }
    }
    
    /// Create a new SSE manager with default configuration
    pub fn default() -> Self {
        Self::new(SseConfig::default())
    }
    
    /// Subscribe to SSE events
    pub fn subscribe(&self) -> broadcast::Receiver<SseEvent> {
        self.tx.subscribe()
    }
    
    /// Send an SSE event to all subscribers
    pub fn send_event(&self, event: SseEvent) -> Result<(), SseError> {
        if self.tx.receiver_count() > 0 {
            self.tx.send(event)?;
            Ok(())
        } else {
            Err(SseError::NoSubscribers)
        }
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
    
    /// Send a server shutdown event
    pub fn send_server_shutdown(&self) -> Result<bool, SseError> {
        let event = SseEvent::new("server_shutdown".to_string());
        self.send_event_if_subscribers(event)
    }
    
    /// Send a custom event with message and optional event type
    pub fn send_custom_event(&self, message: String, event_type: Option<String>) -> Result<bool, SseError> {
        let event = if let Some(event_type) = event_type {
            SseEvent::with_type(message, event_type)
        } else {
            SseEvent::new(message)
        };
        self.send_event_if_subscribers(event)
    }
    
    /// Check if there are any active subscribers
    pub fn has_subscribers(&self) -> bool {
        self.tx.receiver_count() > 0
    }
    
    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
    
    /// Get the broadcast sender (for compatibility with existing code)
    pub fn sender(&self) -> broadcast::Sender<SseEvent> {
        self.tx.clone()
    }
    
    /// Get SSE configuration
    pub fn config(&self) -> &SseConfig {
        &self.config
    }
    
    /// Get keep-alive interval as Duration
    pub fn keep_alive_interval(&self) -> Duration {
        Duration::from_secs(self.config.keep_alive_interval_secs)
    }
    
    /// Log SSE event sending with context
    pub fn log_event_sent(&self, event_type: &str, count: usize) {
        if self.config.verbose_logging && count > 0 {
            println!("SSE: Sent {} event to {} subscriber(s)", event_type, count);
        }
    }
    
    /// Log SSE error with context
    pub fn log_error(&self, operation: &str, error: &SseError) {
        if self.config.verbose_logging {
            eprintln!("SSE Error during {}: {:?}", operation, error);
        }
    }
}

/// Trait for converting messages into SSE events
pub trait IntoSseEvent {
    fn into_sse_event(self) -> SseEvent;
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
    
    /// Process a server shutdown message
    pub fn process_server_shutdown(&self) -> Result<(), SseError> {
        match self.manager.send_server_shutdown() {
            Ok(true) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Server shutdown event sent to all subscribers");
                }
                Ok(())
            }
            Ok(false) => {
                if self.manager.config.verbose_logging {
                    println!("SSE: Server shutdown - no active subscribers");
                }
                Ok(())
            }
            Err(e) => {
                self.manager.log_error("server shutdown", &e);
                Err(e)
            }
        }
    }
    
    /// Get reference to the underlying SSE manager
    pub fn manager(&self) -> &SseManager {
        &self.manager
    }
    
    /// Check if there are active subscribers
    pub fn has_subscribers(&self) -> bool {
        self.manager.has_subscribers()
    }
    
    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.manager.subscriber_count()
    }
}

// === HTTP Route Integration ===

/// Application state wrapper that includes SSE functionality
pub struct AppStateWithSse<T> {
    pub app_state: Arc<T>,
    pub sse_manager: SseManager,
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

/// Create SSE router with the /events endpoint
pub fn create_sse_router<T>() -> Router<Arc<AppStateWithSse<T>>>
where
    T: Send + Sync + 'static,
{
    Router::new()
        .route("/events", get(route_sse_events))
}

#[cfg(test)]
mod tests {
    use super::*;
    
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
        let manager = SseManager::new(config);
        assert_eq!(manager.subscriber_count(), 0);
        assert!(!manager.has_subscribers());
    }
    
    #[test]
    fn test_sse_event_creation() {
        let event = SseEvent::new("test message".to_string());
        assert_eq!(event.message, "test message");
        assert!(event.event_type.is_none());
        
        let event_with_type = SseEvent::with_type("test message".to_string(), "test_event".to_string());
        assert_eq!(event_with_type.message, "test message");
        assert_eq!(event_with_type.event_type, Some("test_event".to_string()));
    }
    
    #[tokio::test]
    async fn test_sse_manager_subscribe_and_send() {
        let manager = SseManager::default();
        let mut rx = manager.subscribe();
        
        assert_eq!(manager.subscriber_count(), 1);
        assert!(manager.has_subscribers());
        
        let event = SseEvent::new("test event".to_string());
        manager.send_event(event.clone()).unwrap();
        
        let received = rx.recv().await.unwrap();
        assert_eq!(received.message, "test event");
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
        
        let result = manager.send_server_shutdown();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }
    
    #[test]
    fn test_message_processor_creation() {
        let manager = SseManager::default();
        let processor = SseMessageProcessor::new(manager.clone());
        
        assert_eq!(processor.subscriber_count(), 0);
        assert!(!processor.has_subscribers());
        // Test that the processor has access to the manager functionality
        assert_eq!(processor.manager().subscriber_count(), manager.subscriber_count());
    }
}