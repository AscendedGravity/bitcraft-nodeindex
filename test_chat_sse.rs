use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde_json;

// Import types from our library
use bitcraft_nodeindex::config::ChatMessage;
use bitcraft_nodeindex::sse::{SseManager, SseMessageProcessor};

#[tokio::main]
async fn main() {
    println!("Testing Chat SSE Integration");
    
    // Create SSE manager
    let sse_manager = SseManager::default();
    let processor = SseMessageProcessor::new(sse_manager.clone());
    
    // Create a test chat message
    let test_message = ChatMessage {
        entity_id: 12345,
        channel_id: 3, // Region channel
        channel_name: "Region".to_string(),
        target_id: 0,
        username: "TestUser".to_string(),
        text: "Hello from SSE test!".to_string(),
        timestamp: chrono::Utc::now().timestamp(),
        context: None,
    };
    
    // Subscribe to SSE events to see if we receive the message
    let mut rx = sse_manager.subscribe();
    
    // Send the chat message through SSE
    println!("Sending test chat message through SSE...");
    match processor.process_chat_message(&test_message, || true) {
        Ok(()) => println!("Chat message sent successfully"),
        Err(e) => println!("Error sending chat message: {:?}", e),
    }
    
    // Try to receive the message
    println!("Listening for SSE events...");
    tokio::select! {
        msg = rx.recv() => {
            match msg {
                Ok(event) => {
                    println!("Received SSE event:");
                    println!("  Event type: {:?}", event.event_type);
                    println!("  Message: {}", event.message);
                    
                    // Try to parse as chat message
                    if let Some(event_type) = &event.event_type {
                        if event_type == "chat_message" {
                            match serde_json::from_str::<ChatMessage>(&event.message) {
                                Ok(parsed_msg) => {
                                    println!("  Parsed chat message:");
                                    println!("    Username: {}", parsed_msg.username);
                                    println!("    Channel: {}", parsed_msg.channel_name);
                                    println!("    Text: {}", parsed_msg.text);
                                }
                                Err(e) => println!("  Failed to parse chat message: {}", e),
                            }
                        }
                    }
                }
                Err(e) => println!("Error receiving SSE event: {:?}", e),
            }
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
            println!("No SSE events received within 2 seconds");
        }
    }
    
    println!("Test complete");
}
