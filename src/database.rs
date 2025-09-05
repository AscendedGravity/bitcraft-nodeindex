use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bindings::region::DbUpdate;
use hashbrown::{HashMap, HashSet};
use tokio::sync::mpsc::UnboundedReceiver;
use crate::config::{AppState, ChatMessage};
use crate::sse::{SseManager, SseMessageProcessor};

struct Update {
    insert: HashMap<u64, [i32; 2]>,
    delete: HashSet<u64>,
}
impl Update {
    fn new() -> Self { Self { insert: HashMap::new(), delete: HashSet::new() } }
    fn additional(&self) -> usize {
        let insert = self.insert.len();
        let delete = self.delete.len();

        if insert > delete { insert - delete } else { 0 }
    }
}


pub async fn consume_with_sse(mut rx: UnboundedReceiver<DbUpdate>, state: Arc<AppState>, sse_manager: SseManager) {
    // Create SSE message processor for handling event sending
    let sse_processor = SseMessageProcessor::new(sse_manager);
    
    // update is drained after each apply, so free to re-use.
    let mut updates = HashMap::new();
    // enemy_state needs to be kept across iterations since enemy locations update.
    let mut enemy_state = HashMap::new();
    // player tracking for entity ID mapping (signed-in players)
    let mut player_entity_map: HashMap<u64, u64> = HashMap::new();
    
    // Event buffering system for handling race conditions
    #[derive(Debug, Clone)]
    struct PendingLoginEvent {
        login_time: u64,
        sse_sent: bool,
    }
    
    // Buffer for login events waiting for username events
    let mut pending_login_events: HashMap<u64, PendingLoginEvent> = HashMap::new();
    
    // player event throttling: track last SSE event time per player
    let mut player_last_sse_time: HashMap<u64, u64> = HashMap::new();
    // global throttling: track last SSE event time for any player
    let mut global_last_player_event_time: u64 = 0;
    
    // Configuration for event buffering
    const LOGIN_USERNAME_BUFFER_MS: u64 = 5000; // Wait up to 5 seconds for username after login
    const BUFFER_CLEANUP_INTERVAL: u64 = 10000; // Clean up expired buffers every 10 seconds
    let mut last_buffer_cleanup: u64 = 0;
    
    // Throttling configuration
    const PLAYER_MOVEMENT_THROTTLE_MS: u64 = 3000; // Max 1 movement event per 3 seconds per player
    const GLOBAL_PLAYER_EVENT_THROTTLE_MS: u64 = 1200; // Min 1200ms between any player events (longer than frontend's 1000ms debounce)
    
    // Helper function to get current timestamp in milliseconds
    let now_ms = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

    // TTL configuration for shared last-known names in `AppState`
    const LAST_KNOWN_TTL_MS: u64 = 7 * 24 * 3_600_000; // 7 days (much longer for better player experience)
    const LAST_KNOWN_CAPACITY: usize = 10_000; // higher capacity for popular servers

    // Chat throttling
    let mut last_chat_event_time: u64 = 0;
    let chat_throttle_ms = state.chat.config.throttle_ms;

    // Throttling helper function
    fn is_throttled(last_event_time: &mut u64, throttle_ms: u64, now: u64) -> bool {
        if now.saturating_sub(*last_event_time) < throttle_ms {
            true
        } else {
            *last_event_time = now;
            false
        }
    }

    while let Some(update) = rx.recv().await {
        // location_state should always be inserted in the batch the corresponding entity
        // is added, so the map can be cleared across iterations.
        let mut location_state = HashMap::new();

        // Process location updates
        for e in update.location_state.inserts {
            location_state.insert(e.row.entity_id, [e.row.x, e.row.z]);
        }

        // Process resource updates with SSE events
        for e in update.resource_state.deletes {
            updates.entry(e.row.resource_id)
                .or_insert_with(Update::new)
                .delete
                .insert(e.row.entity_id);
            
            // Send SSE event for resource deletion
            let _ = sse_processor.process_resource_delete(e.row.resource_id, e.row.entity_id);
        }
        for e in update.resource_state.inserts {
            let loc = location_state.get(&e.row.entity_id).unwrap().clone();
            updates.entry(e.row.resource_id)
                .or_insert_with(Update::new)
                .insert
                .insert(e.row.entity_id, loc);
            
            // Send SSE event for resource insertion
            let _ = sse_processor.process_resource_insert(e.row.resource_id, e.row.entity_id, loc[0], loc[1]);
        }

        // Apply resource updates to state
        for (res_id, updates) in updates.drain() {
            let Some(map) = state.resource.get(&res_id) else { continue };
            let mut map = map.nodes.write().await;

            map.reserve(updates.additional());
            for e_id in updates.delete { map.remove(&e_id); }
            for (e_id, loc) in updates.insert { map.insert(e_id, loc); }
        }

        // Process enemy updates with SSE events
        for e in update.enemy_state.deletes {
            enemy_state.remove(&e.row.entity_id);

            updates.entry(e.row.enemy_type as i32)
                .or_insert_with(Update::new)
                .delete
                .insert(e.row.entity_id);
            
            // Send SSE event for enemy deletion
            let _ = sse_processor.process_enemy_delete(e.row.enemy_type as i32, e.row.entity_id);
        }
        for e in update.enemy_state.inserts {
            enemy_state.insert(e.row.entity_id, e.row.enemy_type as i32);
        }
        for e in update.mobile_entity_state.inserts {
            // Check if this is an enemy entity
            if let Some(&mob_id) = enemy_state.get(&e.row.entity_id) {
                updates.entry(mob_id)
                    .or_insert(Update::new())
                    .insert
                    .insert(e.row.entity_id, [e.row.location_x, e.row.location_z]);
                
                // Send SSE event for enemy movement/insertion
                let _ = sse_processor.process_enemy_insert(mob_id, e.row.entity_id, e.row.location_x, e.row.location_z);
            }
            
            // Check if this entity belongs to a signed-in player
            if player_entity_map.contains_key(&e.row.entity_id) {
                // Update player position
                if let Some(player_group) = state.player.get(&1) {
                    let mut nodes = player_group.nodes.write().await;
                    nodes.insert(e.row.entity_id, [e.row.location_x, e.row.location_z]);
                }
                
                // Send throttled SSE event for player movement
                let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                let last_event_time = player_last_sse_time.get(&e.row.entity_id).copied().unwrap_or(0);
                
                // Check both per-player throttle AND global throttle
                if current_time - last_event_time >= PLAYER_MOVEMENT_THROTTLE_MS && 
                   current_time - global_last_player_event_time >= GLOBAL_PLAYER_EVENT_THROTTLE_MS {
                    let _ = sse_processor.process_player_insert(1, e.row.entity_id, e.row.location_x, e.row.location_z);
                    player_last_sse_time.insert(e.row.entity_id, current_time);
                    global_last_player_event_time = current_time;
                    
                    // Log movement events at debug level (suppressed from console)
                    tracing::debug!("mobile_entity_state.insert: PLAYER MOVEMENT SSE entity_id={} location=[{}, {}]", 
                                   e.row.entity_id, e.row.location_x, e.row.location_z);
                } else {
                    // Calculate throttle wait times for debugging (logging disabled to reduce console noise)
                    let player_wait = PLAYER_MOVEMENT_THROTTLE_MS.saturating_sub(current_time - last_event_time);
                    let global_wait = GLOBAL_PLAYER_EVENT_THROTTLE_MS.saturating_sub(current_time - global_last_player_event_time);
                    let _max_wait = player_wait.max(global_wait);

                    // NOTE: previously we logged throttled player movement here. That log was removed to keep console output cleaner.
                }
            }
        }

        // Apply enemy updates to state
        for (mob_id, updates) in updates.drain() {
            let Some(map) = state.enemy.get(&mob_id) else { continue };
            let mut map = map.nodes.write().await;

            map.reserve(updates.additional());
            for e_id in updates.delete { map.remove(&e_id); }
            for (e_id, loc) in updates.insert { map.insert(e_id, loc); }
        }

        // Process portal state updates for dungeons tracking
        for e in update.portal_state.inserts {
            tracing::debug!("portal_state.insert: Processing portal entity_id={} enabled={}", e.row.entity_id, e.row.enabled);
            
            // Check if this portal entity_id matches any of our tracked dungeon type IDs
            if let Some(_dungeon_entry) = state.dungeons_list.iter().find(|d| d.id == e.row.entity_id) {
                // This portal matches a configured dungeon type - process it
                if let Some(dungeon_group) = state.dungeon.get(&e.row.entity_id) {
                    let mut nodes = dungeon_group.nodes.write().await;
                    
                    // Portal enabled means active - enabled=true means portal is active
                    if e.row.enabled {
                        // Portal is enabled/active - add to tracking
                        // Location coordinates are in the portal_state row as destination_x/destination_z
                        let loc = [e.row.destination_x, e.row.destination_z];
                        nodes.insert(e.row.entity_id, loc);
                        tracing::info!("portal_state.insert: PORTAL ENABLED entity_id={} dungeon_type={} location=[{}, {}]", 
                                     e.row.entity_id, e.row.entity_id, loc[0], loc[1]);
                        
                        // Send SSE event for portal state change (enabled)
                        let _ = sse_processor.process_portal_state_change_with_dungeon(
                            e.row.entity_id, e.row.entity_id, true, Some(loc[0]), Some(loc[1])
                        );
                        
                        // Also send the original dungeon insert event for backwards compatibility
                        let _ = sse_processor.process_dungeon_insert(e.row.entity_id, e.row.entity_id, loc[0], loc[1]);
                    } else {
                        // Portal is disabled - remove from tracking
                        if nodes.remove(&e.row.entity_id).is_some() {
                            tracing::info!("portal_state.insert: PORTAL DISABLED entity_id={} dungeon_type={} (removed from tracking)", 
                                         e.row.entity_id, e.row.entity_id);
                            
                            // Send SSE event for portal state change (disabled)
                            let _ = sse_processor.process_portal_state_change_with_dungeon(
                                e.row.entity_id, e.row.entity_id, false, None, None
                            );
                            
                            // Also send the original dungeon delete event for backwards compatibility
                            let _ = sse_processor.process_dungeon_delete(e.row.entity_id, e.row.entity_id);
                        } else {
                            tracing::debug!("portal_state.insert: PORTAL DISABLED entity_id={} but was not being tracked", e.row.entity_id);
                        }
                    }
                } else {
                    tracing::warn!("portal_state.insert: Dungeon group not found for entity_id={}", e.row.entity_id);
                }
            } else {
                tracing::debug!("portal_state.insert: Portal entity_id={} not in dungeon config", e.row.entity_id);
            }
        }

        // NOTE: We only process portal_state inserts, not deletes
        // Portal state changes are reflected in insert events with enabled flag changes

    // === Claim & Empire name cache updates for chat context ===
        // Process claim state updates for chat context
        for e in update.claim_state.inserts {
            let mut claim_names = state.chat.claim_names.write().await;
            claim_names.insert(e.row.entity_id, e.row.name.clone());
        }
        for e in update.claim_state.deletes {
            let mut claim_names = state.chat.claim_names.write().await;
            claim_names.remove(&e.row.entity_id);
        }

        // Process empire state updates for chat context
        for e in update.empire_state.inserts {
            let mut empire_names = state.chat.empire_names.write().await;
            empire_names.insert(e.row.entity_id, e.row.name.clone());
        }
        for e in update.empire_state.deletes {
            let mut empire_names = state.chat.empire_names.write().await;
            empire_names.remove(&e.row.entity_id);
        }

        // Process player-related updates using correct table names
        // Handle player username updates
        for e in update.player_username_state.inserts {
            // Store username by entity_id
            if let Some(player_group) = state.player.get(&1) { // Using player ID 1 for "All Players"
                let mut player_names = player_group.player_names.write().await;
                let mut last_known = player_group.last_known_names.write().await;
                
                // Check if this is a new username or an update
                let is_new_player = !player_names.contains_key(&e.row.entity_id);
                let was_in_cache = last_known.contains_key(&e.row.entity_id);
                
                player_names.insert(e.row.entity_id, e.row.username.clone());
                
                // update last-known map in shared state so we can restore it on sign-in
                let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                last_known.insert(e.row.entity_id, (e.row.username.clone(), ts));
                
                // Check if this username event resolves a buffered login event
                let was_buffered = if let Some(pending) = pending_login_events.remove(&e.row.entity_id) {
                    let current_time = now_ms();
                    let wait_time = current_time.saturating_sub(pending.login_time);
                    tracing::debug!("event_buffer: USERNAME ARRIVED for buffered login entity_id={} username={} (buffered_for={}ms)", 
                                   e.row.entity_id, e.row.username, wait_time);
                    
                    // Send delayed SSE login event now that we have the username
                    if !pending.sse_sent {
                        if current_time - global_last_player_event_time >= GLOBAL_PLAYER_EVENT_THROTTLE_MS {
                            let _ = sse_processor.process_player_insert(1, e.row.entity_id, 0, 0);
                            global_last_player_event_time = current_time;
                            tracing::debug!("event_buffer: SSE LOGIN EVENT SENT (delayed) entity_id={} username={}", e.row.entity_id, e.row.username);
                        } else {
                            let wait_delay = GLOBAL_PLAYER_EVENT_THROTTLE_MS - (current_time - global_last_player_event_time);
                            tracing::debug!("event_buffer: SSE LOGIN EVENT STILL THROTTLED entity_id={} username={} (wait_{}ms)", 
                                           e.row.entity_id, e.row.username, wait_delay);
                        }
                    }
                    true
                } else {
                    false
                };
                
                // Enhanced logging for player state management (at debug level)
                if is_new_player {
                    tracing::debug!("player_username_state.insert: NEW PLAYER entity_id={} username={} (cache_status={}, buffered_login={})", 
                                   e.row.entity_id, e.row.username, 
                                   if was_in_cache { "restored" } else { "fresh" },
                                   was_buffered);
                } else {
                    tracing::debug!("player_username_state.insert: EXISTING PLAYER entity_id={} username={} (username_update, buffered_login={})", 
                                   e.row.entity_id, e.row.username, was_buffered);
                }
                
                // Log cache statistics periodically (at debug level to reduce noise)
                if player_names.len() % 100 == 0 {
                    tracing::debug!("player_cache_stats: active_players={} last_known_cache={} cache_utilization={:.1}%", 
                                   player_names.len(), last_known.len(), 
                                   (last_known.len() as f64 / LAST_KNOWN_CAPACITY as f64) * 100.0);
                }
            }
        }

        // Handle signed-in player state changes
        for e in update.signed_in_player_state.inserts {
            // For signed-in players, we only have entity_id
            player_entity_map.insert(e.row.entity_id, e.row.entity_id);
            
            // Player login event (debug level to reduce console noise)
            tracing::debug!("signed_in_player_state.insert: PLAYER LOGIN entity_id={} (tracking {} total players)", 
                           e.row.entity_id, player_entity_map.len());
            
            // Ensure we have a username available immediately: restore last-known username if present
            let mut should_buffer_login = false;
            let mut username_available = false;
            
            if let Some(player_group) = state.player.get(&1) {
                let mut player_names = player_group.player_names.write().await;
                let has_current_username = player_names.contains_key(&e.row.entity_id);
                
                if !has_current_username {
                    // Try to restore from shared last-known map
                    if let Some(player_group) = state.player.get(&1) {
                        let mut last_known = player_group.last_known_names.write().await;
                        if let Some((name, ts)) = last_known.get_mut(&e.row.entity_id) {
                            // Check TTL
                            let now = now_ms();
                            let age_ms = now.saturating_sub(*ts);
                            
                            if age_ms < LAST_KNOWN_TTL_MS {
                                player_names.insert(e.row.entity_id, name.clone());
                                // refresh timestamp
                                *ts = now;
                                username_available = true;
                                tracing::debug!("signed_in_player_state.insert: USERNAME RESTORED entity_id={} username={} (cached_age={}ms)", 
                                               e.row.entity_id, name, age_ms);
                            } else {
                                // expired, remove it
                                let expired_name = last_known.remove(&e.row.entity_id).unwrap().0;
                                should_buffer_login = true;
                                tracing::debug!("signed_in_player_state.insert: USERNAME CACHE EXPIRED entity_id={} username={} (age={}ms, ttl={}ms) - buffering login", 
                                               e.row.entity_id, expired_name, age_ms, LAST_KNOWN_TTL_MS);
                            }
                        } else {
                            should_buffer_login = true;
                            tracing::debug!("signed_in_player_state.insert: NO CACHED USERNAME entity_id={} - buffering login for username event", 
                                           e.row.entity_id);
                        }
                        
                        // Optionally prune oldest entries if capacity exceeded
                        if last_known.len() > LAST_KNOWN_CAPACITY {
                            // remove least-recently-seen entries (more aggressively to prevent frequent capacity hits)
                            let initial_size = last_known.len();
                            let target_size = (LAST_KNOWN_CAPACITY as f64 * 0.8) as usize; // Reduce to 80% of capacity
                            let mut evicted_count = 0;
                            let now = now_ms();
                            
                            while last_known.len() > target_size {
                                if let Some((&old_key, _)) = last_known.iter().min_by_key(|(_, (_, ts))| *ts) {
                                    let (old_name, old_ts) = last_known.remove(&old_key).unwrap();
                                    let old_age = now.saturating_sub(old_ts);
                                    tracing::debug!("signed_in_player_state.insert: CACHE EVICTION entity_id={} username={} (age={}ms, reason=capacity_management)", 
                                                   old_key, old_name, old_age);
                                    evicted_count += 1;
                                } else {
                                    break;
                                }
                            }
                            tracing::debug!("signed_in_player_state.insert: CACHE PRUNING COMPLETE evicted={} entries ({}→{} entries, utilization={:.1}%)", 
                                           evicted_count, initial_size, last_known.len(), 
                                           (last_known.len() as f64 / LAST_KNOWN_CAPACITY as f64) * 100.0);
                        }
                    }
                } else {
                    username_available = true;
                    tracing::debug!("signed_in_player_state.insert: USERNAME ALREADY AVAILABLE entity_id={}", e.row.entity_id);
                }
            }
            
            // Handle SSE event sending with event buffering
            if should_buffer_login {
                // Buffer this login event - wait for username event before sending SSE
                let current_time = now_ms();
                pending_login_events.insert(e.row.entity_id, PendingLoginEvent {
                    login_time: current_time,
                    sse_sent: false,
                });
                tracing::debug!("event_buffer: BUFFERING LOGIN EVENT entity_id={} (waiting_for_username, buffer_count={})", 
                               e.row.entity_id, pending_login_events.len());
            } else if username_available {
                // Send SSE event for player login immediately (username is available)
                let current_time = now_ms();
                if current_time - global_last_player_event_time >= GLOBAL_PLAYER_EVENT_THROTTLE_MS {
                    let _ = sse_processor.process_player_insert(1, e.row.entity_id, 0, 0);
                    global_last_player_event_time = current_time;
                    tracing::debug!("signed_in_player_state.insert: SSE LOGIN EVENT SENT (immediate) entity_id={}", e.row.entity_id);
                } else {
                    let wait_time = GLOBAL_PLAYER_EVENT_THROTTLE_MS - (current_time - global_last_player_event_time);
                    tracing::debug!("signed_in_player_state.insert: SSE LOGIN EVENT THROTTLED entity_id={} (wait_{}ms)", 
                                   e.row.entity_id, wait_time);
                }
            }
            
            // Reset throttling timer for this player to allow movement updates sooner
            let current_time = now_ms();
            player_last_sse_time.insert(e.row.entity_id, current_time - PLAYER_MOVEMENT_THROTTLE_MS);
        }
        
        for e in update.signed_in_player_state.deletes {
            let entity_id = e.row.entity_id;
            player_entity_map.remove(&entity_id);
            
            // Remove from pending login buffer if present
            if let Some(_) = pending_login_events.remove(&entity_id) {
                tracing::debug!("event_buffer: REMOVED LOGOUT FROM BUFFER entity_id={} (logged_out_before_username_arrived)", entity_id);
            }
            
            // Player logout event (debug level to reduce console noise)
            tracing::debug!("signed_in_player_state.delete: PLAYER LOGOUT entity_id={} (tracking {} remaining players)", 
                           entity_id, player_entity_map.len());
            
            // Send SSE event for player logout (respect global throttle)
            let current_time = now_ms();
            if current_time - global_last_player_event_time >= GLOBAL_PLAYER_EVENT_THROTTLE_MS {
                let _ = sse_processor.process_player_delete(1, entity_id);
                global_last_player_event_time = current_time;
                tracing::debug!("signed_in_player_state.delete: SSE LOGOUT EVENT SENT entity_id={}", entity_id);
            } else {
                let wait_time = GLOBAL_PLAYER_EVENT_THROTTLE_MS - (current_time - global_last_player_event_time);
                tracing::debug!("signed_in_player_state.delete: SSE LOGOUT EVENT THROTTLED entity_id={} (wait_{}ms)", 
                               entity_id, wait_time);
            }
            
            // Clean up throttling tracking for logged out player
            player_last_sse_time.remove(&entity_id);
            
            // Remove from player tracking but keep last-known in case they sign back in soon
            if let Some(player_group) = state.player.get(&1) {
                let mut nodes = player_group.nodes.write().await;
                let mut player_names = player_group.player_names.write().await;
                let removed_name = player_names.remove(&entity_id);
                nodes.remove(&entity_id);
                
                if let Some(name) = removed_name {
                    tracing::debug!("signed_in_player_state.delete: CLEANUP COMPLETE entity_id={} username={} (keeping last-known cache for future signin)", 
                                   entity_id, name);
                } else {
                    tracing::debug!("signed_in_player_state.delete: CLEANUP COMPLETE entity_id={} (no username was cached)", entity_id);
                }
            }
        }

        // Clean up expired buffer entries periodically
        {
            let current_time = now_ms();
            if current_time - last_buffer_cleanup >= BUFFER_CLEANUP_INTERVAL {
                let initial_count = pending_login_events.len();
                pending_login_events.retain(|&entity_id, pending| {
                    let age = current_time.saturating_sub(pending.login_time);
                    if age >= LOGIN_USERNAME_BUFFER_MS {
                        tracing::debug!("event_buffer: EXPIRED LOGIN BUFFER entity_id={} (waited_{}ms, no_username_event)", 
                                       entity_id, age);
                        false // remove expired entry
                    } else {
                        true // keep entry
                    }
                });
                
                let expired_count = initial_count - pending_login_events.len();
                if expired_count > 0 {
                    tracing::debug!("event_buffer: CLEANUP COMPLETE expired={} remaining={}", expired_count, pending_login_events.len());
                }
                last_buffer_cleanup = current_time;
            }
        }

    // === Chat message processing ===
        if state.chat.config.enabled {
            for e in update.chat_message_state.inserts {
                // Filter channels if configured
                if !state.chat.config.channels.is_empty() && !state.chat.config.channels.contains(&e.row.channel_id) {
                    continue;
                }

                // Determine channel name & context lookup
                let (channel_name, context_lookup) = match e.row.channel_id {
                    2 => ("Empire Internal".to_string(), {
                        let empire_names = state.chat.empire_names.read().await;
                        empire_names.get(&e.row.target_id).cloned()
                    }),
                    3 => ("Region".to_string(), None),
                    4 => ("Claim".to_string(), {
                        let claim_names = state.chat.claim_names.read().await;
                        claim_names.get(&e.row.target_id).cloned()
                    }),
                    5 => ("Empire Public".to_string(), {
                        let empire_names = state.chat.empire_names.read().await;
                        empire_names.get(&e.row.target_id).cloned()
                    }),
                    other => (format!("Unknown ({})", other), None),
                };

                // NOTE: e.row.timestamp is i32; treat as seconds since epoch (adjust if spec differs)
                let mut chat_message = ChatMessage {
                    entity_id: e.row.entity_id,
                    channel_id: e.row.channel_id,
                    channel_name: channel_name.clone(),
                    target_id: e.row.target_id,
                    username: e.row.username.clone(),
                    text: e.row.text.clone(),
                    timestamp: e.row.timestamp as i64,
                    context: if state.chat.config.include_context { context_lookup } else { None },
                };

                // Truncate message if needed
                if state.chat.config.max_message_length > 0 && chat_message.text.len() > state.chat.config.max_message_length {
                    if state.chat.config.max_message_length > 3 { // ensure room for ellipsis
                        chat_message.text.truncate(state.chat.config.max_message_length - 3);
                        chat_message.text.push_str("...");
                    } else {
                        chat_message.text.truncate(state.chat.config.max_message_length);
                    }
                }

                // Store in timestamp-ordered recent buffer (capacity 100)
                // Keep messages sorted by timestamp and only retain the most recent 100
                {
                    let mut recent = state.chat.recent_messages.write().await;
                    
                    // Insert message in timestamp-sorted order
                    let insert_pos = recent.binary_search_by_key(&chat_message.timestamp, |msg| msg.timestamp)
                        .unwrap_or_else(|pos| pos);
                    recent.insert(insert_pos, chat_message.clone());
                    
                    // Trim to keep only the most recent 100 messages by timestamp
                    if recent.len() > 100 {
                        // Remove the oldest messages (lowest timestamps)
                        let excess = recent.len() - 100;
                        for _ in 0..excess {
                            recent.pop_front();
                        }
                    }
                    
                    tracing::debug!("chat_message_state.insert: stored message with timestamp {} in buffer (size: {})", 
                                   chat_message.timestamp, recent.len());
                }

                // Throttle check using helper function
                let current_time = now_ms();
                if is_throttled(&mut last_chat_event_time, chat_throttle_ms, current_time) {
                    continue;
                }

                if let Err(err) = sse_processor.process_chat_message(&chat_message, || true) {
                    tracing::warn!("Failed to send chat SSE event: {:?}", err);
                } else {
                    tracing::debug!("chat_message_state.insert: CHAT SSE SENT channel={} user={} text={}", chat_message.channel_name, chat_message.username, chat_message.text);
                }
            }
        }
    }
}