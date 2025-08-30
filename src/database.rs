use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bindings::region::DbUpdate;
use hashbrown::{HashMap, HashSet};
use std::hash::Hash;
use tokio::sync::mpsc::UnboundedReceiver;
use crate::config::AppState;
use tracing::info;
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

pub async fn consume(mut rx: UnboundedReceiver<DbUpdate>, state: Arc<AppState>) {
    // updates is drained during each apply, so free to re-use.
    let mut updates = HashMap::new();
    // enemy_state needs to be kept across iterations since enemy locations update.
    let mut enemy_state = HashMap::new();

    while let Some(update) = rx.recv().await {
        // location_state should always be inserted in the batch the corresponding entity
        // is added, so the map can be cleared across iterations.
        let mut location_state = HashMap::new();
        for e in update.location_state.inserts {
            location_state.insert(e.row.entity_id, [e.row.x, e.row.z]);
        }

        // as resources cannot move all inserts and deletes are handled via resource_state.
        for e in update.resource_state.deletes {
            updates.entry(e.row.resource_id)
                .or_insert_with(Update::new)
                .delete
                .insert(e.row.entity_id);
        }
        for e in update.resource_state.inserts {
            let loc = location_state.get(&e.row.entity_id).unwrap().clone();
            updates.entry(e.row.resource_id)
                .or_insert_with(Update::new)
                .insert
                .insert(e.row.entity_id, loc);
        }

        for (res_id, updates) in updates.drain() {
            let Some(map) = state.resource.get(&res_id) else { continue };
            let mut map = map.nodes.write().await;

            map.reserve(updates.additional());
            for e_id in updates.delete { map.remove(&e_id); }
            for (e_id, loc) in updates.insert { map.insert(e_id, loc); }
        }

        // build index for enemy_type for entity_id
        // deletes are handled via enemy_state, but inserts are
        // handled via mobile_entity_state, as they also handle moves
        for e in update.enemy_state.deletes {
            enemy_state.remove(&e.row.entity_id);

            updates.entry(e.row.enemy_type as i32)
                .or_insert_with(Update::new)
                .delete
                .insert(e.row.entity_id);
        }
        for e in update.enemy_state.inserts {
            enemy_state.insert(e.row.entity_id, e.row.enemy_type as i32);
        }
        for e in update.mobile_entity_state.inserts {
            let mob_id = enemy_state.get(&e.row.entity_id).unwrap().clone();
            updates.entry(mob_id)
                .or_insert(Update::new())
                .insert
                .insert(e.row.entity_id, [e.row.location_x, e.row.location_z]);
        }

        for (mob_id, updates) in updates.drain() {
            let Some(map) = state.enemy.get(&mob_id) else { continue };
            let mut map = map.nodes.write().await;

            map.reserve(updates.additional());
            for e_id in updates.delete { map.remove(&e_id); }
            for (e_id, loc) in updates.insert { map.insert(e_id, loc); }
        }
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
    // last-known usernames cache so we can restore a username quickly when a player signs in
    // Use a TTL+LRU-style cache (timestamp-based) so we retain recent players but evict old ones.
    #[derive(Debug)]
    struct TtlLruCache<K, V> {
        map: HashMap<K, (V, u64)>, // value + last-seen timestamp (ms)
        capacity: usize,
        ttl_ms: u64,
    }

    impl<K, V> TtlLruCache<K, V>
    where
        K: Eq + Hash + Copy,
        V: Clone,
    {
        fn new(capacity: usize, ttl_ms: u64) -> Self {
            Self { map: HashMap::new(), capacity, ttl_ms }
        }

        fn now_ms() -> u64 {
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
        }

        fn insert(&mut self, key: K, value: V) {
            let ts = Self::now_ms();
            self.map.insert(key, (value, ts));
            self.prune_if_needed();
        }

        fn get(&mut self, key: &K) -> Option<V> {
            if let Some((val, ts)) = self.map.get_mut(key) {
                let now = Self::now_ms();
                // Evict if expired
                if now.saturating_sub(*ts) >= self.ttl_ms {
                    self.map.remove(key);
                    return None;
                }
                // update last-seen timestamp
                *ts = now;
                return Some(val.clone());
            }
            None
        }

        fn remove(&mut self, key: &K) -> Option<V> {
            self.map.remove(key).map(|(v, _)| v)
        }

        fn prune_if_needed(&mut self) {
            let now = Self::now_ms();
            // First remove expired entries
            let mut expired = Vec::new();
            for (k, (_, ts)) in self.map.iter() {
                if now.saturating_sub(*ts) >= self.ttl_ms {
                    expired.push(*k);
                }
            }
            for k in expired { self.map.remove(&k); }

            // Enforce capacity by removing least-recently-seen entries (smallest timestamp)
            while self.map.len() > self.capacity {
                if let Some((&old_key, _)) = self.map.iter().min_by_key(|(_, (_, ts))| *ts) {
                    self.map.remove(&old_key);
                } else {
                    break;
                }
            }
        }
    }

    // TTL configuration for shared last-known names in `AppState`
    const LAST_KNOWN_TTL_MS: u64 = 3_600_000; // 1 hour
    const LAST_KNOWN_CAPACITY: usize = 3000; // soft capacity per player group (not enforced globally)
    // player event throttling: track last SSE event time per player
    let mut player_last_sse_time: HashMap<u64, u64> = HashMap::new();
    // global throttling: track last SSE event time for any player
    let mut global_last_player_event_time: u64 = 0;
    
    // Throttling configuration
    const PLAYER_MOVEMENT_THROTTLE_MS: u64 = 3000; // Max 1 movement event per 3 seconds per player
    const GLOBAL_PLAYER_EVENT_THROTTLE_MS: u64 = 1200; // Min 1200ms between any player events (longer than frontend's 1000ms debounce)

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

        // Process player-related updates using correct table names
        // Handle player username updates
        for e in update.player_username_state.inserts {
            // Store username by entity_id
            if let Some(player_group) = state.player.get(&1) { // Using player ID 1 for "All Players"
                let mut player_names = player_group.player_names.write().await;
                player_names.insert(e.row.entity_id, e.row.username.clone());
                // update last-known map in shared state so we can restore it on sign-in
                let mut last_known = player_group.last_known_names.write().await;
                let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                last_known.insert(e.row.entity_id, (e.row.username.clone(), ts));
                // Log username insertion for diagnostics
                info!("player_username_state.insert: entity_id={} username={}", e.row.entity_id, e.row.username);
            }
        }

        // Handle signed-in player state changes
        for e in update.signed_in_player_state.inserts {
            // For signed-in players, we only have entity_id
            player_entity_map.insert(e.row.entity_id, e.row.entity_id);
            // Ensure we have a username available immediately: restore last-known username if present
            if let Some(player_group) = state.player.get(&1) {
                let mut player_names = player_group.player_names.write().await;
                if !player_names.contains_key(&e.row.entity_id) {
                    // Try to restore from shared last-known map
                    if let Some(player_group) = state.player.get(&1) {
                        let mut last_known = player_group.last_known_names.write().await;
                        if let Some((name, ts)) = last_known.get_mut(&e.row.entity_id) {
                            // Check TTL
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            if now.saturating_sub(*ts) < LAST_KNOWN_TTL_MS {
                                player_names.insert(e.row.entity_id, name.clone());
                                // refresh timestamp
                                *ts = now;
                                info!("signed_in_player_state.insert: restored last-known username for entity_id={} -> {}", e.row.entity_id, name);
                            } else {
                                // expired, remove it
                                last_known.remove(&e.row.entity_id);
                            }
                        }
                        // Optionally prune oldest entries if capacity exceeded
                        if last_known.len() > LAST_KNOWN_CAPACITY {
                            // remove least-recently-seen entries
                            if let Some((&old_key, _)) = last_known.iter().min_by_key(|(_, (_, ts))| *ts) {
                                last_known.remove(&old_key);
                            }
                        }
                    }
                }
            }
            
            // Send SSE event for player login immediately (but respect global throttle)
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            if current_time - global_last_player_event_time >= GLOBAL_PLAYER_EVENT_THROTTLE_MS {
                let _ = sse_processor.process_player_insert(1, e.row.entity_id, 0, 0);
                global_last_player_event_time = current_time;
            }
            
            // Reset throttling timer for this player to allow movement updates sooner
            player_last_sse_time.insert(e.row.entity_id, current_time - PLAYER_MOVEMENT_THROTTLE_MS);
        }
        
        for e in update.signed_in_player_state.deletes {
            let entity_id = e.row.entity_id;
            player_entity_map.remove(&entity_id);
            
            // Send SSE event for player logout (respect global throttle)
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            if current_time - global_last_player_event_time >= GLOBAL_PLAYER_EVENT_THROTTLE_MS {
                let _ = sse_processor.process_player_delete(1, entity_id);
                global_last_player_event_time = current_time;
            }
            
            // Clean up throttling tracking for logged out player
            player_last_sse_time.remove(&entity_id);
            
                // Remove from player tracking but keep last-known in case they sign back in soon
            if let Some(player_group) = state.player.get(&1) {
                let mut nodes = player_group.nodes.write().await;
                let mut player_names = player_group.player_names.write().await;
                nodes.remove(&entity_id);
                player_names.remove(&entity_id);
                info!("signed_in_player_state.delete: entity_id={} - removed tracking and username", entity_id);
            }
        }

    }
}