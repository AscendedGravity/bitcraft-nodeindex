use std::sync::Arc;
use bindings::region::DbUpdate;
use hashbrown::{HashMap, HashSet};
use tokio::sync::mpsc::UnboundedReceiver;
use crate::config::AppState;
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
    // update is drained after each apply, so free to re-use.
    let mut updates = HashMap::new();
    // enemy_state needs to be kept across iterations since enemy locations update.
    let mut enemy_state = HashMap::new();

    while let Some(update) = rx.recv().await {
        // location_state should always be inserted in the batch the corresponding entity
        // is added, so the map can be cleared across iterations.
        let mut location_state = HashMap::new();

        // all resources should arrive with location_state inserts
        // deletes are handled via delete on resource_state, no moves should happen here.
        for e in update.location_state.inserts {
            location_state.insert(e.row.entity_id, [e.row.x, e.row.z]);
        }
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

        // build reverse index for enemy_type for entity_id
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
                
                // Send SSE event for player position update
                let _ = sse_processor.process_player_insert(e.row.entity_id as i32, e.row.entity_id, e.row.location_x, e.row.location_z);
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
            }
        }

        // Handle signed-in player state changes
        for e in update.signed_in_player_state.inserts {
            // For signed-in players, we only have entity_id
            player_entity_map.insert(e.row.entity_id, e.row.entity_id);
            
            // Send SSE event for player login (using entity_id as char_id for now)
            let _ = sse_processor.process_player_insert(e.row.entity_id as i32, e.row.entity_id, 0, 0);
        }
        
        for e in update.signed_in_player_state.deletes {
            let entity_id = e.row.entity_id;
            player_entity_map.remove(&entity_id);
            
            // Send SSE event for player logout
            let _ = sse_processor.process_player_delete(entity_id as i32, entity_id);
            
            // Remove from player tracking
            if let Some(player_group) = state.player.get(&1) {
                let mut nodes = player_group.nodes.write().await;
                let mut player_names = player_group.player_names.write().await;
                nodes.remove(&entity_id);
                player_names.remove(&entity_id);
            }
        }

    }
}