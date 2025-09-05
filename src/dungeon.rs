//! Dungeon state management and lifecycle tracking
//! 
//! This module handles dungeon entity management, dimension network state tracking,
//! and derived state computation for the collapse lifecycle.

use serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Derived state for a dungeon's dimension network lifecycle
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DerivedState {
    /// Dimension is currently open with no scheduled collapse
    Open,
    /// Dimension is open but scheduled to collapse at a future time
    Cleared,
    /// Dimension is currently collapsed/closed
    Closed,
}

/// Dimension network state tracking for dungeon collapse lifecycle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionNetworkState {
    /// Collapse respawn timestamp in epoch milliseconds (0 = no scheduled collapse)
    pub collapse_respawn_timestamp: u64,
    /// Whether the dimension is currently collapsed
    pub is_collapsed: bool,
}

impl DimensionNetworkState {
    /// Create a new dimension network state
    pub fn new(collapse_respawn_timestamp: u64, is_collapsed: bool) -> Self {
        Self {
            collapse_respawn_timestamp,
            is_collapsed,
        }
    }
    
    /// Compute the derived state based on collapse fields and current time
    pub fn derived_state(&self) -> DerivedState {
        self.derived_state_at_time(current_time_ms())
    }
    
    /// Compute the derived state at a specific timestamp (for testing)
    pub fn derived_state_at_time(&self, current_time_ms: u64) -> DerivedState {
        // If collapsed, the dungeon is closed regardless of timestamp
        if self.is_collapsed {
            return DerivedState::Closed;
        }
        
        // If not collapsed, check the collapse timestamp
        match self.collapse_respawn_timestamp {
            0 => DerivedState::Open, // 0 means no scheduled collapse
            future_timestamp if future_timestamp > current_time_ms => DerivedState::Cleared, // Scheduled for future collapse
            _ => DerivedState::Open, // Past timestamp but not collapsed = open (edge case)
        }
    }
    
    /// Check if this state represents a transition from the previous state
    pub fn is_transition_from(&self, previous: &DimensionNetworkState) -> bool {
        self.derived_state() != previous.derived_state()
    }
    
    /// Get a transition description from previous state to current state
    pub fn transition_from(&self, previous: &DimensionNetworkState) -> Option<StateTransition> {
        let from = previous.derived_state();
        let to = self.derived_state();
        
        if from != to {
            Some(StateTransition {
                from,
                to,
                at_timestamp: current_time_ms(),
            })
        } else {
            None
        }
    }
}

/// Represents a state transition event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    pub from: DerivedState,
    pub to: DerivedState,
    pub at_timestamp: u64,
}

/// Get current time in milliseconds since Unix epoch
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Complete dungeon state including entity data and network state
#[derive(Debug, Clone)]
pub struct DungeonState {
    pub entity_id: u64,
    pub dungeon_row: Option<serde_json::Value>, // Raw dungeon_state row
    pub network_state: Option<DimensionNetworkState>,
    pub derived_state: DerivedState,
    pub last_seq: u64,
}

impl DungeonState {
    /// Create a new dungeon state with just entity data
    pub fn new(entity_id: u64, dungeon_row: serde_json::Value) -> Self {
        Self {
            entity_id,
            dungeon_row: Some(dungeon_row),
            network_state: None,
            derived_state: DerivedState::Open, // Default to open if no network state
            last_seq: 0,
        }
    }
    
    /// Update the network state and recompute derived state
    pub fn update_network_state(&mut self, network_state: DimensionNetworkState) -> Option<StateTransition> {
        let old_derived = self.derived_state.clone();
        let new_derived = network_state.derived_state();
        
        self.network_state = Some(network_state);
        self.derived_state = new_derived.clone();
        
        if old_derived != new_derived {
            Some(StateTransition {
                from: old_derived,
                to: new_derived,
                at_timestamp: current_time_ms(),
            })
        } else {
            None
        }
    }
    
    /// Remove network state and reset to open
    pub fn clear_network_state(&mut self) -> Option<StateTransition> {
        if self.network_state.is_some() {
            let old_derived = self.derived_state.clone();
            self.network_state = None;
            self.derived_state = DerivedState::Open;
            
            if old_derived != DerivedState::Open {
                Some(StateTransition {
                    from: old_derived,
                    to: DerivedState::Open,
                    at_timestamp: current_time_ms(),
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_derived_state_open() {
        let state = DimensionNetworkState::new(0, false);
        assert_eq!(state.derived_state(), DerivedState::Open);
    }
    
    #[test]
    fn test_derived_state_closed() {
        let state = DimensionNetworkState::new(1000, true);
        assert_eq!(state.derived_state(), DerivedState::Closed);
        
        // Should be closed regardless of timestamp
        let state = DimensionNetworkState::new(0, true);
        assert_eq!(state.derived_state(), DerivedState::Closed);
    }
    
    #[test]
    fn test_derived_state_cleared() {
        let current_time = current_time_ms();
        let future_time = current_time + 60000; // 1 minute in future
        
        let state = DimensionNetworkState::new(future_time, false);
        assert_eq!(state.derived_state_at_time(current_time), DerivedState::Cleared);
    }
    
    #[test]
    fn test_derived_state_at_specific_time() {
        let test_time = 1900000000000u64; // Fixed timestamp for testing (year 2030)
        let future_time = test_time + 60000;
        let past_time = test_time - 60000;
        
        // Future timestamp + not collapsed = cleared
        let state = DimensionNetworkState::new(future_time, false);
        assert_eq!(state.derived_state_at_time(test_time), DerivedState::Cleared);
        
        // Past timestamp + not collapsed = open (edge case)
        let state = DimensionNetworkState::new(past_time, false);
        assert_eq!(state.derived_state_at_time(test_time), DerivedState::Open);
        
        // Any timestamp + collapsed = closed
        let state = DimensionNetworkState::new(future_time, true);
        assert_eq!(state.derived_state_at_time(test_time), DerivedState::Closed);
    }
    
    #[test]
    fn test_state_transitions() {
        let old_state = DimensionNetworkState::new(0, false); // Open
        let new_state = DimensionNetworkState::new(1900000000000u64, false); // Cleared (year 2030)
        
        assert!(new_state.is_transition_from(&old_state));
        
        let transition = new_state.transition_from(&old_state).unwrap();
        assert_eq!(transition.from, DerivedState::Open);
        assert_eq!(transition.to, DerivedState::Cleared);
    }
    
    #[test]
    fn test_no_transition_same_state() {
        let state1 = DimensionNetworkState::new(0, false); // Open
        let state2 = DimensionNetworkState::new(0, false); // Open
        
        assert!(!state2.is_transition_from(&state1));
        assert!(state2.transition_from(&state1).is_none());
    }
    
    #[test]
    fn test_dungeon_state_management() {
        let mut dungeon = DungeonState::new(123, serde_json::json!({"name": "test"}));
        assert_eq!(dungeon.derived_state, DerivedState::Open);
        
        // Add network state with future timestamp (year 2030)
        let future_timestamp = 1900000000000u64; // Much further in the future
        let network_state = DimensionNetworkState::new(future_timestamp, false);
        let transition = dungeon.update_network_state(network_state);
        
        assert!(transition.is_some());
        assert_eq!(dungeon.derived_state, DerivedState::Cleared);
        
        // Clear network state
        let transition = dungeon.clear_network_state();
        assert!(transition.is_some());
        assert_eq!(dungeon.derived_state, DerivedState::Open);
    }
}