use std::time::{SystemTime, UNIX_EPOCH};
use hashbrown::HashMap;
use std::hash::Hash;

#[derive(Debug)]
pub struct TtlLruCache<K, V> {
    map: HashMap<K, (V, u64)>, // value + last-seen timestamp (ms)
    capacity: usize,
    ttl_ms: u64,
}

impl<K, V> TtlLruCache<K, V>
where
    K: Eq + Hash + Copy,
    V: Clone,
{
    pub fn new(capacity: usize, ttl_ms: u64) -> Self {
        Self { map: HashMap::new(), capacity, ttl_ms }
    }

    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    pub fn insert(&mut self, key: K, value: V) {
        let ts = Self::now_ms();
        self.map.insert(key, (value, ts));
        self.prune_if_needed();
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        if let Some((val, ts)) = self.map.get_mut(key) {
            let now = Self::now_ms();
            if now.saturating_sub(*ts) >= self.ttl_ms {
                self.map.remove(key);
                return None;
            }
            *ts = now;
            return Some(val.clone());
        }
        None
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove(key).map(|(v, _)| v)
    }

    fn prune_if_needed(&mut self) {
        let now = Self::now_ms();
        // remove expired
        let mut expired = Vec::new();
        for (k, (_, ts)) in self.map.iter() {
            if now.saturating_sub(*ts) >= self.ttl_ms {
                expired.push(*k);
            }
        }
        for k in expired { self.map.remove(&k); }

        while self.map.len() > self.capacity {
            if let Some((&old_key, _)) = self.map.iter().min_by_key(|(_, (_, ts))| *ts) {
                self.map.remove(&old_key);
            } else { break; }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TtlLruCache;

    #[test]
    fn restore_within_ttl() {
        let mut c = TtlLruCache::new(10, 3_600_000);
        c.insert(1u64, "Alice".to_string());
        let got = c.get(&1).expect("should restore");
        assert_eq!(got, "Alice");
    }

    #[test]
    fn expire_after_ttl() {
        let mut c = TtlLruCache::new(10, 1);
        c.insert(2u64, "Bob".to_string());
        // sleep past ttl
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(c.get(&2).is_none());
    }

    #[test]
    fn prune_oldest_when_capacity_exceeded() {
        let mut c = TtlLruCache::new(3, 3_600_000);
        for i in 0u64..5u64 {
            c.insert(i, format!("P{}", i));
        }
        // ensure capacity respected
        let len = c.map.len();
        assert!(len <= 3, "len is {}", len);
    }
}
