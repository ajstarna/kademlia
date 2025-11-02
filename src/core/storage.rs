use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::identifier::Key;

pub type Value = Vec<u8>;

const DEFAULT_TTL: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours

#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expires_at: Instant,
}

#[derive(Debug)]
pub struct Storage {
    map: HashMap<Key, Entry>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        let expires_at = Instant::now() + DEFAULT_TTL;
        let entry = Entry { value, expires_at };
        self.map.insert(key, entry);
    }

    #[cfg(test)]
    pub fn insert_with_ttl(&mut self, key: Key, value: Value, ttl: Duration) {
        let expires_at = Instant::now() + ttl;
        let entry = Entry { value, expires_at };
        self.map.insert(key, entry);
    }

    pub fn get(&self, key: &Key) -> Option<&Value> {
        match self.map.get(key) {
            Some(entry) if entry.expires_at > Instant::now() => Some(&entry.value),
            _ => None,
        }
    }

    pub fn remove(&mut self, key: &Key) -> Option<Value> {
        self.map.remove(key).map(|e| e.value)
    }

    pub fn contains(&self, key: &Key) -> bool {
        match self.map.get(key) {
            Some(entry) if entry.expires_at > Instant::now() => true,
            _ => false,
        }
    }

    pub fn purge_expired(&mut self) {
        let now = Instant::now();
        self.map.retain(|_, entry| entry.expires_at > now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::identifier::NodeID;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn value_expires_and_is_purged() {
        let mut storage = Storage::new();
        let key: Key = NodeID::new();
        let val: Value = b"hello".to_vec();

        // Insert with a very short TTL
        storage.insert_with_ttl(key, val.clone(), Duration::from_millis(10));

        // Immediately available
        assert_eq!(storage.get(&key), Some(&val));

        // Wait for TTL to pass
        thread::sleep(Duration::from_millis(20));

        // Expired entries are invisible to get/contains
        assert_eq!(storage.get(&key), None);
        assert!(!storage.contains(&key));

        // Purge removes the expired entry physically
        storage.purge_expired();
        assert!(!storage.map.contains_key(&key));

        // Removing now yields None
        assert_eq!(storage.remove(&key), None);
    }
}
