use std::collections::HashMap;

use super::identifier::Key;

pub type Value = Vec<u8>;

#[derive(Debug)]
pub struct Storage {
    map: HashMap<Key, Value>,
}

impl Storage {
    pub fn new() -> Self {
        Storage { map: HashMap::new() }
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: &Key) -> Option<&Value> {
        self.map.get(key)
    }

    pub fn remove(&mut self, key: &Key) -> Option<Value> {
        self.map.remove(key)
    }

    pub fn contains(&self, key: &Key) -> bool {
        self.map.contains_key(key)
    }
}

