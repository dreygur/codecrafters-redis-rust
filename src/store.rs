use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::error::RedisError;

pub struct Entry {
    pub value: String,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: String, px: Option<u64>) -> Self {
        Self {
            value,
            expires_at: px.map(|ms| Instant::now() + Duration::from_millis(ms)),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |t| Instant::now() > t)
    }
}

#[derive(Clone)]
pub struct StoreService {
    inner: Arc<Mutex<HashMap<String, Entry>>>,
}

impl StoreService {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut map = self.inner.lock().unwrap();
        match map.get(key) {
            Some(entry) if entry.is_expired() => {
                map.remove(key);
                None
            }
            Some(entry) => Some(entry.value.clone()),
            None => None,
        }
    }

    pub fn set(&self, key: String, value: String, px: Option<u64>) {
        self.inner.lock().unwrap().insert(key, Entry::new(value, px));
    }

    pub fn incr(&self, key: &str) -> Result<i64, RedisError> {
        let mut map = self.inner.lock().unwrap();
        let current = match map.get(key) {
            Some(entry) if entry.is_expired() => {
                map.remove(key);
                0i64
            }
            Some(entry) => match entry.value.parse::<i64>() {
                Ok(n) => n,
                Err(_) => return Err(RedisError::NotAnInteger),
            },
            None => 0i64,
        };
        let new_val = current + 1;
        map.insert(key.to_string(), Entry::new(new_val.to_string(), None));
        Ok(new_val)
    }
}
