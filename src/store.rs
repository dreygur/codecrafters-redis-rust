use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{error::RedisError, sorted_set::SortedSet};

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
    strings: Arc<Mutex<HashMap<String, Entry>>>,
    zsets: Arc<Mutex<HashMap<String, SortedSet>>>,
}

impl StoreService {
    pub fn new() -> Self {
        Self {
            strings: Arc::new(Mutex::new(HashMap::new())),
            zsets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // -- string operations --

    pub fn get(&self, key: &str) -> Option<String> {
        let mut map = self.strings.lock().unwrap();
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
        self.strings
            .lock()
            .unwrap()
            .insert(key, Entry::new(value, px));
    }

    pub fn incr(&self, key: &str) -> Result<i64, RedisError> {
        let mut map = self.strings.lock().unwrap();
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

    // -- sorted set operations --

    /// Adds/updates members. Returns the number of newly added members.
    pub fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> i64 {
        let mut map = self.zsets.lock().unwrap();
        let zset = map.entry(key.to_string()).or_insert_with(SortedSet::new);
        pairs
            .into_iter()
            .filter(|(score, member)| zset.add(*score, member.clone()))
            .count() as i64
    }

    /// Returns the 0-based rank of a member, or `None` if not found.
    pub fn zrank(&self, key: &str, member: &str) -> Option<i64> {
        self.zsets
            .lock()
            .unwrap()
            .get(key)?
            .rank(member)
            .map(|r| r as i64)
    }

    /// Returns members in the [start, stop] index range.
    pub fn zrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        self.zsets
            .lock()
            .unwrap()
            .get(key)
            .map(|zs| zs.range(start, stop))
            .unwrap_or_default()
    }

    /// Returns the number of members in the sorted set.
    pub fn zcard(&self, key: &str) -> i64 {
        self.zsets
            .lock()
            .unwrap()
            .get(key)
            .map(|zs| zs.card() as i64)
            .unwrap_or(0)
    }

    /// Returns the score of a member, or `None` if not found.
    pub fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        self.zsets.lock().unwrap().get(key)?.score(member)
    }

    /// Removes members. Returns the number that were actually removed.
    pub fn zrem(&self, key: &str, members: &[String]) -> i64 {
        let mut map = self.zsets.lock().unwrap();
        let Some(zset) = map.get_mut(key) else {
            return 0;
        };
        members.iter().filter(|m| zset.remove(m)).count() as i64
    }
}
