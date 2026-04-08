use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::domain::entities::Entry;
use crate::domain::DomainError;

pub(super) struct StringStore {
    data: Arc<Mutex<HashMap<String, Entry>>>,
}

impl StringStore {
    pub(super) fn new() -> Self {
        Self { data: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub(super) fn get(&self, key: &str) -> Option<String> {
        let mut map = self.data.lock().unwrap();
        match map.get(key) {
            Some(entry) if entry.is_expired() => { map.remove(key); None }
            Some(entry) => Some(entry.value.clone()),
            None => None,
        }
    }

    pub(super) fn set(&self, key: &str, value: String, ttl_millis: Option<u64>) {
        self.data.lock().unwrap().insert(key.to_string(), Entry::new(value, ttl_millis));
    }

    pub(super) fn incr(&self, key: &str) -> Result<i64, DomainError> {
        let mut map = self.data.lock().unwrap();
        let current = match map.get(key) {
            Some(entry) if entry.is_expired() => { map.remove(key); 0i64 }
            Some(entry) => entry.value.parse::<i64>().map_err(|_| DomainError::NotAnInteger)?,
            None => 0i64,
        };
        let new_val = current + 1;
        map.insert(key.to_string(), Entry::new(new_val.to_string(), None));
        Ok(new_val)
    }

    pub(super) fn contains(&self, key: &str) -> bool {
        self.data.lock().unwrap().contains_key(key)
    }

    pub(super) fn keys(&self, pattern: &str) -> Vec<String> {
        let mut map = self.data.lock().unwrap();
        map.retain(|_, e| !e.is_expired());
        if pattern == "*" {
            return map.keys().cloned().collect();
        }
        map.keys()
            .filter(|k| glob_match(pattern, k))
            .cloned()
            .collect()
    }
}

fn glob_match(pattern: &str, s: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = s.chars().collect();
    let mut dp = vec![vec![false; t.len() + 1]; p.len() + 1];
    dp[0][0] = true;
    for i in 1..=p.len() {
        if p[i - 1] == '*' { dp[i][0] = dp[i - 1][0]; }
    }
    for i in 1..=p.len() {
        for j in 1..=t.len() {
            dp[i][j] = if p[i - 1] == '*' {
                dp[i - 1][j] || dp[i][j - 1]
            } else if p[i - 1] == '?' || p[i - 1] == t[j - 1] {
                dp[i - 1][j - 1]
            } else {
                false
            };
        }
    }
    dp[p.len()][t.len()]
}

impl Clone for StringStore {
    fn clone(&self) -> Self {
        Self { data: Arc::clone(&self.data) }
    }
}
