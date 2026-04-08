use std::{collections::HashMap, sync::{Arc, Mutex}};

use tokio::sync::{mpsc::UnboundedSender, oneshot::Receiver};

use crate::{application::ports::StorePort, domain::{DomainError, XAddError}};
use super::{
    list_store::ListStore, stream_store::StreamStore,
    string_store::StringStore, zset_store::ZSetStore,
};

type StreamNotification = (String, String, Vec<(String, String)>);

pub struct InMemoryStore {
    strings: StringStore,
    lists: ListStore,
    zsets: ZSetStore,
    streams: StreamStore,
    key_versions: Arc<Mutex<HashMap<String, u64>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            strings: StringStore::new(),
            lists: ListStore::new(),
            zsets: ZSetStore::new(),
            streams: StreamStore::new(),
            key_versions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn bump_version(&self, key: &str) {
        *self.key_versions.lock().unwrap().entry(key.to_string()).or_insert(0) += 1;
    }
}

impl StorePort for InMemoryStore {
    fn get(&self, key: &str) -> Option<String> {
        self.strings.get(key)
    }

    fn set(&self, key: String, value: String, ttl_millis: Option<u64>) {
        self.strings.set(&key, value, ttl_millis);
        self.bump_version(&key);
    }

    fn incr(&self, key: &str) -> Result<i64, DomainError> {
        let value = self.strings.incr(key)?;
        self.bump_version(key);
        Ok(value)
    }

    fn get_type(&self, key: &str) -> Option<String> {
        if self.strings.contains(key) { return Some("string".to_string()); }
        if self.zsets.contains(key) { return Some("zset".to_string()); }
        if self.lists.contains(key) { return Some("list".to_string()); }
        if self.streams.contains(key) { return Some("stream".to_string()); }
        None
    }

    fn rpush(&self, key: &str, values: Vec<String>) -> i64 {
        let count = self.lists.rpush(key, values);
        self.bump_version(key);
        count
    }

    fn lpush(&self, key: &str, values: Vec<String>) -> i64 {
        let count = self.lists.lpush(key, values);
        self.bump_version(key);
        count
    }

    fn lpop(&self, key: &str) -> Option<String> {
        let value = self.lists.lpop(key)?;
        self.bump_version(key);
        Some(value)
    }

    fn lpop_count(&self, key: &str, count: usize) -> Vec<String> {
        let values = self.lists.lpop_count(key, count);
        if !values.is_empty() {
            self.bump_version(key);
        }
        values
    }

    fn rpop(&self, key: &str) -> Option<String> {
        let value = self.lists.rpop(key)?;
        self.bump_version(key);
        Some(value)
    }

    fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        self.lists.lrange(key, start, stop)
    }

    fn lrem(&self, key: &str, count: i64, value: &str) -> i64 {
        let removed = self.lists.lrem(key, count, value);
        if removed > 0 {
            self.bump_version(key);
        }
        removed
    }

    fn llen(&self, key: &str) -> i64 {
        self.lists.llen(key)
    }

    fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> i64 {
        let count = self.zsets.zadd(key, pairs);
        self.bump_version(key);
        count
    }

    fn zrank(&self, key: &str, member: &str) -> Option<i64> { self.zsets.zrank(key, member) }
    fn zrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> { self.zsets.zrange(key, start, stop) }
    fn zcard(&self, key: &str) -> i64 { self.zsets.zcard(key) }
    fn zscore(&self, key: &str, member: &str) -> Option<f64> { self.zsets.zscore(key, member) }

    fn zrem(&self, key: &str, members: &[String]) -> i64 {
        let removed = self.zsets.zrem(key, members);
        if removed > 0 {
            self.bump_version(key);
        }
        removed
    }

    fn geoadd(&self, key: &str, lon: f64, lat: f64, member: String) -> bool {
        let is_new = self.zsets.geoadd(key, lon, lat, member);
        self.bump_version(key);
        is_new
    }

    fn geopos(&self, key: &str, member: &str) -> Option<(f64, f64)> { self.zsets.geopos(key, member) }
    fn geodist(&self, key: &str, m1: &str, m2: &str) -> Option<f64> { self.zsets.geodist(key, m1, m2) }

    fn geosearch_radius(&self, key: &str, lon: f64, lat: f64, radius_m: f64) -> Vec<(String, f64)> {
        self.zsets.geosearch_radius(key, lon, lat, radius_m)
    }

    fn xadd(&self, key: &str, id: &str, fields: Vec<(String, String)>) -> Result<String, XAddError> {
        let entry_id = self.streams.xadd(key, id, fields)?;
        self.bump_version(key);
        Ok(entry_id)
    }

    fn xrange(&self, key: &str, start: &str, end: &str, count: Option<usize>) -> Vec<(String, Vec<(String, String)>)> {
        self.streams.xrange(key, start, end, count)
    }

    fn xread(&self, keys: &[String], ids: &[String]) -> Vec<(String, Vec<(String, Vec<(String, String)>)>)> {
        self.streams.xread(keys, ids)
    }

    fn key_version(&self, key: &str) -> u64 { *self.key_versions.lock().unwrap().get(key).unwrap_or(&0) }
    fn blpop_or_wait(&self, key: &str) -> Result<String, Receiver<String>> { self.lists.blpop_or_wait(key) }
    fn stream_last_id(&self, key: &str) -> (u64, u64) { self.streams.last_entry_id(key) }

    fn xread_blocking(
        &self,
        key: &str,
        after_id: (u64, u64),
        tx: UnboundedSender<StreamNotification>,
    ) -> Option<Vec<(String, Vec<(String, String)>)>> {
        self.streams.xread_blocking(key, after_id, tx)
    }
}

impl Clone for InMemoryStore {
    fn clone(&self) -> Self {
        Self {
            strings: self.strings.clone(),
            lists: self.lists.clone(),
            zsets: self.zsets.clone(),
            streams: self.streams.clone(),
            key_versions: Arc::clone(&self.key_versions),
        }
    }
}
