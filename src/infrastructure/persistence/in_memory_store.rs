use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::application::ports::StorePort;
use crate::domain::entities::{Entry, SortedSet};
use crate::domain::DomainError;

pub struct InMemoryStore {
    strings: Arc<Mutex<HashMap<String, Entry>>>,
    zsets: Arc<Mutex<HashMap<String, SortedSet>>>,
    lists: Arc<Mutex<HashMap<String, Vec<String>>>>,
    streams: Arc<Mutex<HashMap<String, Vec<(String, Vec<(String, String)>)>>>>,
    key_versions: Arc<Mutex<HashMap<String, u64>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            strings: Arc::new(Mutex::new(HashMap::new())),
            zsets: Arc::new(Mutex::new(HashMap::new())),
            lists: Arc::new(Mutex::new(HashMap::new())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            key_versions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn bump_version(&self, key: &str) {
        *self
            .key_versions
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert(0) += 1;
    }
}

impl StorePort for InMemoryStore {
    fn get(&self, key: &str) -> Option<String> {
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

    fn set(&self, key: String, value: String, ttl_millis: Option<u64>) {
        self.strings
            .lock()
            .unwrap()
            .insert(key.clone(), Entry::new(value, ttl_millis));
        self.bump_version(&key);
    }

    fn incr(&self, key: &str) -> Result<i64, DomainError> {
        let new_val = {
            let mut map = self.strings.lock().unwrap();
            let current = match map.get(key) {
                Some(entry) if entry.is_expired() => {
                    map.remove(key);
                    0i64
                }
                Some(entry) => match entry.value.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return Err(DomainError::NotAnInteger),
                },
                None => 0i64,
            };
            let new_val = current + 1;
            map.insert(key.to_string(), Entry::new(new_val.to_string(), None));
            new_val
        };
        self.bump_version(key);
        Ok(new_val)
    }

    fn rpush(&self, key: &str, values: Vec<String>) -> i64 {
        let count = {
            let mut map = self.lists.lock().unwrap();
            let list = map.entry(key.to_string()).or_insert_with(Vec::new);
            for v in values {
                list.push(v);
            }
            list.len() as i64
        };
        self.bump_version(key);
        count
    }

    fn lpush(&self, key: &str, values: Vec<String>) -> i64 {
        let count = {
            let mut map = self.lists.lock().unwrap();
            let list = map.entry(key.to_string()).or_insert_with(Vec::new);
            for v in values {
                list.insert(0, v);
            }
            list.len() as i64
        };
        self.bump_version(key);
        count
    }

    fn lpop(&self, key: &str) -> Option<String> {
        let result = self.lists.lock().unwrap().get_mut(key)?.remove(0);
        self.bump_version(key);
        Some(result)
    }

    fn lpop_count(&self, key: &str, count: usize) -> Vec<String> {
        let result = {
            let mut lists = self.lists.lock().unwrap();
            let Some(list) = lists.get_mut(key) else { return vec![] };
            let n = count.min(list.len());
            list.drain(..n).collect::<Vec<_>>()
        };
        self.bump_version(key);
        result
    }

    fn rpop(&self, key: &str) -> Option<String> {
        let result = self.lists.lock().unwrap().get_mut(key)?.pop()?;
        self.bump_version(key);
        Some(result)
    }

    fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        let map = self.lists.lock().unwrap();
        let list = match map.get(key) {
            Some(list) => list,
            None => return vec![],
        };
        let len = list.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };
        if start > stop || start >= len {
            return vec![];
        }
        let start = start.max(0) as usize;
        let stop = (stop + 1).min(len) as usize;
        list[start..stop].to_vec()
    }

    fn lrem(&self, key: &str, count: i64, value: &str) -> i64 {
        let removed = {
            let mut map = self.lists.lock().unwrap();
            let Some(list) = map.get_mut(key) else {
                return 0;
            };
            if count == 0 {
                let before = list.len();
                list.retain(|v| v != value);
                list.len() as i64 - (before as i64 - list.len() as i64)
            } else if count > 0 {
                let mut removed = 0;
                let mut i = 0;
                while i < list.len() && removed < count {
                    if list[i] == value {
                        list.remove(i);
                        removed += 1;
                    } else {
                        i += 1;
                    }
                }
                removed
            } else {
                let mut removed = 0;
                let mut i = (list.len() as i64 - 1) as isize;
                while i >= 0 && removed < -count {
                    if list[i as usize] == value {
                        list.remove(i as usize);
                        removed += 1;
                    }
                    i -= 1;
                }
                removed
            }
        };
        if removed > 0 {
            self.bump_version(key);
        }
        removed
    }

    fn llen(&self, key: &str) -> i64 {
        self.lists
            .lock()
            .unwrap()
            .get(key)
            .map(|l| l.len() as i64)
            .unwrap_or(0)
    }

    fn get_type(&self, key: &str) -> Option<String> {
        let map = self.strings.lock().unwrap();
        if map.contains_key(key) {
            return Some("string".to_string());
        }
        drop(map);
        let map = self.zsets.lock().unwrap();
        if map.contains_key(key) {
            return Some("zset".to_string());
        }
        drop(map);
        let map = self.lists.lock().unwrap();
        if map.contains_key(key) {
            return Some("list".to_string());
        }
        drop(map);
        let map = self.streams.lock().unwrap();
        if map.contains_key(key) {
            return Some("stream".to_string());
        }
        None
    }

    fn xadd(&self, key: &str, id: &str, fields: Vec<(String, String)>) -> Option<String> {
        if id.is_empty() || id.contains('*') {
            return None;
        }
        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 {
            return None;
        }
        let timestamp: u64 = parts[0].parse().ok()?;
        let seq: u64 = parts[1].parse().ok()?;
        if timestamp == 0 && seq == 0 {
            return None;
        }

        let final_id = format!("{}-{}", timestamp, seq);
        let mut map = self.streams.lock().unwrap();
        let stream = map.entry(key.to_string()).or_insert_with(Vec::new);

        if let Some(pos) = stream
            .iter()
            .position(|(existing_id, _)| existing_id > &final_id)
        {
            stream.insert(pos, (final_id.clone(), fields));
        } else {
            stream.push((final_id.clone(), fields));
        }

        self.bump_version(key);
        Some(final_id)
    }

    fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Vec<(String, Vec<(String, String)>)> {
        let map = self.streams.lock().unwrap();
        let stream = match map.get(key) {
            Some(s) => s,
            None => return vec![],
        };

        let start_id = if start == "-" {
            "".to_string()
        } else {
            start.to_string()
        };
        let end_id = if end == "+" {
            std::char::MAX.to_string()
        } else {
            end.to_string()
        };

        stream
            .iter()
            .filter(|(id, _)| id >= &start_id && id <= &end_id)
            .take(count.unwrap_or(usize::MAX))
            .map(|(id, fields)| (id.clone(), fields.clone()))
            .collect()
    }

    fn xread(
        &self,
        keys: &[String],
        ids: &[String],
    ) -> Vec<(String, Vec<(String, Vec<(String, String)>)>)> {
        let map = self.streams.lock().unwrap();
        let mut results = vec![];

        for (key, start_id) in keys.iter().zip(ids.iter()) {
            let stream = match map.get(key) {
                Some(s) => s,
                None => continue,
            };

            let entries: Vec<(String, Vec<(String, String)>)> = stream
                .iter()
                .filter(|(id, _)| id > start_id)
                .map(|(id, fields)| (id.clone(), fields.clone()))
                .collect();

            if !entries.is_empty() {
                results.push((key.clone(), entries));
            }
        }
        results
    }

    fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> i64 {
        let count = {
            let mut map = self.zsets.lock().unwrap();
            let zset = map.entry(key.to_string()).or_insert_with(SortedSet::new);
            pairs
                .into_iter()
                .filter(|(score, member)| zset.add(*score, member.clone()))
                .count() as i64
        };
        self.bump_version(key);
        count
    }

    fn zrank(&self, key: &str, member: &str) -> Option<i64> {
        self.zsets
            .lock()
            .unwrap()
            .get(key)?
            .rank(member)
            .map(|r| r as i64)
    }

    fn zrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        self.zsets
            .lock()
            .unwrap()
            .get(key)
            .map(|zs| zs.range(start, stop))
            .unwrap_or_default()
    }

    fn zcard(&self, key: &str) -> i64 {
        self.zsets
            .lock()
            .unwrap()
            .get(key)
            .map(|zs| zs.card() as i64)
            .unwrap_or(0)
    }

    fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        self.zsets.lock().unwrap().get(key)?.score(member)
    }

    fn zrem(&self, key: &str, members: &[String]) -> i64 {
        let count = {
            let mut map = self.zsets.lock().unwrap();
            let Some(zset) = map.get_mut(key) else {
                return 0;
            };
            members.iter().filter(|m| zset.remove(m)).count() as i64
        };
        if count > 0 {
            self.bump_version(key);
        }
        count
    }

    fn geoadd(&self, key: &str, lon: f64, lat: f64, member: String) -> bool {
        let score = crate::infrastructure::geo::GeoUtils::encode(lon, lat);
        let is_new = {
            let mut map = self.zsets.lock().unwrap();
            map.entry(key.to_string())
                .or_insert_with(SortedSet::new)
                .add(score, member)
        };
        self.bump_version(key);
        is_new
    }

    fn geopos(&self, key: &str, member: &str) -> Option<(f64, f64)> {
        let score = self.zsets.lock().unwrap().get(key)?.score(member)?;
        Some(crate::infrastructure::geo::GeoUtils::decode(score))
    }

    fn geodist(&self, key: &str, m1: &str, m2: &str) -> Option<f64> {
        let (lon1, lat1) = self.geopos(key, m1)?;
        let (lon2, lat2) = self.geopos(key, m2)?;
        Some(crate::infrastructure::geo::GeoUtils::distance_m(
            lon1, lat1, lon2, lat2,
        ))
    }

    fn geosearch_radius(
        &self,
        key: &str,
        center_lon: f64,
        center_lat: f64,
        radius_m: f64,
    ) -> Vec<(String, f64)> {
        let map = self.zsets.lock().unwrap();
        let Some(zset) = map.get(key) else {
            return vec![];
        };
        zset.all()
            .into_iter()
            .filter_map(|(member, score)| {
                let (lon, lat) = crate::infrastructure::geo::GeoUtils::decode(score);
                let dist = crate::infrastructure::geo::GeoUtils::distance_m(
                    center_lon, center_lat, lon, lat,
                );
                (dist <= radius_m).then_some((member, dist))
            })
            .collect()
    }

    fn key_version(&self, key: &str) -> u64 {
        *self.key_versions.lock().unwrap().get(key).unwrap_or(&0)
    }
}

impl Clone for InMemoryStore {
    fn clone(&self) -> Self {
        Self {
            strings: self.strings.clone(),
            zsets: self.zsets.clone(),
            lists: self.lists.clone(),
            streams: self.streams.clone(),
            key_versions: self.key_versions.clone(),
        }
    }
}
