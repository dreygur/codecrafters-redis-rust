use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{error::RedisError, geo, sorted_set::SortedSet};

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

    // -- geo operations (backed by zsets; score = 52-bit geohash as f64) --

    /// Adds a member. Returns `true` if the member is new.
    pub fn geoadd(&self, key: &str, lon: f64, lat: f64, member: String) -> bool {
        let score = geo::encode(lon, lat);
        let mut map = self.zsets.lock().unwrap();
        map.entry(key.to_string())
            .or_insert_with(SortedSet::new)
            .add(score, member)
    }

    /// Returns decoded (lon, lat) for a member, or `None` if not found.
    pub fn geopos(&self, key: &str, member: &str) -> Option<(f64, f64)> {
        let score = self.zsets.lock().unwrap().get(key)?.score(member)?;
        Some(geo::decode(score))
    }

    /// Returns the distance in metres between two members, or `None` if either is missing.
    pub fn geodist(&self, key: &str, m1: &str, m2: &str) -> Option<f64> {
        let (lon1, lat1) = self.geopos(key, m1)?;
        let (lon2, lat2) = self.geopos(key, m2)?;
        Some(geo::distance_m(lon1, lat1, lon2, lat2))
    }

    /// Returns all members within `radius_m` of `(center_lon, center_lat)`,
    /// as `(member, distance_m)` pairs.
    pub fn geosearch_radius(
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
                let (lon, lat) = geo::decode(score);
                let dist = geo::distance_m(center_lon, center_lat, lon, lat);
                (dist <= radius_m).then_some((member, dist))
            })
            .collect()
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
