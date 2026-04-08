use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::domain::entities::SortedSet;
use crate::infrastructure::geo::GeoUtils;

pub(super) struct ZSetStore {
    data: Arc<Mutex<HashMap<String, SortedSet>>>,
}

impl ZSetStore {
    pub(super) fn new() -> Self {
        Self { data: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub(super) fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> i64 {
        let mut map = self.data.lock().unwrap();
        let zset = map.entry(key.to_string()).or_insert_with(SortedSet::new);
        pairs.into_iter().filter(|(score, member)| zset.add(*score, member.clone())).count() as i64
    }

    pub(super) fn zrank(&self, key: &str, member: &str) -> Option<i64> {
        self.data.lock().unwrap().get(key)?.rank(member).map(|r| r as i64)
    }

    pub(super) fn zrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        self.data.lock().unwrap().get(key).map(|zs| zs.range(start, stop)).unwrap_or_default()
    }

    pub(super) fn zcard(&self, key: &str) -> i64 {
        self.data.lock().unwrap().get(key).map(|zs| zs.card() as i64).unwrap_or(0)
    }

    pub(super) fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        self.data.lock().unwrap().get(key)?.score(member)
    }

    pub(super) fn zrem(&self, key: &str, members: &[String]) -> i64 {
        let mut map = self.data.lock().unwrap();
        let Some(zset) = map.get_mut(key) else { return 0; };
        members.iter().filter(|m| zset.remove(m)).count() as i64
    }

    pub(super) fn contains(&self, key: &str) -> bool {
        self.data.lock().unwrap().contains_key(key)
    }

    pub(super) fn geoadd(&self, key: &str, lon: f64, lat: f64, member: String) -> bool {
        let score = GeoUtils::encode(lon, lat);
        let mut map = self.data.lock().unwrap();
        map.entry(key.to_string()).or_insert_with(SortedSet::new).add(score, member)
    }

    pub(super) fn geopos(&self, key: &str, member: &str) -> Option<(f64, f64)> {
        let score = self.data.lock().unwrap().get(key)?.score(member)?;
        Some(GeoUtils::decode(score))
    }

    pub(super) fn geodist(&self, key: &str, m1: &str, m2: &str) -> Option<f64> {
        let (lon1, lat1) = self.geopos(key, m1)?;
        let (lon2, lat2) = self.geopos(key, m2)?;
        Some(GeoUtils::distance_m(lon1, lat1, lon2, lat2))
    }

    pub(super) fn geosearch_radius(
        &self, key: &str, lon: f64, lat: f64, radius_m: f64,
    ) -> Vec<(String, f64)> {
        let map = self.data.lock().unwrap();
        let Some(zset) = map.get(key) else { return vec![]; };
        zset.all().into_iter().filter_map(|(member, score)| {
            let (mlon, mlat) = GeoUtils::decode(score);
            let dist = GeoUtils::distance_m(lon, lat, mlon, mlat);
            (dist <= radius_m).then_some((member, dist))
        }).collect()
    }
}

impl Clone for ZSetStore {
    fn clone(&self) -> Self {
        Self { data: Arc::clone(&self.data) }
    }
}
