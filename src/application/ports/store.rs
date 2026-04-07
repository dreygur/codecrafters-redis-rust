use crate::domain::entities::SortedSet;
use crate::domain::DomainError;

pub trait StorePort: Send + Sync {
    fn get(&self, key: &str) -> Option<String>;
    fn set(&self, key: String, value: String, ttl_millis: Option<u64>);
    fn incr(&self, key: &str) -> Result<i64, DomainError>;
    fn get_type(&self, key: &str) -> Option<String>;

    fn rpush(&self, key: &str, values: Vec<String>) -> i64;
    fn lpush(&self, key: &str, values: Vec<String>) -> i64;
    fn lpop(&self, key: &str) -> Option<String>;
    fn rpop(&self, key: &str) -> Option<String>;
    fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String>;
    fn lrem(&self, key: &str, count: i64, value: &str) -> i64;
    fn llen(&self, key: &str) -> i64;

    fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> i64;
    fn zrank(&self, key: &str, member: &str) -> Option<i64>;
    fn zrange(&self, key: &str, start: i64, stop: i64) -> Vec<String>;
    fn zcard(&self, key: &str) -> i64;
    fn zscore(&self, key: &str, member: &str) -> Option<f64>;
    fn zrem(&self, key: &str, members: &[String]) -> i64;

    fn geoadd(&self, key: &str, lon: f64, lat: f64, member: String) -> bool;
    fn geopos(&self, key: &str, member: &str) -> Option<(f64, f64)>;
    fn geodist(&self, key: &str, m1: &str, m2: &str) -> Option<f64>;
    fn geosearch_radius(&self, key: &str, lon: f64, lat: f64, radius_m: f64) -> Vec<(String, f64)>;

    fn xadd(&self, key: &str, id: &str, fields: Vec<(String, String)>) -> Option<String>;
    fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Vec<(String, Vec<(String, String)>)>;
    fn xread(
        &self,
        keys: &[String],
        ids: &[String],
    ) -> Vec<(String, Vec<(String, Vec<(String, String)>)>)>;

    fn key_version(&self, key: &str) -> u64;
}
