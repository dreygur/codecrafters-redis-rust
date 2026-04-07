use std::time::{Duration, Instant};

pub struct Entry {
    pub value: String,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: String, ttl_millis: Option<u64>) -> Self {
        Self {
            value,
            expires_at: ttl_millis.map(|ms| Instant::now() + Duration::from_millis(ms)),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |t| Instant::now() > t)
    }
}
