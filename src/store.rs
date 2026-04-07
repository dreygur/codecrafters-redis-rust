use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

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

pub type Store = Arc<Mutex<HashMap<String, Entry>>>;

pub fn new_store() -> Store {
    Arc::new(Mutex::new(HashMap::new()))
}
