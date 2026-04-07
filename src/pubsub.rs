use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct PubSubService {
    subscribers: Arc<Mutex<HashMap<String, usize>>>,
}

impl PubSubService {
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers one subscription to a channel.
    pub fn subscribe(&self, channel: &str) {
        *self
            .subscribers
            .lock()
            .unwrap()
            .entry(channel.to_string())
            .or_insert(0) += 1;
    }

    /// Returns the total number of clients subscribed to the channel.
    pub fn subscriber_count(&self, channel: &str) -> i64 {
        self.subscribers
            .lock()
            .unwrap()
            .get(channel)
            .copied()
            .unwrap_or(0) as i64
    }
}
