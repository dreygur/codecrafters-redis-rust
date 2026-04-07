use std::collections::{HashMap, HashSet};

pub struct Session {
    tx_queue: Option<Vec<Vec<String>>>,
    subscriptions: HashSet<String>,
    current_user: String,
    authenticated: bool,
    watched: HashMap<String, u64>,
}

impl Session {
    pub fn new(pre_authenticated: bool) -> Self {
        Self {
            tx_queue: None,
            subscriptions: HashSet::new(),
            current_user: "default".to_string(),
            authenticated: pre_authenticated,
            watched: HashMap::new(),
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn current_user(&self) -> &str {
        &self.current_user
    }

    pub fn authenticate(&mut self, username: String) {
        self.current_user = username;
        self.authenticated = true;
    }

    pub fn watch(&mut self, key: &str, version: u64) {
        self.watched.insert(key.to_string(), version);
    }

    pub fn watched_versions(&self) -> &HashMap<String, u64> {
        &self.watched
    }

    pub fn unwatch(&mut self) {
        self.watched.clear();
    }

    pub fn is_tx_active(&self) -> bool {
        self.tx_queue.is_some()
    }

    pub fn begin_tx(&mut self) -> bool {
        if self.tx_queue.is_some() {
            return false;
        }
        self.tx_queue = Some(Vec::new());
        true
    }

    pub fn enqueue(&mut self, args: Vec<String>) {
        if let Some(q) = &mut self.tx_queue {
            q.push(args);
        }
    }

    pub fn execute_tx(&mut self) -> Vec<Vec<String>> {
        self.tx_queue.take().unwrap_or_default()
    }

    pub fn discard_tx(&mut self) -> bool {
        if self.tx_queue.is_none() {
            return false;
        }
        self.tx_queue = None;
        true
    }

    pub fn is_subscribed(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    pub fn is_subscribed_to(&self, channel: &str) -> bool {
        self.subscriptions.contains(channel)
    }

    pub fn subscribe(&mut self, channel: &str) -> i64 {
        self.subscriptions.insert(channel.to_string());
        self.subscriptions.len() as i64
    }

    pub fn unsubscribe(&mut self, channel: &str) -> i64 {
        self.subscriptions.remove(channel);
        self.subscriptions.len() as i64
    }
}
