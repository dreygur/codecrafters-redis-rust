use std::collections::{HashMap, HashSet};

pub struct Session {
    tx_queue: Option<Vec<Vec<String>>>,
    subscriptions: HashSet<String>,
    current_user: String,
    authenticated: bool,
    /// Key → version recorded at WATCH time. Empty when no keys are watched.
    watched: HashMap<String, u64>,
}

impl Session {
    /// `pre_authenticated` should be true when the default user has the nopass flag.
    pub fn new(pre_authenticated: bool) -> Self {
        Self {
            tx_queue: None,
            subscriptions: HashSet::new(),
            current_user: "default".to_string(),
            authenticated: pre_authenticated,
            watched: HashMap::new(),
        }
    }

    // -- auth --

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

    // -- watch --

    /// Records the version of a key at WATCH time.
    pub fn watch(&mut self, key: &str, version: u64) {
        self.watched.insert(key.to_string(), version);
    }

    /// Returns the map of watched key → version recorded at WATCH time.
    pub fn watched_versions(&self) -> &HashMap<String, u64> {
        &self.watched
    }

    /// Clears all watched keys (called on EXEC, DISCARD, and UNWATCH).
    pub fn unwatch(&mut self) {
        self.watched.clear();
    }

    // -- transaction --

    pub fn is_tx_active(&self) -> bool {
        self.tx_queue.is_some()
    }

    /// Returns false if a transaction is already active.
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

    /// Takes the queued commands and resets the transaction state.
    pub fn execute_tx(&mut self) -> Vec<Vec<String>> {
        self.tx_queue.take().unwrap_or_default()
    }

    /// Returns false if no transaction is active.
    pub fn discard_tx(&mut self) -> bool {
        if self.tx_queue.is_none() {
            return false;
        }
        self.tx_queue = None;
        true
    }

    // -- subscriptions --

    /// Returns true when the client has at least one active subscription.
    pub fn is_subscribed(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    /// Returns true if the client is already subscribed to the given channel.
    pub fn is_subscribed_to(&self, channel: &str) -> bool {
        self.subscriptions.contains(channel)
    }

    /// Subscribes to a channel and returns the new total unique subscription count.
    pub fn subscribe(&mut self, channel: &str) -> i64 {
        self.subscriptions.insert(channel.to_string());
        self.subscriptions.len() as i64
    }

    /// Unsubscribes from a channel and returns the remaining subscription count.
    pub fn unsubscribe(&mut self, channel: &str) -> i64 {
        self.subscriptions.remove(channel);
        self.subscriptions.len() as i64
    }
}
