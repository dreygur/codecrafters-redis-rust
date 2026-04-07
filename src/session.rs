use std::collections::HashSet;

pub struct Session {
    tx_queue: Option<Vec<Vec<String>>>,
    subscriptions: HashSet<String>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            tx_queue: None,
            subscriptions: HashSet::new(),
        }
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

    /// Subscribes to a channel and returns the new total unique subscription count.
    pub fn subscribe(&mut self, channel: &str) -> i64 {
        self.subscriptions.insert(channel.to_string());
        self.subscriptions.len() as i64
    }
}
