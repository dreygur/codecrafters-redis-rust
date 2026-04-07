use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::resp;

type Sender = mpsc::UnboundedSender<Bytes>;

#[derive(Clone)]
pub struct PubSubService {
    inner: Arc<Mutex<HashMap<String, Vec<Sender>>>>,
}

impl PubSubService {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers a sender for the given channel.
    pub fn subscribe(&self, channel: &str, tx: Sender) {
        self.inner
            .lock()
            .unwrap()
            .entry(channel.to_string())
            .or_default()
            .push(tx);
    }

    /// Delivers a message to all clients subscribed to the channel.
    /// Prunes dead senders (disconnected clients) automatically.
    /// Returns the number of clients that received the message.
    pub fn publish(&self, channel: &str, message: &str) -> i64 {
        let msg = resp::array(vec![
            resp::bulk_string("message"),
            resp::bulk_string(channel),
            resp::bulk_string(message),
        ]);
        let mut map = self.inner.lock().unwrap();
        let Some(senders) = map.get_mut(channel) else {
            return 0;
        };
        let mut count = 0i64;
        senders.retain(|tx| match tx.send(msg.clone()) {
            Ok(()) => {
                count += 1;
                true
            }
            Err(_) => false,
        });
        count
    }
}
