use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::application::ports::PubSubPort;
use crate::infrastructure::networking::resp::RespEncoder;

type Sender = mpsc::UnboundedSender<Bytes>;

pub struct PubSubService {
    inner: Arc<Mutex<HashMap<String, Vec<Sender>>>>,
}

impl PubSubService {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl PubSubPort for PubSubService {
    fn subscribe(&self, channel: &str, tx: Sender) {
        self.inner
            .lock()
            .unwrap()
            .entry(channel.to_string())
            .or_default()
            .push(tx);
    }

    fn unsubscribe(&self, channel: &str, tx: &Sender) {
        let mut map = self.inner.lock().unwrap();
        if let Some(senders) = map.get_mut(channel) {
            senders.retain(|s| !s.same_channel(tx));
        }
    }

    fn publish(&self, channel: &str, message: &str) -> i64 {
        let msg = RespEncoder::array(vec![
            RespEncoder::bulk_string("message"),
            RespEncoder::bulk_string(channel),
            RespEncoder::bulk_string(message),
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

impl Clone for PubSubService {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
