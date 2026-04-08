use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::resp;

type Sender = mpsc::UnboundedSender<Bytes>;

pub struct PubSub {
    channels: Mutex<HashMap<String, Vec<Sender>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    pub fn subscribe(&self, channel: &str, sender: Sender) {
        self.channels
            .lock()
            .unwrap()
            .entry(channel.to_string())
            .or_default()
            .push(sender);
    }

    pub fn unsubscribe(&self, channel: &str, sender: &Sender) {
        let mut map = self.channels.lock().unwrap();
        if let Some(senders) = map.get_mut(channel) {
            senders.retain(|s| !s.same_channel(sender));
        }
    }

    pub fn publish(&self, channel: &str, message: &str) -> i64 {
        let msg = resp::array(vec![
            resp::bulk_string("message"),
            resp::bulk_string(channel),
            resp::bulk_string(message),
        ]);
        let mut map = self.channels.lock().unwrap();
        let Some(senders) = map.get_mut(channel) else {
            return 0;
        };
        let mut count = 0i64;
        senders.retain(|sender| match sender.send(msg.clone()) {
            Ok(()) => {
                count += 1;
                true
            }
            Err(_) => false,
        });
        count
    }
}
