use std::sync::Arc;

use crate::application::ports::PubSubPort;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct PubSubCommands {
    pubsub: Arc<dyn PubSubPort>,
}

impl PubSubCommands {
    pub fn new(pubsub: Arc<dyn PubSubPort>) -> Self {
        Self { pubsub }
    }

    pub fn publish(&self, args: &[String]) -> Bytes {
        let channel = args.get(1).map(String::as_str).unwrap_or("");
        let message = args.get(2).map(String::as_str).unwrap_or("");
        RespEncoder::integer(self.pubsub.publish(channel, message))
    }
}
