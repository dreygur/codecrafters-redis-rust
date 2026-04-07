use crate::application::ports::PubSubPort;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

pub struct PubSubCommands<P: PubSubPort> {
    pubsub: P,
}

impl<P: PubSubPort> PubSubCommands<P> {
    pub fn new(pubsub: P) -> Self {
        Self { pubsub }
    }

    pub fn subscribe(
        &self,
        args: &[String],
        tx: mpsc::UnboundedSender<Bytes>,
        subscribed_count: i64,
    ) -> Bytes {
        let mut out = BytesMut::new();
        for channel in args.iter().skip(1) {
            self.pubsub.subscribe(channel, tx.clone());
            let response = RespEncoder::array(vec![
                RespEncoder::bulk_string("subscribe"),
                RespEncoder::bulk_string(channel),
                RespEncoder::integer(subscribed_count),
            ]);
            out.extend_from_slice(&response);
        }
        out.freeze()
    }

    pub fn unsubscribe(
        &self,
        args: &[String],
        tx: &mpsc::UnboundedSender<Bytes>,
        remaining_count: i64,
    ) -> Bytes {
        let mut out = BytesMut::new();
        for channel in args.iter().skip(1) {
            self.pubsub.unsubscribe(channel, tx);
            let response = RespEncoder::array(vec![
                RespEncoder::bulk_string("unsubscribe"),
                RespEncoder::bulk_string(channel),
                RespEncoder::integer(remaining_count),
            ]);
            out.extend_from_slice(&response);
        }
        out.freeze()
    }

    pub fn publish(&self, args: &[String]) -> Bytes {
        let channel = args.get(1).map(String::as_str).unwrap_or("");
        let message = args.get(2).map(String::as_str).unwrap_or("");
        RespEncoder::integer(self.pubsub.publish(channel, message))
    }
}
