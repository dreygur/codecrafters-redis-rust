use bytes::Bytes;

pub trait PubSubPort: Send + Sync {
    fn subscribe(&self, channel: &str, tx: tokio::sync::mpsc::UnboundedSender<Bytes>);
    fn unsubscribe(&self, channel: &str, tx: &tokio::sync::mpsc::UnboundedSender<Bytes>);
    fn publish(&self, channel: &str, message: &str) -> i64;
}
