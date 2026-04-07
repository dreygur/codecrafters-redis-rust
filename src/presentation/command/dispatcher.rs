use crate::application::ports::StorePort;
use bytes::Bytes;

pub trait CommandDispatcher: Send + Sync {
    fn dispatch(&self, args: &[String], store: &impl StorePort) -> Bytes;
}
