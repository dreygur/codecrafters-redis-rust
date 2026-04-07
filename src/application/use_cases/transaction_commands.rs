use std::sync::Arc;

use crate::application::ports::StorePort;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct TransactionCommands {
    store: Arc<dyn StorePort>,
}

impl TransactionCommands {
    pub fn new(store: Arc<dyn StorePort>) -> Self {
        Self { store }
    }

    pub fn exec(
        &self,
        watched: &[(String, u64)],
        queue: Vec<Vec<String>>,
        dispatch: impl Fn(&[String]) -> Bytes,
    ) -> Bytes {
        let dirty = watched.iter().any(|(key, ver)| self.store.key_version(key) != *ver);
        if dirty {
            return RespEncoder::null_array();
        }
        let results: Vec<Bytes> = queue.iter().map(|cmd| dispatch(cmd)).collect();
        RespEncoder::array(results)
    }
}
