use std::sync::Arc;

use crate::application::ports::StorePort;
use crate::domain::DomainError;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct StreamCommands {
    store: Arc<dyn StorePort>,
}

impl StreamCommands {
    pub fn new(store: Arc<dyn StorePort>) -> Self {
        Self { store }
    }

    pub fn xadd(&self, args: &[String]) -> Bytes {
        if args.len() < 4 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let fields: Vec<(String, String)> = args[3..]
            .chunks(2)
            .filter(|c| c.len() == 2)
            .map(|c| (c[0].clone(), c[1].clone()))
            .collect();
        match self.store.xadd(&args[1], &args[2], fields) {
            Ok(id) => RespEncoder::bulk_string(&id),
            Err(e) => RespEncoder::error(&e.to_string()),
        }
    }

    pub fn xrange(&self, args: &[String]) -> Bytes {
        if args.len() < 4 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let count = args.get(5).and_then(|c| c.parse().ok());
        let entries = self.store.xrange(&args[1], &args[2], &args[3], count);
        let response = entries
            .into_iter()
            .map(|(id, fields)| {
                let mut arr = vec![RespEncoder::bulk_string(&id)];
                for (k, v) in fields {
                    arr.push(RespEncoder::bulk_string(&k));
                    arr.push(RespEncoder::bulk_string(&v));
                }
                RespEncoder::array(arr)
            })
            .collect();
        RespEncoder::array(response)
    }

    pub fn xread(&self, args: &[String]) -> Bytes {
        if args.len() < 5 || args[1].to_uppercase() != "STREAMS" {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let mut i = 2;
        let mut keys = vec![];
        let mut ids = vec![];
        while i < args.len() - 1 {
            keys.push(args[i].clone());
            ids.push(args[i + 1].clone());
            i += 2;
        }
        let results = self.store.xread(&keys, &ids);
        let response: Vec<Bytes> = results
            .into_iter()
            .map(|(key, entries)| {
                let entries_arr: Vec<Bytes> = entries
                    .into_iter()
                    .map(|(id, fields)| {
                        let mut arr = vec![RespEncoder::bulk_string(&id)];
                        for (k, v) in fields {
                            arr.push(RespEncoder::bulk_string(&k));
                            arr.push(RespEncoder::bulk_string(&v));
                        }
                        RespEncoder::array(arr)
                    })
                    .collect();
                RespEncoder::array(vec![
                    RespEncoder::bulk_string(&key),
                    RespEncoder::array(entries_arr),
                ])
            })
            .collect();
        if response.is_empty() {
            RespEncoder::null_array()
        } else {
            RespEncoder::array(response)
        }
    }
}
