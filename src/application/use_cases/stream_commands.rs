use std::sync::Arc;

use bytes::Bytes;

use crate::application::ports::StorePort;
use crate::domain::DomainError;
use crate::infrastructure::networking::resp::RespEncoder;

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
        encode_entries_array(entries)
    }

    pub fn xread(&self, args: &[String]) -> Bytes {
        let Some((count, keys, ids)) = parse_xread_args(args) else {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        };
        let results = self.store.xread(&keys, &ids);
        if results.is_empty() {
            return RespEncoder::null_array();
        }
        encode_xread_results(results, count)
    }
}

pub fn encode_xread_results(
    results: Vec<(String, Vec<(String, Vec<(String, String)>)>)>,
    count: Option<usize>,
) -> Bytes {
    let streams: Vec<Bytes> = results.into_iter().map(|(key, entries)| {
        let limit = count.unwrap_or(usize::MAX);
        let entries_encoded = encode_entries_array(entries.into_iter().take(limit).collect());
        RespEncoder::array(vec![
            RespEncoder::bulk_string(&key),
            entries_encoded,
        ])
    }).collect();
    RespEncoder::array(streams)
}

fn encode_entries_array(entries: Vec<(String, Vec<(String, String)>)>) -> Bytes {
    let encoded: Vec<Bytes> = entries.into_iter().map(|(id, fields)| {
        let fields_arr: Vec<Bytes> = fields.into_iter()
            .flat_map(|(k, v)| [RespEncoder::bulk_string(&k), RespEncoder::bulk_string(&v)])
            .collect();
        RespEncoder::array(vec![
            RespEncoder::bulk_string(&id),
            RespEncoder::array(fields_arr),
        ])
    }).collect();
    RespEncoder::array(encoded)
}

fn parse_xread_args(args: &[String]) -> Option<(Option<usize>, Vec<String>, Vec<String>)> {
    let mut count = None;
    let mut i = 1;

    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "COUNT" => {
                count = args.get(i + 1)?.parse().ok();
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return None,
        }
    }

    let remaining = &args[i..];
    if remaining.len() < 2 || remaining.len() % 2 != 0 {
        return None;
    }
    let mid = remaining.len() / 2;
    Some((count, remaining[..mid].to_vec(), remaining[mid..].to_vec()))
}
