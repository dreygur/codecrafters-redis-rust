use std::sync::Arc;

use crate::application::ports::StorePort;
use crate::domain::DomainError;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct StringCommands {
    store: Arc<dyn StorePort>,
}

impl StringCommands {
    pub fn new(store: Arc<dyn StorePort>) -> Self {
        Self { store }
    }

    pub fn echo(&self, args: &[String]) -> Bytes {
        match args.get(1) {
            Some(arg) => RespEncoder::bulk_string(arg),
            None => RespEncoder::error(&DomainError::WrongArgCount.to_string()),
        }
    }

    pub fn set(&self, args: &[String]) -> Bytes {
        if args.len() < 3 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let ttl = match args.get(3).map(|s| s.to_uppercase()).as_deref() {
            Some("PX") => args.get(4).and_then(|v| v.parse().ok()),
            Some("EX") => args.get(4).and_then(|v| v.parse::<u64>().ok().map(|s| s * 1000)),
            _ => None,
        };
        self.store.set(args[1].clone(), args[2].clone(), ttl);
        RespEncoder::simple_string("OK")
    }

    pub fn get(&self, args: &[String]) -> Bytes {
        if args.len() < 2 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        match self.store.get(&args[1]) {
            Some(v) => RespEncoder::bulk_string(&v),
            None => RespEncoder::null_bulk(),
        }
    }

    pub fn incr(&self, args: &[String]) -> Bytes {
        if args.len() < 2 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        match self.store.incr(&args[1]) {
            Ok(n) => RespEncoder::integer(n),
            Err(e) => RespEncoder::error(&e.to_string()),
        }
    }
}
