use std::sync::Arc;

use crate::application::ports::StorePort;
use crate::domain::DomainError;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct ListCommands {
    store: Arc<dyn StorePort>,
}

impl ListCommands {
    pub fn new(store: Arc<dyn StorePort>) -> Self {
        Self { store }
    }

    pub fn rpush(&self, args: &[String]) -> Bytes {
        if args.len() < 3 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        RespEncoder::integer(self.store.rpush(&args[1], args[2..].to_vec()))
    }

    pub fn lpush(&self, args: &[String]) -> Bytes {
        if args.len() < 3 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        RespEncoder::integer(self.store.lpush(&args[1], args[2..].to_vec()))
    }

    pub fn lpop(&self, args: &[String]) -> Bytes {
        if args.len() < 2 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        if let Some(count_str) = args.get(2) {
            let Ok(count) = count_str.parse::<usize>() else {
                return RespEncoder::error("value is not an integer or out of range");
            };
            let items = self.store.lpop_count(&args[1], count);
            if items.is_empty() {
                return RespEncoder::null_array();
            }
            return RespEncoder::array(items.iter().map(|v| RespEncoder::bulk_string(v)).collect());
        }
        match self.store.lpop(&args[1]) {
            Some(v) => RespEncoder::bulk_string(&v),
            None => RespEncoder::null_bulk(),
        }
    }

    pub fn rpop(&self, args: &[String]) -> Bytes {
        if args.len() < 2 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        match self.store.rpop(&args[1]) {
            Some(v) => RespEncoder::bulk_string(&v),
            None => RespEncoder::null_bulk(),
        }
    }

    pub fn lrange(&self, args: &[String]) -> Bytes {
        if args.len() < 4 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let start: i64 = args[2].parse().unwrap_or(0);
        let stop: i64 = args[3].parse().unwrap_or(-1);
        let items = self.store.lrange(&args[1], start, stop);
        RespEncoder::array(items.into_iter().map(|s| RespEncoder::bulk_string(&s)).collect())
    }

    pub fn llen(&self, args: &[String]) -> Bytes {
        if args.len() < 2 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        RespEncoder::integer(self.store.llen(&args[1]))
    }

    pub fn lrem(&self, args: &[String]) -> Bytes {
        if args.len() < 4 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let count: i64 = args[2].parse().unwrap_or(0);
        RespEncoder::integer(self.store.lrem(&args[1], count, &args[3]))
    }

    pub fn type_of(&self, args: &[String]) -> Bytes {
        if args.len() < 2 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        RespEncoder::simple_string(
            self.store.get_type(&args[1]).unwrap_or_else(|| "none".to_string()).as_str(),
        )
    }
}
