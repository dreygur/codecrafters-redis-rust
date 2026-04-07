use std::sync::Arc;

use crate::application::ports::StorePort;
use crate::domain::DomainError;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct SortedSetCommands {
    store: Arc<dyn StorePort>,
}

impl SortedSetCommands {
    pub fn new(store: Arc<dyn StorePort>) -> Self {
        Self { store }
    }

    pub fn zadd(&self, args: &[String]) -> Bytes {
        if args.len() < 4 || (args.len() - 2) % 2 != 0 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let pairs: Option<Vec<(f64, String)>> = args[2..]
            .chunks(2)
            .map(|c| Some((c[0].parse::<f64>().ok()?, c[1].clone())))
            .collect();
        match pairs {
            Some(pairs) => RespEncoder::integer(self.store.zadd(&args[1], pairs)),
            None => RespEncoder::error("value is not a valid float"),
        }
    }

    pub fn zrank(&self, args: &[String]) -> Bytes {
        if args.len() < 3 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        match self.store.zrank(&args[1], &args[2]) {
            Some(rank) => RespEncoder::integer(rank),
            None => RespEncoder::null_bulk(),
        }
    }

    pub fn zrange(&self, args: &[String]) -> Bytes {
        if args.len() < 4 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let Ok(start) = args[2].parse::<i64>() else {
            return RespEncoder::error("value is not an integer");
        };
        let Ok(stop) = args[3].parse::<i64>() else {
            return RespEncoder::error("value is not an integer");
        };
        let members = self.store.zrange(&args[1], start, stop);
        RespEncoder::array(members.iter().map(|m| RespEncoder::bulk_string(m)).collect())
    }

    pub fn zcard(&self, args: &[String]) -> Bytes {
        if args.len() < 2 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        RespEncoder::integer(self.store.zcard(&args[1]))
    }

    pub fn zscore(&self, args: &[String]) -> Bytes {
        if args.len() < 3 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        match self.store.zscore(&args[1], &args[2]) {
            Some(score) => RespEncoder::bulk_string(&score.to_string()),
            None => RespEncoder::null_bulk(),
        }
    }

    pub fn zrem(&self, args: &[String]) -> Bytes {
        if args.len() < 3 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        RespEncoder::integer(self.store.zrem(&args[1], &args[2..].to_vec()))
    }
}
