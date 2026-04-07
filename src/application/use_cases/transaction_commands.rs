use crate::application::ports::StorePort;
use crate::domain::DomainError;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct TransactionCommands<S: StorePort> {
    store: S,
}

impl<S: StorePort> TransactionCommands<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn watch(&self, args: &[String]) -> Bytes {
        if args.len() < 2 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        RespEncoder::simple_string("OK")
    }

    pub fn unwatch(&self) -> Bytes {
        RespEncoder::simple_string("OK")
    }

    pub fn multi(&self, tx_active: bool) -> Bytes {
        if tx_active {
            RespEncoder::error(&DomainError::NestedMulti.to_string())
        } else {
            RespEncoder::simple_string("OK")
        }
    }

    pub fn exec(
        &self,
        tx_active: bool,
        watched_versions: &[(String, u64)],
        queue: Vec<Vec<String>>,
    ) -> Bytes {
        if !tx_active {
            return RespEncoder::error(&DomainError::ExecWithoutMulti.to_string());
        }

        let dirty = watched_versions
            .iter()
            .any(|(key, ver)| self.store.key_version(key) != *ver);

        if dirty {
            RespEncoder::null_array()
        } else {
            let results: Vec<Bytes> = queue
                .iter()
                .map(|cmd| dispatch_queued_command(cmd, &self.store))
                .collect();
            RespEncoder::array(results)
        }
    }

    pub fn discard(&self, tx_active: bool) -> Bytes {
        if !tx_active {
            return RespEncoder::error(&DomainError::DiscardWithoutMulti.to_string());
        }
        RespEncoder::simple_string("OK")
    }
}

fn dispatch_queued_command(args: &[String], store: &impl StorePort) -> bytes::Bytes {
    match args[0].to_uppercase().as_str() {
        "SET" => {
            if args.len() < 3 {
                return RespEncoder::error("wrong number of arguments for 'set' command");
            }
            let ttl = match args.get(3).map(|s| s.to_uppercase()).as_deref() {
                Some("PX") => args.get(4).and_then(|v| v.parse().ok()),
                Some("EX") => args
                    .get(4)
                    .and_then(|v| v.parse::<u64>().ok().map(|s| s * 1000)),
                _ => None,
            };
            store.set(args[1].clone(), args[2].clone(), ttl);
            RespEncoder::simple_string("OK")
        }
        "GET" => {
            if args.len() < 2 {
                return RespEncoder::error("wrong number of arguments for 'get' command");
            }
            match store.get(&args[1]) {
                Some(v) => RespEncoder::bulk_string(&v),
                None => RespEncoder::null_bulk(),
            }
        }
        "INCR" => {
            if args.len() < 2 {
                return RespEncoder::error("wrong number of arguments for 'incr' command");
            }
            match store.incr(&args[1]) {
                Ok(n) => RespEncoder::integer(n),
                Err(e) => RespEncoder::error(&e.to_string()),
            }
        }
        _ => RespEncoder::error("unknown command"),
    }
}
