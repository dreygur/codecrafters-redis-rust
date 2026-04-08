use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;

use crate::application::ports::{AclPort, PubSubPort, StorePort};
use crate::application::use_cases::{
    auth_commands::AuthCommands, geo_commands::GeoCommands, list_commands::ListCommands,
    pubsub_commands::PubSubCommands, sorted_set_commands::SortedSetCommands,
    stream_commands::StreamCommands, string_commands::StringCommands,
    transaction_commands::TransactionCommands,
};
use crate::infrastructure::config::Config;
use crate::infrastructure::networking::resp::RespEncoder;

pub struct CommandRouter {
    string_cmds: StringCommands,
    list_cmds: ListCommands,
    stream_cmds: StreamCommands,
    sorted_set_cmds: SortedSetCommands,
    geo_cmds: GeoCommands,
    auth_cmds: AuthCommands,
    pubsub_cmds: PubSubCommands,
    tx_cmds: TransactionCommands,
    store: Arc<dyn StorePort>,
    pubsub: Arc<dyn PubSubPort>,
    config: Arc<Config>,
}

impl CommandRouter {
    pub fn new(
        store: Arc<dyn StorePort>,
        acl: Arc<dyn AclPort>,
        pubsub: Arc<dyn PubSubPort>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            string_cmds: StringCommands::new(Arc::clone(&store)),
            list_cmds: ListCommands::new(Arc::clone(&store)),
            stream_cmds: StreamCommands::new(Arc::clone(&store)),
            sorted_set_cmds: SortedSetCommands::new(Arc::clone(&store)),
            geo_cmds: GeoCommands::new(Arc::clone(&store)),
            auth_cmds: AuthCommands::new(Arc::clone(&acl)),
            pubsub_cmds: PubSubCommands::new(Arc::clone(&pubsub)),
            tx_cmds: TransactionCommands::new(Arc::clone(&store)),
            store,
            pubsub,
            config,
        }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        self.auth_cmds.authenticate(username, password)
    }

    pub fn key_version(&self, key: &str) -> u64 {
        self.store.key_version(key)
    }

    pub fn blpop_or_wait(&self, key: &str) -> Result<String, tokio::sync::oneshot::Receiver<String>> {
        self.store.blpop_or_wait(key)
    }

    pub fn stream_last_id(&self, key: &str) -> (u64, u64) {
        self.store.stream_last_id(key)
    }

    pub fn xread_non_blocking(
        &self,
        keys: &[String],
        ids: &[String],
    ) -> Vec<(String, Vec<(String, Vec<(String, String)>)>)> {
        self.store.xread(keys, ids)
    }

    pub fn xread_blocking(
        &self,
        key: &str,
        after_id: (u64, u64),
        tx: tokio::sync::mpsc::UnboundedSender<(String, String, Vec<(String, String)>)>,
    ) -> Option<Vec<(String, Vec<(String, String)>)>> {
        self.store.xread_blocking(key, after_id, tx)
    }

    pub fn subscribe(&self, channel: &str, tx: UnboundedSender<Bytes>) {
        self.pubsub.subscribe(channel, tx);
    }

    pub fn unsubscribe(&self, channel: &str, tx: &UnboundedSender<Bytes>) {
        self.pubsub.unsubscribe(channel, tx);
    }

    pub fn exec_transaction(&self, watched: &[(String, u64)], queue: Vec<Vec<String>>) -> Bytes {
        self.tx_cmds.exec(watched, queue, |args| self.dispatch(args))
    }

    pub fn dispatch(&self, args: &[String]) -> Bytes {
        if args.is_empty() {
            return RespEncoder::error("empty command");
        }
        match args[0].to_uppercase().as_str() {
            "KEYS" => {
                let pattern = args.get(1).map(|s| s.as_str()).unwrap_or("*");
                let keys = self.store.keys(pattern);
                RespEncoder::array(keys.iter().map(|k| RespEncoder::bulk_string(k)).collect())
            }
            "PING" => match args.get(1) {
                Some(msg) => RespEncoder::bulk_string(msg),
                None => RespEncoder::simple_string("PONG"),
            },
            "ECHO" => self.string_cmds.echo(args),
            "SET" => self.string_cmds.set(args),
            "GET" => self.string_cmds.get(args),
            "INCR" => self.string_cmds.incr(args),

            "RPUSH" => self.list_cmds.rpush(args),
            "LPUSH" => self.list_cmds.lpush(args),
            "LPOP" => self.list_cmds.lpop(args),
            "RPOP" => self.list_cmds.rpop(args),
            "LRANGE" => self.list_cmds.lrange(args),
            "LLEN" => self.list_cmds.llen(args),
            "LREM" => self.list_cmds.lrem(args),
            "TYPE" => self.list_cmds.type_of(args),

            "ZADD" => self.sorted_set_cmds.zadd(args),
            "ZRANGE" => self.sorted_set_cmds.zrange(args),
            "ZRANK" => self.sorted_set_cmds.zrank(args),
            "ZCARD" => self.sorted_set_cmds.zcard(args),
            "ZSCORE" => self.sorted_set_cmds.zscore(args),
            "ZREM" => self.sorted_set_cmds.zrem(args),

            "GEOADD" => self.geo_cmds.geoadd(args),
            "GEOPOS" => self.geo_cmds.geopos(args),
            "GEODIST" => self.geo_cmds.geodist(args),
            "GEOSEARCH" => self.geo_cmds.geosearch(args),

            "XADD" => self.stream_cmds.xadd(args),
            "XRANGE" => self.stream_cmds.xrange(args),
            "XREAD" => self.stream_cmds.xread(args),

            "PUBLISH" => self.pubsub_cmds.publish(args),

            "CONFIG" => self.handle_config(args),

            "ACL" => match args.get(1).map(|s| s.to_uppercase()).as_deref() {
                Some("GETUSER") => match args.get(2) {
                    Some(username) => self.auth_cmds.getuser(username),
                    None => RespEncoder::error("wrong number of arguments for 'acl getuser' command"),
                },
                Some("SETUSER") => self.auth_cmds.setuser(args),
                _ => RespEncoder::error("unknown ACL subcommand"),
            },

            _ => RespEncoder::error("unknown command"),
        }
    }

    fn handle_config(&self, args: &[String]) -> Bytes {
        let sub = args.get(1).map(|s| s.to_uppercase());
        match sub.as_deref() {
            Some("GET") => {
                let param = match args.get(2) {
                    Some(p) => p.to_lowercase(),
                    None => return RespEncoder::error("wrong number of arguments"),
                };
                let mut pairs: Vec<Bytes> = vec![];
                if param == "dir" || param == "*" {
                    if let Some(dir) = &self.config.dir {
                        pairs.push(RespEncoder::bulk_string("dir"));
                        pairs.push(RespEncoder::bulk_string(dir));
                    }
                }
                if param == "dbfilename" || param == "*" {
                    if let Some(name) = &self.config.dbfilename {
                        pairs.push(RespEncoder::bulk_string("dbfilename"));
                        pairs.push(RespEncoder::bulk_string(name));
                    }
                }
                RespEncoder::array(pairs)
            }
            _ => RespEncoder::error("unknown CONFIG subcommand"),
        }
    }
}
