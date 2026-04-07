use std::sync::Arc;

use crate::application::ports::{AclPort, PubSubPort, StorePort};
use crate::application::use_cases::{
    auth_commands::AuthCommands, geo_commands::GeoCommands, list_commands::ListCommands,
    pubsub_commands::PubSubCommands, sorted_set_commands::SortedSetCommands,
    stream_commands::StreamCommands, string_commands::StringCommands,
    transaction_commands::TransactionCommands,
};
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;

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
}

impl CommandRouter {
    pub fn new(
        store: Arc<dyn StorePort>,
        acl: Arc<dyn AclPort>,
        pubsub: Arc<dyn PubSubPort>,
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
        }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        self.auth_cmds.authenticate(username, password)
    }

    pub fn key_version(&self, key: &str) -> u64 {
        self.store.key_version(key)
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
}
