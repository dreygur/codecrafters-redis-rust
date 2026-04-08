mod application;
mod domain;
mod infrastructure;
mod presentation;

use std::sync::Arc;

use application::ports::{AclPort, PubSubPort, StorePort};
use infrastructure::config::Config;
use infrastructure::persistence::in_memory_store::InMemoryStore;
use infrastructure::persistence::rdb;
use infrastructure::replication::ReplicationState;
use infrastructure::services::acl_service::AclService;
use infrastructure::services::pubsub_service::PubSubService;
use presentation::connection_handler::handle_connection;

#[tokio::main]
async fn main() {
    let config = Arc::new(Config::from_args());

    let store: Arc<dyn StorePort> = Arc::new(InMemoryStore::new());
    let pubsub: Arc<dyn PubSubPort> = Arc::new(PubSubService::new());
    let acl: Arc<dyn AclPort> = Arc::new(AclService::new());

    if let Some(path) = config.rdb_path() {
        rdb::load(&path, &store);
    }

    let repl = if let Some((host, port)) = config.replicaof.clone() {
        let r = ReplicationState::slave();
        let s = Arc::clone(&store);
        let rl = Arc::clone(&r);
        let cfg = Arc::clone(&config);
        tokio::spawn(async move {
            presentation::replica_client::run(host, port, cfg.port, s, rl).await;
        });
        r
    } else {
        ReplicationState::master()
    };

    let addr = format!("127.0.0.1:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("Listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let store = Arc::clone(&store);
                let pubsub = Arc::clone(&pubsub);
                let acl = Arc::clone(&acl);
                let config = Arc::clone(&config);
                let repl = Arc::clone(&repl);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, store, pubsub, acl, config, repl).await {
                        eprintln!("Connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("Accept error: {e}"),
        }
    }
}
