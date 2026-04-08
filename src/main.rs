mod acl;
mod config;
mod geo;
mod pubsub;
mod rdb;
mod replication;
mod resp;
mod server;
mod session;
mod store;

use std::sync::Arc;

use acl::Acl;
use config::Config;
use pubsub::PubSub;
use replication::ReplicationState;
use store::Store;

#[tokio::main]
async fn main() {
    let config = Arc::new(Config::from_args());
    let store = Arc::new(Store::new());
    let pubsub = Arc::new(PubSub::new());
    let acl = Arc::new(Acl::new());

    if let Some(path) = config.rdb_path() {
        rdb::load(&path, &store);
    }

    let repl = if let Some((host, port)) = config.replicaof.clone() {
        let repl = Arc::new(ReplicationState::new_slave());
        let store_clone = Arc::clone(&store);
        let repl_clone = Arc::clone(&repl);
        let my_port = config.port;
        tokio::spawn(async move {
            replication::run_replica_client(host, port, my_port, store_clone, repl_clone).await;
        });
        repl
    } else {
        Arc::new(ReplicationState::new_master())
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
                    if let Err(error) = server::handle_connection(stream, store, pubsub, acl, config, repl).await {
                        eprintln!("Connection error: {error}");
                    }
                });
            }
            Err(error) => eprintln!("Accept error: {error}"),
        }
    }
}
