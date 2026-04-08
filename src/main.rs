mod application;
mod domain;
mod infrastructure;
mod presentation;

use std::sync::Arc;

use application::ports::{AclPort, PubSubPort, StorePort};
use infrastructure::persistence::in_memory_store::InMemoryStore;
use infrastructure::services::acl_service::AclService;
use infrastructure::services::pubsub_service::PubSubService;
use presentation::connection_handler::handle_connection;

const ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let requirepass = args
        .windows(2)
        .find(|w| w[0] == "--requirepass")
        .map(|w| w[1].clone());

    let store: Arc<dyn StorePort> = Arc::new(InMemoryStore::new());
    let pubsub: Arc<dyn PubSubPort> = Arc::new(PubSubService::new());
    let acl: Arc<dyn AclPort> = Arc::new(AclService::new());

    if let Some(password) = requirepass {
        acl.set_default_password(password);
    }

    let listener = tokio::net::TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {}", ADDR);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let store = Arc::clone(&store);
                let pubsub = Arc::clone(&pubsub);
                let acl = Arc::clone(&acl);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, store, pubsub, acl).await {
                        eprintln!("Connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("Accept error: {e}"),
        }
    }
}
