mod acl;
mod command;
mod error;
mod geo;
mod pubsub;
mod resp;
mod server;
mod session;
mod sorted_set;
mod store;

use acl::AclService;
use pubsub::PubSubService;
use store::StoreService;

const ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let requirepass = args
        .windows(2)
        .find(|w| w[0] == "--requirepass")
        .map(|w| w[1].clone());

    let store = StoreService::new();
    let pubsub = PubSubService::new();
    let acl = AclService::new();

    if let Some(password) = requirepass {
        acl.set_default_password(password);
    }

    let listener = tokio::net::TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {}", ADDR);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let store = store.clone();
                let pubsub = pubsub.clone();
                let acl = acl.clone();
                tokio::spawn(async move {
                    if let Err(e) = server::handle(stream, store, pubsub, acl).await {
                        eprintln!("Connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("Accept error: {e}"),
        }
    }
}
