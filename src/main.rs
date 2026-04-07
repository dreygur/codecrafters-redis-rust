mod command;
mod error;
mod resp;
mod server;
mod session;
mod store;

use store::StoreService;

const ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind(ADDR).await.unwrap();
    let store = StoreService::new();
    println!("Listening on {}", ADDR);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let store = store.clone();
                tokio::spawn(async move {
                    if let Err(e) = server::handle(stream, store).await {
                        eprintln!("Connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("Accept error: {e}"),
        }
    }
}
