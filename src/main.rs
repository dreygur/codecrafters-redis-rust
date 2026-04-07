mod command;
mod resp;
mod server;
mod store;
mod transaction;

use std::{net::TcpListener, thread};

use store::new_store;

const ADDR: &str = "127.0.0.1:6379";

fn main() {
    let listener = TcpListener::bind(ADDR).unwrap();
    let store = new_store();
    println!("Listening on {}", ADDR);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = std::sync::Arc::clone(&store);
                thread::spawn(move || server::handle(stream, store));
            }
            Err(e) => eprintln!("Accept error: {}", e),
        }
    }
}
