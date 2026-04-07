// Uncomment this block to pass the first stage
use std::{
    io::{Read, Result, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let _ = handle(&mut _stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle(stream: &mut TcpStream) -> Result<()> {
    // stream.write(b"+PONG\r\n")?;
    let mut buffer = [0; 512];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("connection closed");
                break;
            }
            Ok(n) => {
                println!("read {} bytes", n);
                stream.write(b"+PONG\r\n")?;
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        }
    }
    Ok(())
}
