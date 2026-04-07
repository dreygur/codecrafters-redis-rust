use std::{
    io::{Read, Result, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    let _ = handle(&mut stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle(stream: &mut TcpStream) -> Result<()> {
    let mut buffer = [0; 512];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(_) => {
                stream.write_all(b"+PONG\r\n")?;
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        }
    }
    Ok(())
}
