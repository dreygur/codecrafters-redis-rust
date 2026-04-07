use std::{
    io::{Read, Result, Write},
    net::{TcpListener, TcpStream},
    thread,
};

const ADDR: &str = "127.0.0.1:6379";
const BUF_SIZE: usize = 512;

fn main() {
    let listener = TcpListener::bind(ADDR).unwrap();
    println!("Listening on {}", ADDR);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || handle(stream));
            }
            Err(e) => eprintln!("Accept error: {}", e),
        }
    }
}

fn parse_resp(input: &[u8]) -> Option<Vec<String>> {
    let s = std::str::from_utf8(input).ok()?;
    let mut lines = s.split("\r\n");

    let count = lines.next()?.strip_prefix('*')?.parse::<usize>().ok()?;
    let mut args = Vec::with_capacity(count);

    for _ in 0..count {
        lines.next()?.strip_prefix('$')?.parse::<usize>().ok()?;
        args.push(lines.next()?.to_string());
    }

    Some(args)
}

fn bulk_string(s: &str) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}

fn dispatch(args: &[String]) -> Vec<u8> {
    match args[0].to_uppercase().as_str() {
        "PING" => b"+PONG\r\n".to_vec(),
        "ECHO" => args
            .get(1)
            .map(|arg| bulk_string(arg).into_bytes())
            .unwrap_or_else(|| b"-ERR wrong number of arguments\r\n".to_vec()),
        _ => b"-ERR unknown command\r\n".to_vec(),
    }
}

fn handle(mut stream: TcpStream) -> Result<()> {
    let mut buf = [0; BUF_SIZE];

    loop {
        match stream.read(&mut buf)? {
            0 => {
                println!("Client disconnected");
                break;
            }
            n => {
                if let Some(args) = parse_resp(&buf[..n]) {
                    stream.write_all(&dispatch(&args))?;
                }
            }
        }
    }

    Ok(())
}
