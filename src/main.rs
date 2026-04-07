use std::{
    collections::HashMap,
    io::{Read, Result, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

const ADDR: &str = "127.0.0.1:6379";
const BUF_SIZE: usize = 512;

struct Entry {
    value: String,
    expires_at: Option<Instant>,
}

impl Entry {
    fn new(value: String, px: Option<u64>) -> Self {
        Self {
            value,
            expires_at: px.map(|ms| Instant::now() + Duration::from_millis(ms)),
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |t| Instant::now() > t)
    }
}

type Store = Arc<Mutex<HashMap<String, Entry>>>;

enum TxState {
    Inactive,
    Active(Vec<Vec<String>>),
}

fn main() {
    let listener = TcpListener::bind(ADDR).unwrap();
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    println!("Listening on {}", ADDR);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = Arc::clone(&store);
                thread::spawn(move || handle(stream, store));
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

fn cmd_set(args: &[String], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments\r\n".to_vec();
    }

    let key = args[1].clone();
    let value = args[2].clone();

    let px: Option<u64> = match args.get(3).map(|s| s.to_uppercase()).as_deref() {
        Some("PX") => args.get(4).and_then(|v| v.parse().ok()),
        Some("EX") => args
            .get(4)
            .and_then(|v| v.parse::<u64>().ok().map(|s| s * 1000)),
        _ => None,
    };

    store.lock().unwrap().insert(key, Entry::new(value, px));
    b"+OK\r\n".to_vec()
}

fn cmd_get(args: &[String], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments\r\n".to_vec();
    }

    let mut store = store.lock().unwrap();
    match store.get(&args[1]) {
        Some(entry) if entry.is_expired() => {
            store.remove(&args[1]);
            b"$-1\r\n".to_vec()
        }
        Some(entry) => bulk_string(&entry.value).into_bytes(),
        None => b"$-1\r\n".to_vec(),
    }
}

fn cmd_incr(args: &[String], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments\r\n".to_vec();
    }

    let mut store = store.lock().unwrap();
    let key = &args[1];

    let current = match store.get(key) {
        Some(entry) if entry.is_expired() => {
            store.remove(key);
            0i64
        }
        Some(entry) => match entry.value.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return b"-ERR value is not an integer or out of range\r\n".to_vec(),
        },
        None => 0i64,
    };

    let new_val = current + 1;
    store.insert(key.to_string(), Entry::new(new_val.to_string(), None));
    format!(":{}\r\n", new_val).into_bytes()
}

fn dispatch(args: &[String], store: &Store) -> Vec<u8> {
    match args[0].to_uppercase().as_str() {
        "PING" => b"+PONG\r\n".to_vec(),
        "ECHO" => args
            .get(1)
            .map(|arg| bulk_string(arg).into_bytes())
            .unwrap_or_else(|| b"-ERR wrong number of arguments\r\n".to_vec()),
        "SET" => cmd_set(args, store),
        "GET" => cmd_get(args, store),
        "INCR" => cmd_incr(args, store),
        _ => b"-ERR unknown command\r\n".to_vec(),
    }
}

fn handle(mut stream: TcpStream, store: Store) -> Result<()> {
    let mut buf = [0; BUF_SIZE];
    let mut tx = TxState::Inactive;

    loop {
        match stream.read(&mut buf)? {
            0 => {
                println!("Client disconnected");
                break;
            }
            n => {
                let Some(args) = parse_resp(&buf[..n]) else {
                    continue;
                };

                let response = match args[0].to_uppercase().as_str() {
                    "MULTI" => match tx {
                        TxState::Active(_) => b"-ERR MULTI calls can not be nested\r\n".to_vec(),
                        TxState::Inactive => {
                            tx = TxState::Active(Vec::new());
                            b"+OK\r\n".to_vec()
                        }
                    },
                    "EXEC" => {
                        match tx {
                            TxState::Inactive => b"-ERR EXEC without MULTI\r\n".to_vec(),
                            TxState::Active(ref queue) => {
                                // Execute all queued commands and collect responses
                                let results: Vec<Vec<u8>> =
                                    queue.iter().map(|cmd| dispatch(cmd, &store)).collect();

                                // Encode as RESP array
                                let mut resp = format!("*{}\r\n", results.len()).into_bytes();
                                for r in results {
                                    resp.extend(r);
                                }

                                tx = TxState::Inactive;
                                resp
                            }
                        }
                    }
                    "DISCARD" => match tx {
                        TxState::Inactive => b"-ERR DISCARD without MULTI\r\n".to_vec(),
                        TxState::Active(_) => {
                            tx = TxState::Inactive;
                            b"+OK\r\n".to_vec()
                        }
                    },
                    _ => match tx {
                        TxState::Active(ref mut queue) => {
                            queue.push(args);
                            b"+QUEUED\r\n".to_vec()
                        }
                        TxState::Inactive => dispatch(&args, &store),
                    },
                };

                stream.write_all(&response)?;
            }
        }
    }

    Ok(())
}
