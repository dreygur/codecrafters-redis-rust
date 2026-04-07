use crate::{
    resp,
    store::{Entry, Store},
};

pub fn dispatch(args: &[String], store: &Store) -> Vec<u8> {
    match args[0].to_uppercase().as_str() {
        "PING" => ping(args),
        "ECHO" => echo(args),
        "SET" => set(args, store),
        "GET" => get(args, store),
        "INCR" => incr(args, store),
        "SUBSCRIBE" => subscribe(args),
        _ => resp::error("unknown command"),
    }
}

fn ping(args: &[String]) -> Vec<u8> {
    match args.get(1) {
        Some(msg) => resp::bulk_string(msg),
        None => b"+PONG\r\n".to_vec(),
    }
}

fn echo(args: &[String]) -> Vec<u8> {
    match args.get(1) {
        Some(arg) => resp::bulk_string(arg),
        None => resp::error("wrong number of arguments"),
    }
}

fn set(args: &[String], store: &Store) -> Vec<u8> {
    if args.len() < 3 {
        return resp::error("wrong number of arguments");
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

fn get(args: &[String], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return resp::error("wrong number of arguments");
    }

    let mut store = store.lock().unwrap();
    match store.get(&args[1]) {
        Some(entry) if entry.is_expired() => {
            store.remove(&args[1]);
            resp::NULL_BULK.to_vec()
        }
        Some(entry) => resp::bulk_string(&entry.value),
        None => resp::NULL_BULK.to_vec(),
    }
}

fn incr(args: &[String], store: &Store) -> Vec<u8> {
    if args.len() < 2 {
        return resp::error("wrong number of arguments");
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
    resp::integer(new_val)
}

fn subscribe(args: &[String]) -> Vec<u8> {
    match args.get(1) {
        Some(channel) => resp::array(vec![
            resp::bulk_string("subscribe"),
            resp::bulk_string(channel),
            resp::integer(1),
        ]),
        None => resp::error("wrong number of arguments"),
    }
}
