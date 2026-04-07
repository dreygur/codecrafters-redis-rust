use bytes::Bytes;

use crate::{resp, store::StoreService};

pub fn dispatch(args: &[String], store: &StoreService) -> Bytes {
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

fn ping(args: &[String]) -> Bytes {
    match args.get(1) {
        Some(msg) => resp::bulk_string(msg),
        None => Bytes::from_static(b"+PONG\r\n"),
    }
}

fn echo(args: &[String]) -> Bytes {
    match args.get(1) {
        Some(arg) => resp::bulk_string(arg),
        None => resp::error("wrong number of arguments"),
    }
}

fn set(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments");
    }

    let px: Option<u64> = match args.get(3).map(|s| s.to_uppercase()).as_deref() {
        Some("PX") => args.get(4).and_then(|v| v.parse().ok()),
        Some("EX") => args
            .get(4)
            .and_then(|v| v.parse::<u64>().ok().map(|s| s * 1000)),
        _ => None,
    };

    store.set(args[1].clone(), args[2].clone(), px);
    Bytes::from_static(b"+OK\r\n")
}

fn get(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments");
    }

    match store.get(&args[1]) {
        Some(v) => resp::bulk_string(&v),
        None => resp::null_bulk(),
    }
}

fn incr(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments");
    }

    match store.incr(&args[1]) {
        Ok(n) => resp::integer(n),
        Err(e) => resp::error(&e.to_string()),
    }
}

fn subscribe(args: &[String]) -> Bytes {
    match args.get(1) {
        Some(channel) => subscribe_response(channel, 1),
        None => resp::error("wrong number of arguments"),
    }
}

pub fn subscribe_response(channel: &str, count: i64) -> Bytes {
    resp::array(vec![
        resp::bulk_string("subscribe"),
        resp::bulk_string(channel),
        resp::integer(count),
    ])
}
