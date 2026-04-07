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
        "ZADD" => zadd(args, store),
        "ZRANK" => zrank(args, store),
        "ZRANGE" => zrange(args, store),
        "ZCARD" => zcard(args, store),
        "ZSCORE" => zscore(args, store),
        "ZREM" => zrem(args, store),
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

fn zadd(args: &[String], store: &StoreService) -> Bytes {
    // ZADD key score member [score member ...]
    if args.len() < 4 || (args.len() - 2) % 2 != 0 {
        return resp::error("wrong number of arguments");
    }
    let key = &args[1];
    let pairs: Option<Vec<(f64, String)>> = args[2..]
        .chunks(2)
        .map(|chunk| Some((chunk[0].parse::<f64>().ok()?, chunk[1].clone())))
        .collect();
    match pairs {
        Some(pairs) => resp::integer(store.zadd(key, pairs)),
        None => resp::error("value is not a valid float"),
    }
}

fn zrank(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments");
    }
    match store.zrank(&args[1], &args[2]) {
        Some(rank) => resp::integer(rank),
        None => resp::null_bulk(),
    }
}

fn zrange(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 4 {
        return resp::error("wrong number of arguments");
    }
    let (Ok(start), Ok(stop)) = (args[2].parse::<i64>(), args[3].parse::<i64>()) else {
        return resp::error("value is not an integer");
    };
    let members = store.zrange(&args[1], start, stop);
    resp::array(members.iter().map(|m| resp::bulk_string(m)).collect())
}

fn zcard(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments");
    }
    resp::integer(store.zcard(&args[1]))
}

fn zscore(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments");
    }
    match store.zscore(&args[1], &args[2]) {
        Some(score) => resp::bulk_string(&score.to_string()),
        None => resp::null_bulk(),
    }
}

fn zrem(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments");
    }
    resp::integer(store.zrem(&args[1], &args[2..].to_vec()))
}

pub fn unsubscribe_response(channel: &str, count: i64) -> Bytes {
    resp::array(vec![
        resp::bulk_string("unsubscribe"),
        resp::bulk_string(channel),
        resp::integer(count),
    ])
}

pub fn subscribe_response(channel: &str, count: i64) -> Bytes {
    resp::array(vec![
        resp::bulk_string("subscribe"),
        resp::bulk_string(channel),
        resp::integer(count),
    ])
}
