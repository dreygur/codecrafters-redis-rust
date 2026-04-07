use bytes::Bytes;

use crate::{error::RedisError, geo, resp, store::StoreService};

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
        "GEOADD" => geoadd(args, store),
        "GEOPOS" => geopos(args, store),
        "GEODIST" => geodist(args, store),
        "GEOSEARCH" => geosearch(args, store),
        _ => resp::error("unknown command"),
    }
}

fn ping(args: &[String]) -> Bytes {
    match args.get(1) {
        Some(msg) => resp::bulk_string(msg),
        None => resp::simple_string("PONG"),
    }
}

fn echo(args: &[String]) -> Bytes {
    match args.get(1) {
        Some(arg) => resp::bulk_string(arg),
        None => resp::error(&RedisError::WrongArgCount.to_string()),
    }
}

fn set(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 3 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }

    let px: Option<u64> = match args.get(3).map(|s| s.to_uppercase()).as_deref() {
        Some("PX") => args.get(4).and_then(|v| v.parse().ok()),
        Some("EX") => args
            .get(4)
            .and_then(|v| v.parse::<u64>().ok().map(|s| s * 1000)),
        _ => None,
    };

    store.set(args[1].clone(), args[2].clone(), px);
    resp::simple_string("OK")
}

fn get(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 2 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }

    match store.get(&args[1]) {
        Some(v) => resp::bulk_string(&v),
        None => resp::null_bulk(),
    }
}

fn incr(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 2 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }

    match store.incr(&args[1]) {
        Ok(n) => resp::integer(n),
        Err(e) => resp::error(&e.to_string()),
    }
}

fn subscribe(args: &[String]) -> Bytes {
    match args.get(1) {
        Some(channel) => subscribe_response(channel, 1),
        None => resp::error(&RedisError::WrongArgCount.to_string()),
    }
}

fn zadd(args: &[String], store: &StoreService) -> Bytes {
    // ZADD key score member [score member ...]
    if args.len() < 4 || (args.len() - 2) % 2 != 0 {
        return resp::error(&RedisError::WrongArgCount.to_string());
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
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    match store.zrank(&args[1], &args[2]) {
        Some(rank) => resp::integer(rank),
        None => resp::null_bulk(),
    }
}

fn zrange(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 4 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    let (Ok(start), Ok(stop)) = (args[2].parse::<i64>(), args[3].parse::<i64>()) else {
        return resp::error("value is not an integer");
    };
    let members = store.zrange(&args[1], start, stop);
    resp::array(members.iter().map(|m| resp::bulk_string(m)).collect())
}

fn zcard(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 2 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    resp::integer(store.zcard(&args[1]))
}

fn zscore(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 3 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    match store.zscore(&args[1], &args[2]) {
        Some(score) => resp::bulk_string(&score.to_string()),
        None => resp::null_bulk(),
    }
}

fn zrem(args: &[String], store: &StoreService) -> Bytes {
    if args.len() < 3 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    resp::integer(store.zrem(&args[1], &args[2..].to_vec()))
}

// ---------------------------------------------------------------------------
// Geo commands
// ---------------------------------------------------------------------------

fn geoadd(args: &[String], store: &StoreService) -> Bytes {
    // GEOADD key longitude latitude member [longitude latitude member ...]
    if args.len() < 5 || (args.len() - 2) % 3 != 0 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    let key = &args[1];
    let mut added = 0i64;
    for chunk in args[2..].chunks(3) {
        let (Ok(lon), Ok(lat)) = (chunk[0].parse::<f64>(), chunk[1].parse::<f64>()) else {
            return resp::error("value is not a valid float");
        };
        if !geo::validate(lon, lat) {
            return resp::error("invalid longitude,latitude pair");
        }
        if store.geoadd(key, lon, lat, chunk[2].clone()) {
            added += 1;
        }
    }
    resp::integer(added)
}

fn geopos(args: &[String], store: &StoreService) -> Bytes {
    // GEOPOS key member [member ...]
    if args.len() < 3 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    let key = &args[1];
    let items = args[2..]
        .iter()
        .map(|member| match store.geopos(key, member) {
            Some((lon, lat)) => resp::array(vec![
                resp::bulk_string(&format!("{lon}")),
                resp::bulk_string(&format!("{lat}")),
            ]),
            None => resp::null_array(),
        })
        .collect();
    resp::array(items)
}

fn geodist(args: &[String], store: &StoreService) -> Bytes {
    // GEODIST key member1 member2 [m|km|mi|ft]
    if args.len() < 4 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    let unit = args.get(4).map(String::as_str).unwrap_or("m");
    match store.geodist(&args[1], &args[2], &args[3]) {
        Some(dist_m) => resp::bulk_string(&format!("{:.4}", geo::from_metres(dist_m, unit))),
        None => resp::null_bulk(),
    }
}

fn geosearch(args: &[String], store: &StoreService) -> Bytes {
    // GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit [ASC|DESC]
    // GEOSEARCH key FROMMEMBER member  BYRADIUS radius unit [ASC|DESC]
    if args.len() < 7 {
        return resp::error(&RedisError::WrongArgCount.to_string());
    }
    let key = &args[1];
    let mut i = 2;

    // --- centre ---
    let (center_lon, center_lat) = match args[i].to_uppercase().as_str() {
        "FROMLONLAT" => {
            let (Ok(lon), Ok(lat)) = (args[i + 1].parse::<f64>(), args[i + 2].parse::<f64>())
            else {
                return resp::error("value is not a valid float");
            };
            i += 3;
            (lon, lat)
        }
        "FROMMEMBER" => {
            let Some(pos) = store.geopos(key, &args[i + 1]) else {
                return resp::error("could not find the requested member");
            };
            i += 2;
            pos
        }
        _ => return resp::error("syntax error"),
    };

    // --- search shape ---
    if args.get(i).map(String::as_str) != Some("BYRADIUS")
        && args.get(i).map(|s| s.to_uppercase()).as_deref() != Some("BYRADIUS")
    {
        return resp::error("syntax error");
    }
    let (Ok(radius), unit) = (args[i + 1].parse::<f64>(), args[i + 2].as_str()) else {
        return resp::error("value is not a valid float");
    };
    let radius_m = geo::to_metres(radius, unit);
    i += 3;

    // --- optional sort ---
    let descending = args
        .get(i)
        .map(|s| s.to_uppercase() == "DESC")
        .unwrap_or(false);

    let mut hits = store.geosearch_radius(key, center_lon, center_lat, radius_m);
    if descending {
        hits.sort_by(|a, b| b.1.total_cmp(&a.1));
    } else {
        hits.sort_by(|a, b| a.1.total_cmp(&b.1));
    }

    resp::array(hits.into_iter().map(|(m, _)| resp::bulk_string(&m)).collect())
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
