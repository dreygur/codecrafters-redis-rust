mod application;
mod domain;
mod infrastructure;
mod presentation;

use infrastructure::persistence::in_memory_store::InMemoryStore;
use infrastructure::services::acl_service::AclService;
use infrastructure::services::pubsub_service::PubSubService;
use infrastructure::networking::resp::RespEncoder;
use infrastructure::geo::GeoUtils;
use application::ports::{StorePort, AclPort, PubSubPort};
use domain::entities::Session;

const ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let requirepass = args
        .windows(2)
        .find(|w| w[0] == "--requirepass")
        .map(|w| w[1].clone());

    let store: InMemoryStore = InMemoryStore::new();
    let pubsub = PubSubService::new();
    let acl = AclService::new();

    if let Some(password) = requirepass {
        acl.set_default_password(password);
    }

    let listener = tokio::net::TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {}", ADDR);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let store = store.clone();
                let pubsub = pubsub.clone();
                let acl = acl.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, store, pubsub, acl).await {
                        eprintln!("Connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("Accept error: {e}"),
        }
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    store: impl StorePort,
    pubsub: impl PubSubPort,
    acl: impl AclPort,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use bytes::BytesMut;
    use tokio::sync::mpsc;

    const BUF_SIZE: usize = 512;

    let mut buf = [0u8; BUF_SIZE];
    let mut session = Session::new(acl.is_nopass());
    let (tx, mut rx) = mpsc::unbounded_channel::<bytes::Bytes>();
    let (mut reader, mut writer) = stream.into_split();

    loop {
        tokio::select! {
            result = reader.read(&mut buf) => {
                let n = match result? {
                    0 => {
                        println!("Client disconnected");
                        break;
                    }
                    n => n,
                };

                let Some(args) = RespEncoder::parse(&buf[..n]) else {
                    continue;
                };

                let cmd = args[0].to_uppercase();

                if !session.is_authenticated() && cmd != "AUTH" {
                    writer
                        .write_all(&RespEncoder::raw_error("NOAUTH Authentication required."))
                        .await?;
                    continue;
                }

                if session.is_subscribed()
                    && !matches!(
                        cmd.as_str(),
                        "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE"
                        | "PING" | "QUIT" | "RESET"
                    )
                {
                    let msg = format!(
                        "Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                        args[0].to_lowercase()
                    );
                    writer.write_all(&RespEncoder::error(&msg)).await?;
                    continue;
                }

                let response = match cmd.as_str() {
                    "AUTH" => {
                        let (username, password) = match args.len() {
                            2 => ("default", args[1].as_str()),
                            3 => (args[1].as_str(), args[2].as_str()),
                            _ => {
                                writer.write_all(&RespEncoder::error("wrong number of arguments for 'auth' command")).await?;
                                continue;
                            }
                        };
                        if acl.authenticate(username, password) {
                            session.authenticate(username.to_string());
                            RespEncoder::simple_string("OK")
                        } else {
                            RespEncoder::raw_error("WRONGPASS invalid username-password pair or user is disabled.")
                        }
                    }

                    "ACL" => match args.get(1).map(|s| s.to_uppercase()).as_deref() {
                        Some("WHOAMI") => RespEncoder::bulk_string(session.current_user()),
                        Some("GETUSER") => {
                            let Some(username) = args.get(2).map(String::as_str) else {
                                writer.write_all(&RespEncoder::error("wrong number of arguments for 'acl getuser' command")).await?;
                                continue;
                            };
                            match (acl.user_flags(username), acl.user_passwords(username)) {
                                (Some(flags), Some(passwords)) => RespEncoder::array(vec![
                                    RespEncoder::bulk_string("flags"),
                                    RespEncoder::array(flags.iter().map(|f| RespEncoder::bulk_string(f)).collect()),
                                    RespEncoder::bulk_string("passwords"),
                                    RespEncoder::array(passwords.iter().map(|p| RespEncoder::bulk_string(p)).collect()),
                                    RespEncoder::bulk_string("commands"),
                                    RespEncoder::bulk_string("+@all"),
                                    RespEncoder::bulk_string("keys"),
                                    RespEncoder::bulk_string(""),
                                    RespEncoder::bulk_string("channels"),
                                    RespEncoder::bulk_string("*"),
                                    RespEncoder::bulk_string("selectors"),
                                    RespEncoder::array(vec![]),
                                ]),
                                _ => RespEncoder::null_bulk(),
                            }
                        }
                        Some("SETUSER") => {
                            if args.len() < 4 {
                                RespEncoder::error("wrong number of arguments for 'acl setuser' command")
                            } else {
                                let username = args[1].as_str();
                                let password_arg = &args[3];
                                if !password_arg.starts_with('>') {
                                    RespEncoder::error("syntax error")
                                } else {
                                    let password = password_arg[1..].to_string();
                                    if acl.set_user_password(username, password) {
                                        RespEncoder::simple_string("OK")
                                    } else {
                                        RespEncoder::error("ERR user does not exist")
                                    }
                                }
                            }
                        }
                        _ => RespEncoder::error("unknown ACL subcommand"),
                    },

                    "WATCH" => {
                        if session.is_tx_active() {
                            RespEncoder::error("WATCH inside MULTI is not allowed")
                        } else if args.len() < 2 {
                            RespEncoder::error("wrong number of arguments for 'watch' command")
                        } else {
                            for key in args.iter().skip(1) {
                                session.watch(key, store.key_version(key));
                            }
                            RespEncoder::simple_string("OK")
                        }
                    }
                    "UNWATCH" => {
                        session.unwatch();
                        RespEncoder::simple_string("OK")
                    }

                    "SUBSCRIBE" => {
                        args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
                            if !session.is_subscribed_to(channel) {
                                pubsub.subscribe(channel, tx.clone());
                            }
                            let count = session.subscribe(channel);
                            out.extend_from_slice(&RespEncoder::array(vec![
                                RespEncoder::bulk_string("subscribe"),
                                RespEncoder::bulk_string(channel),
                                RespEncoder::integer(count),
                            ]));
                            out
                        }).freeze()
                    }
                    "UNSUBSCRIBE" => {
                        args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
                            pubsub.unsubscribe(channel, &tx);
                            let count = session.unsubscribe(channel);
                            out.extend_from_slice(&RespEncoder::array(vec![
                                RespEncoder::bulk_string("unsubscribe"),
                                RespEncoder::bulk_string(channel),
                                RespEncoder::integer(count),
                            ]));
                            out
                        }).freeze()
                    }
                    "PUBLISH" => {
                        let channel = args.get(1).map(String::as_str).unwrap_or("");
                        let message = args.get(2).map(String::as_str).unwrap_or("");
                        RespEncoder::integer(pubsub.publish(channel, message))
                    }

                    "MULTI" => {
                        if session.begin_tx() {
                            RespEncoder::simple_string("OK")
                        } else {
                            RespEncoder::error("MULTI calls can not be nested")
                        }
                    }
                    "EXEC" => {
                        if !session.is_tx_active() {
                            RespEncoder::error("EXEC without MULTI")
                        } else {
                            let dirty = session.watched_versions().iter().any(|(key, ver)| store.key_version(key) != *ver);
                            let queue = session.execute_tx();
                            session.unwatch();
                            if dirty {
                                RespEncoder::null_array()
                            } else {
                                let results: Vec<bytes::Bytes> = queue.iter().map(|cmd| dispatch_command(cmd, &store)).collect();
                                RespEncoder::array(results)
                            }
                        }
                    }
                    "DISCARD" => {
                        if session.discard_tx() {
                            session.unwatch();
                            RespEncoder::simple_string("OK")
                        } else {
                            RespEncoder::error("DISCARD without MULTI")
                        }
                    }

                    "PING" if session.is_subscribed() => RespEncoder::array(vec![
                        RespEncoder::bulk_string("pong"),
                        RespEncoder::bulk_string(""),
                    ]),

                    "PING" => match args.get(1) {
                        Some(msg) => RespEncoder::bulk_string(msg),
                        None => RespEncoder::simple_string("PONG"),
                    },
                    "ECHO" => match args.get(1) {
                        Some(arg) => RespEncoder::bulk_string(arg),
                        None => RespEncoder::error("wrong number of arguments for 'echo' command"),
                    },
                    "SET" => {
                        if args.len() < 3 {
                            RespEncoder::error("wrong number of arguments for 'set' command")
                        } else {
                            let ttl = match args.get(3).map(|s| s.to_uppercase()).as_deref() {
                                Some("PX") => args.get(4).and_then(|v| v.parse().ok()),
                                Some("EX") => args.get(4).and_then(|v| v.parse::<u64>().ok().map(|s| s * 1000)),
                                _ => None,
                            };
                            store.set(args[1].clone(), args[2].clone(), ttl);
                            RespEncoder::simple_string("OK")
                        }
                    }
                    "GET" => {
                        if args.len() < 2 {
                            RespEncoder::error("wrong number of arguments for 'get' command")
                        } else {
                            match store.get(&args[1]) {
                                Some(v) => RespEncoder::bulk_string(&v),
                                None => RespEncoder::null_bulk(),
                            }
                        }
                    }
                    "INCR" => {
                        if args.len() < 2 {
                            RespEncoder::error("wrong number of arguments for 'incr' command")
                        } else {
                            match store.incr(&args[1]) {
                                Ok(n) => RespEncoder::integer(n),
                                Err(e) => RespEncoder::error(&e.to_string()),
                            }
                        }
                    }

                    "ZADD" => {
                        if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                            RespEncoder::error("wrong number of arguments for 'zadd' command")
                        } else {
                            let pairs: Option<Vec<(f64, String)>> = args[2..].chunks(2).map(|chunk| {
                                Some((chunk[0].parse::<f64>().ok()?, chunk[1].clone()))
                            }).collect();
                            match pairs {
                                Some(pairs) => RespEncoder::integer(store.zadd(&args[1], pairs)),
                                None => RespEncoder::error("value is not a valid float"),
                            }
                        }
                    }
                    "ZRANGE" => {
                        if args.len() < 4 {
                            RespEncoder::error("wrong number of arguments for 'zrange' command")
                        } else {
                            let start = args[2].parse::<i64>().ok();
                            let stop = args[3].parse::<i64>().ok();
                            match (start, stop) {
                                (Some(start), Some(stop)) => {
                                    let members = store.zrange(&args[1], start, stop);
                                    RespEncoder::array(members.iter().map(|m| RespEncoder::bulk_string(m)).collect())
                                }
                                _ => RespEncoder::error("value is not an integer"),
                            }
                        }
                    }
                    "ZRANK" => {
                        if args.len() < 3 {
                            RespEncoder::error("wrong number of arguments for 'zrank' command")
                        } else {
                            match store.zrank(&args[1], &args[2]) {
                                Some(rank) => RespEncoder::integer(rank),
                                None => RespEncoder::null_bulk(),
                            }
                        }
                    }
                    "ZCARD" => {
                        if args.len() < 2 {
                            RespEncoder::error("wrong number of arguments for 'zcard' command")
                        } else {
                            RespEncoder::integer(store.zcard(&args[1]))
                        }
                    }
                    "ZSCORE" => {
                        if args.len() < 3 {
                            RespEncoder::error("wrong number of arguments for 'zscore' command")
                        } else {
                            match store.zscore(&args[1], &args[2]) {
                                Some(score) => RespEncoder::bulk_string(&score.to_string()),
                                None => RespEncoder::null_bulk(),
                            }
                        }
                    }
                    "ZREM" => {
                        if args.len() < 3 {
                            RespEncoder::error("wrong number of arguments for 'zrem' command")
                        } else {
                            RespEncoder::integer(store.zrem(&args[1], &args[2..].to_vec()))
                        }
                    }

                    "GEOADD" => {
                        if args.len() < 5 || (args.len() - 2) % 3 != 0 {
                            RespEncoder::error("wrong number of arguments for 'geoadd' command")
                        } else {
                            let key = &args[1];
                            let mut added = 0i64;
                            let mut valid = true;
                            for chunk in args[2..].chunks(3) {
                                let lon: Result<f64, _> = chunk[0].parse();
                                let lat: Result<f64, _> = chunk[1].parse();
                                if lon.is_err() || lat.is_err() {
                                    valid = false;
                                    break;
                                }
                                let (lon, lat) = (lon.unwrap(), lat.unwrap());
                                if !GeoUtils::validate(lon, lat) {
                                    valid = false;
                                    break;
                                }
                                if store.geoadd(key, lon, lat, chunk[2].clone()) {
                                    added += 1;
                                }
                            }
                            if valid {
                                RespEncoder::integer(added)
                            } else {
                                RespEncoder::error("value is not a valid float")
                            }
                        }
                    }
                    "GEOPOS" => {
                        if args.len() < 3 {
                            RespEncoder::error("wrong number of arguments for 'geopos' command")
                        } else {
                            let key = &args[1];
                            let items = args[2..].iter().map(|member| {
                                match store.geopos(key, member) {
                                    Some((lon, lat)) => RespEncoder::array(vec![
                                        RespEncoder::bulk_string(&format!("{lon}")),
                                        RespEncoder::bulk_string(&format!("{lat}")),
                                    ]),
                                    None => RespEncoder::null_array(),
                                }
                            }).collect();
                            RespEncoder::array(items)
                        }
                    }
                    "GEODIST" => {
                        if args.len() < 4 {
                            RespEncoder::error("wrong number of arguments for 'geodist' command")
                        } else {
                            let unit = args.get(4).map(String::as_str).unwrap_or("m");
                            match store.geodist(&args[1], &args[2], &args[3]) {
                                Some(dist_m) => {
                                    let converted = GeoUtils::from_metres(dist_m, unit);
                                    RespEncoder::bulk_string(&format!("{:.4}", converted))
                                }
                                None => RespEncoder::null_bulk(),
                            }
                        }
                    }
                    "GEOSEARCH" => {
                        if args.len() < 7 {
                            RespEncoder::error("wrong number of arguments for 'geosearch' command")
                        } else {
                            geosearch_handler(&store, &args)
                        }
                    }

                    _ => RespEncoder::error("unknown command"),
                };

                writer.write_all(&response).await?;
            }
            Some(msg) = rx.recv() => {
                writer.write_all(&msg).await?;
            }
        }
    }

    Ok(())
}

fn geosearch_handler(store: &impl StorePort, args: &[String]) -> bytes::Bytes {
    let key = &args[1];
    let mut i = 2;

    let center = match args[i].to_uppercase().as_str() {
        "FROMLONLAT" => {
            let lon: Result<f64, _> = args[i + 1].parse();
            let lat: Result<f64, _> = args[i + 2].parse();
            match (lon, lat) {
                (Ok(lon), Ok(lat)) => {
                    i += 3;
                    Some((lon, lat))
                }
                _ => None,
            }
        }
        "FROMMEMBER" => {
            let pos = store.geopos(key, &args[i + 1]);
            i += 2;
            pos
        }
        _ => None,
    };

    if center.is_none() {
        return RespEncoder::error("syntax error");
    }

    let (center_lon, center_lat) = center.unwrap();

    if args.get(i).map(String::as_str) != Some("BYRADIUS")
        && args.get(i).map(|s| s.to_uppercase()).as_deref() != Some("BYRADIUS")
    {
        return RespEncoder::error("syntax error");
    }

    let radius: Result<f64, _> = args[i + 1].parse();
    let unit = args[i + 2].as_str();

    if radius.is_err() {
        return RespEncoder::error("value is not a valid float");
    }

    let radius_m = GeoUtils::to_metres(radius.unwrap(), unit);
    i += 3;

    let descending = args.get(i).map(|s| s.to_uppercase() == "DESC").unwrap_or(false);

    let mut hits = store.geosearch_radius(key, center_lon, center_lat, radius_m);
    if descending {
        hits.sort_by(|a, b| b.1.total_cmp(&a.1));
    } else {
        hits.sort_by(|a, b| a.1.total_cmp(&b.1));
    }

    RespEncoder::array(hits.into_iter().map(|(m, _)| RespEncoder::bulk_string(&m)).collect())
}

fn dispatch_command(args: &[String], store: &impl StorePort) -> bytes::Bytes {
    match args[0].to_uppercase().as_str() {
        "SET" => {
            if args.len() < 3 {
                return RespEncoder::error("wrong number of arguments for 'set' command");
            }
            let ttl = match args.get(3).map(|s| s.to_uppercase()).as_deref() {
                Some("PX") => args.get(4).and_then(|v| v.parse().ok()),
                Some("EX") => args.get(4).and_then(|v| v.parse::<u64>().ok().map(|s| s * 1000)),
                _ => None,
            };
            store.set(args[1].clone(), args[2].clone(), ttl);
            RespEncoder::simple_string("OK")
        }
        "GET" => {
            if args.len() < 2 {
                return RespEncoder::error("wrong number of arguments for 'get' command");
            }
            match store.get(&args[1]) {
                Some(v) => RespEncoder::bulk_string(&v),
                None => RespEncoder::null_bulk(),
            }
        }
        "INCR" => {
            if args.len() < 2 {
                return RespEncoder::error("wrong number of arguments for 'incr' command");
            }
            match store.incr(&args[1]) {
                Ok(n) => RespEncoder::integer(n),
                Err(e) => RespEncoder::error(&e.to_string()),
            }
        }
        _ => RespEncoder::error("unknown command"),
    }
}
