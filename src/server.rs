use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::acl::Acl;
use crate::config::Config;
use crate::pubsub::PubSub;
use crate::replication::ReplicationState;
use crate::session::Session;
use crate::store::{Store, StreamEntry, StreamNotification};
use crate::{geo, resp};

const BUF_SIZE: usize = 512;

// Shared state passed to every command handler
struct Ctx {
    store: Arc<Store>,
    pubsub: Arc<PubSub>,
    acl: Arc<Acl>,
    config: Arc<Config>,
}

// ── Connection handler ────────────────────────────────────────────────────────

pub async fn handle_connection(
    stream: tokio::net::TcpStream,
    store: Arc<Store>,
    pubsub: Arc<PubSub>,
    acl: Arc<Acl>,
    config: Arc<Config>,
    repl: Arc<ReplicationState>,
) -> anyhow::Result<()> {
    let ctx = Ctx { store, pubsub, acl, config };
    let mut session = Session::new(ctx.acl.is_nopass());
    let (pubsub_tx, mut pubsub_rx) = mpsc::unbounded_channel::<Bytes>();
    let (mut reader, mut writer) = stream.into_split();
    let mut buf = [0u8; BUF_SIZE];

    loop {
        tokio::select! {
            result = reader.read(&mut buf) => {
                let n = match result? {
                    0 => { println!("Client disconnected"); break; }
                    n => n,
                };

                let Some(args) = resp::parse(&buf[..n]) else { continue; };
                let cmd = args[0].to_uppercase();

                let is_replication_cmd = matches!(cmd.as_str(), "REPLCONF" | "PSYNC");
                if !session.is_authenticated() && cmd != "AUTH" && !is_replication_cmd {
                    writer.write_all(&resp::raw_error("NOAUTH Authentication required.")).await?;
                    continue;
                }

                if cmd == "PSYNC" {
                    drop(pubsub_rx);
                    crate::replication::run_replica_server(reader, writer, repl).await?;
                    return Ok(());
                }

                if cmd == "REPLCONF" {
                    writer.write_all(&resp::simple_string("OK")).await?;
                    continue;
                }

                if cmd == "INFO" {
                    writer.write_all(&build_info_response(&ctx.config, &repl, &args)).await?;
                    continue;
                }

                if cmd == "WAIT" {
                    let response = handle_wait(&args, &repl).await;
                    writer.write_all(&response).await?;
                    continue;
                }

                if session.is_subscribed() && !is_pubsub_command(&cmd) {
                    let msg = format!(
                        "Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                        args[0].to_lowercase()
                    );
                    writer.write_all(&resp::error(&msg)).await?;
                    continue;
                }

                if session.is_tx_active() && !matches!(cmd.as_str(), "MULTI" | "EXEC" | "DISCARD" | "WATCH") {
                    session.enqueue(args.to_vec());
                    writer.write_all(&resp::simple_string("QUEUED")).await?;
                    continue;
                }

                if cmd == "BLPOP" {
                    handle_blpop(&args, &ctx, &mut writer).await?;
                    continue;
                }

                if cmd == "XREAD" && args.iter().any(|a| a.eq_ignore_ascii_case("BLOCK")) {
                    handle_xread_block(&args, &ctx, &mut writer).await?;
                    continue;
                }

                let response = dispatch(&cmd, &args, &mut session, &ctx, &pubsub_tx);
                writer.write_all(&response).await?;

                if repl.role == "master" && is_write_command(&cmd) {
                    repl.propagate(encode_command(&args));
                }
            }
            Some(msg) = pubsub_rx.recv() => {
                writer.write_all(&msg).await?;
            }
        }
    }

    Ok(())
}

// ── Command dispatch ──────────────────────────────────────────────────────────

fn dispatch(
    cmd: &str,
    args: &[String],
    session: &mut Session,
    ctx: &Ctx,
    pubsub_tx: &mpsc::UnboundedSender<Bytes>,
) -> Bytes {
    match cmd {
        "AUTH" => cmd_auth(args, session, ctx),

        "ACL" => match args.get(1).map(|s| s.to_uppercase()).as_deref() {
            Some("WHOAMI") => resp::bulk_string(session.current_user()),
            Some("GETUSER") => match args.get(2) {
                Some(username) => cmd_acl_getuser(username, ctx),
                None => resp::error("wrong number of arguments for 'acl getuser' command"),
            },
            Some("SETUSER") => cmd_acl_setuser(args, ctx),
            _ => resp::error("unknown ACL subcommand"),
        },

        "PING" => {
            if session.is_subscribed() {
                let msg = args.get(1).map(|s| s.as_str()).unwrap_or("");
                resp::array(vec![resp::bulk_string("pong"), resp::bulk_string(msg)])
            } else {
                match args.get(1) {
                    Some(msg) => resp::bulk_string(msg),
                    None => resp::simple_string("PONG"),
                }
            }
        }

        "ECHO" => match args.get(1) {
            Some(msg) => resp::bulk_string(msg),
            None => resp::error("wrong number of arguments for 'echo' command"),
        },

        "GET" => cmd_get(args, ctx),
        "SET" => cmd_set(args, ctx),
        "INCR" => cmd_incr(args, ctx),
        "KEYS" => cmd_keys(args, ctx),

        "TYPE" => match args.get(1) {
            Some(key) => resp::simple_string(ctx.store.get_type(key)),
            None => resp::error("wrong number of arguments for 'type' command"),
        },

        "RPUSH" => cmd_rpush(args, ctx),
        "LPUSH" => cmd_lpush(args, ctx),
        "LPOP" => cmd_lpop(args, ctx),
        "RPOP" => cmd_rpop(args, ctx),
        "LRANGE" => cmd_lrange(args, ctx),
        "LLEN" => cmd_llen(args, ctx),
        "LREM" => cmd_lrem(args, ctx),

        "ZADD" => cmd_zadd(args, ctx),
        "ZRANGE" => cmd_zrange(args, ctx),
        "ZRANK" => cmd_zrank(args, ctx),
        "ZCARD" => cmd_zcard(args, ctx),
        "ZSCORE" => cmd_zscore(args, ctx),
        "ZREM" => cmd_zrem(args, ctx),

        "GEOADD" => cmd_geoadd(args, ctx),
        "GEOPOS" => cmd_geopos(args, ctx),
        "GEODIST" => cmd_geodist(args, ctx),
        "GEOSEARCH" => cmd_geosearch(args, ctx),

        "XADD" => cmd_xadd(args, ctx),
        "XRANGE" => cmd_xrange(args, ctx),
        "XREAD" => cmd_xread(args, ctx),

        "PUBLISH" => cmd_publish(args, ctx),

        "SUBSCRIBE" => cmd_subscribe(args, session, ctx, pubsub_tx),
        "UNSUBSCRIBE" => cmd_unsubscribe(args, session, ctx, pubsub_tx),

        "WATCH" => cmd_watch(args, session, ctx),
        "UNWATCH" => {
            session.unwatch();
            resp::simple_string("OK")
        }
        "MULTI" => {
            if session.begin_tx() {
                resp::simple_string("OK")
            } else {
                resp::error("MULTI calls can not be nested")
            }
        }
        "EXEC" => cmd_exec(args, session, ctx),
        "DISCARD" => {
            if session.discard_tx() {
                session.unwatch();
                resp::simple_string("OK")
            } else {
                resp::error("DISCARD without MULTI")
            }
        }

        "CONFIG" => cmd_config(args, ctx),

        _ => resp::error("unknown command"),
    }
}

// ── Auth and ACL ──────────────────────────────────────────────────────────────

fn cmd_auth(args: &[String], session: &mut Session, ctx: &Ctx) -> Bytes {
    let (username, password) = match args.len() {
        2 => ("default", args[1].as_str()),
        3 => (args[1].as_str(), args[2].as_str()),
        _ => return resp::error("wrong number of arguments for 'auth' command"),
    };
    if ctx.acl.authenticate(username, password) {
        session.authenticate(username.to_string());
        resp::simple_string("OK")
    } else {
        resp::raw_error("WRONGPASS invalid username-password pair or user is disabled.")
    }
}

fn cmd_acl_getuser(username: &str, ctx: &Ctx) -> Bytes {
    match (ctx.acl.user_flags(username), ctx.acl.user_passwords(username)) {
        (Some(flags), Some(passwords)) => resp::array(vec![
            resp::bulk_string("flags"),
            resp::array(flags.iter().map(|f| resp::bulk_string(f)).collect()),
            resp::bulk_string("passwords"),
            resp::array(passwords.iter().map(|p| resp::bulk_string(p)).collect()),
            resp::bulk_string("commands"),
            resp::bulk_string("+@all"),
            resp::bulk_string("keys"),
            resp::bulk_string(""),
            resp::bulk_string("channels"),
            resp::bulk_string("*"),
            resp::bulk_string("selectors"),
            resp::array(vec![]),
        ]),
        _ => resp::null_bulk(),
    }
}

fn cmd_acl_setuser(args: &[String], ctx: &Ctx) -> Bytes {
    let (Some(username), Some(password_arg)) = (args.get(2), args.get(3)) else {
        return resp::error("wrong number of arguments for 'acl setuser' command");
    };
    let password = password_arg.strip_prefix('>').unwrap_or(password_arg);
    if ctx.acl.set_user_password(username, password.to_string()) {
        resp::simple_string("OK")
    } else {
        resp::error("user does not exist")
    }
}

// ── String commands ───────────────────────────────────────────────────────────

fn cmd_get(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments for 'get' command");
    }
    match ctx.store.get(&args[1]) {
        Some(value) => resp::bulk_string(&value),
        None => resp::null_bulk(),
    }
}

fn cmd_set(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments for 'set' command");
    }
    let ttl = match args.get(3).map(|s| s.to_uppercase()).as_deref() {
        Some("PX") => args.get(4).and_then(|v| v.parse().ok()),
        Some("EX") => args.get(4).and_then(|v| v.parse::<u64>().ok().map(|s| s * 1000)),
        _ => None,
    };
    ctx.store.set(args[1].clone(), args[2].clone(), ttl);
    resp::simple_string("OK")
}

fn cmd_incr(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments for 'incr' command");
    }
    match ctx.store.incr(&args[1]) {
        Ok(n) => resp::integer(n),
        Err(error) => resp::error(&error.to_string()),
    }
}

fn cmd_keys(args: &[String], ctx: &Ctx) -> Bytes {
    let pattern = args.get(1).map(|s| s.as_str()).unwrap_or("*");
    let keys = ctx.store.keys(pattern);
    resp::array(keys.iter().map(|k| resp::bulk_string(k)).collect())
}

// ── List commands ─────────────────────────────────────────────────────────────

fn cmd_rpush(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments for 'rpush' command");
    }
    resp::integer(ctx.store.rpush(&args[1], args[2..].to_vec()))
}

fn cmd_lpush(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments for 'lpush' command");
    }
    resp::integer(ctx.store.lpush(&args[1], args[2..].to_vec()))
}

fn cmd_lpop(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments for 'lpop' command");
    }
    if let Some(count_str) = args.get(2) {
        let Ok(count) = count_str.parse::<usize>() else {
            return resp::error("value is not an integer or out of range");
        };
        let items = ctx.store.lpop_count(&args[1], count);
        if items.is_empty() {
            return resp::null_array();
        }
        return resp::array(items.iter().map(|v| resp::bulk_string(v)).collect());
    }
    match ctx.store.lpop(&args[1]) {
        Some(value) => resp::bulk_string(&value),
        None => resp::null_bulk(),
    }
}

fn cmd_rpop(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments for 'rpop' command");
    }
    match ctx.store.rpop(&args[1]) {
        Some(value) => resp::bulk_string(&value),
        None => resp::null_bulk(),
    }
}

fn cmd_lrange(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 4 {
        return resp::error("wrong number of arguments for 'lrange' command");
    }
    let start: i64 = args[2].parse().unwrap_or(0);
    let stop: i64 = args[3].parse().unwrap_or(-1);
    let items = ctx.store.lrange(&args[1], start, stop);
    resp::array(items.into_iter().map(|s| resp::bulk_string(&s)).collect())
}

fn cmd_llen(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments for 'llen' command");
    }
    resp::integer(ctx.store.llen(&args[1]))
}

fn cmd_lrem(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 4 {
        return resp::error("wrong number of arguments for 'lrem' command");
    }
    let count: i64 = args[2].parse().unwrap_or(0);
    resp::integer(ctx.store.lrem(&args[1], count, &args[3]))
}

// ── Sorted set commands ───────────────────────────────────────────────────────

fn cmd_zadd(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 4 || (args.len() - 2) % 2 != 0 {
        return resp::error("wrong number of arguments for 'zadd' command");
    }
    let pairs: Option<Vec<(f64, String)>> = args[2..]
        .chunks(2)
        .map(|c| Some((c[0].parse::<f64>().ok()?, c[1].clone())))
        .collect();
    match pairs {
        Some(pairs) => resp::integer(ctx.store.zadd(&args[1], pairs)),
        None => resp::error("value is not a valid float"),
    }
}

fn cmd_zrange(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 4 {
        return resp::error("wrong number of arguments for 'zrange' command");
    }
    let Ok(start) = args[2].parse::<i64>() else {
        return resp::error("value is not an integer");
    };
    let Ok(stop) = args[3].parse::<i64>() else {
        return resp::error("value is not an integer");
    };
    let members = ctx.store.zrange(&args[1], start, stop);
    resp::array(members.iter().map(|m| resp::bulk_string(m)).collect())
}

fn cmd_zrank(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments for 'zrank' command");
    }
    match ctx.store.zrank(&args[1], &args[2]) {
        Some(rank) => resp::integer(rank),
        None => resp::null_bulk(),
    }
}

fn cmd_zcard(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 2 {
        return resp::error("wrong number of arguments for 'zcard' command");
    }
    resp::integer(ctx.store.zcard(&args[1]))
}

fn cmd_zscore(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments for 'zscore' command");
    }
    match ctx.store.zscore(&args[1], &args[2]) {
        Some(score) => resp::bulk_string(&score.to_string()),
        None => resp::null_bulk(),
    }
}

fn cmd_zrem(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments for 'zrem' command");
    }
    resp::integer(ctx.store.zrem(&args[1], &args[2..].to_vec()))
}

// ── Geo commands ──────────────────────────────────────────────────────────────

fn cmd_geoadd(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 5 || (args.len() - 2) % 3 != 0 {
        return resp::error("wrong number of arguments for 'geoadd' command");
    }
    let key = &args[1];
    let mut added = 0i64;
    for chunk in args[2..].chunks(3) {
        let Ok(lon) = chunk[0].parse::<f64>() else {
            return resp::error("value is not a valid float");
        };
        let Ok(lat) = chunk[1].parse::<f64>() else {
            return resp::error("value is not a valid float");
        };
        if !geo::validate(lon, lat) {
            return resp::error("invalid longitude,latitude pair");
        }
        if ctx.store.geoadd(key, lon, lat, chunk[2].clone()) {
            added += 1;
        }
    }
    resp::integer(added)
}

fn cmd_geopos(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments for 'geopos' command");
    }
    let key = &args[1];
    let items = args[2..].iter().map(|member| {
        match ctx.store.geopos(key, member) {
            Some((lon, lat)) => resp::array(vec![
                resp::bulk_string(&format!("{lon}")),
                resp::bulk_string(&format!("{lat}")),
            ]),
            None => resp::null_array(),
        }
    }).collect();
    resp::array(items)
}

fn cmd_geodist(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 4 {
        return resp::error("wrong number of arguments for 'geodist' command");
    }
    let unit = args.get(4).map(String::as_str).unwrap_or("m");
    match ctx.store.geodist(&args[1], &args[2], &args[3]) {
        Some(dist_m) => resp::bulk_string(&format!("{:.4}", geo::from_metres(dist_m, unit))),
        None => resp::null_bulk(),
    }
}

fn cmd_geosearch(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 7 {
        return resp::error("wrong number of arguments for 'geosearch' command");
    }
    let key = &args[1];
    let mut i = 2;

    let (center_lon, center_lat) = match args[i].to_uppercase().as_str() {
        "FROMLONLAT" => {
            let Ok(lon) = args[i + 1].parse::<f64>() else {
                return resp::error("value is not a valid float");
            };
            let Ok(lat) = args[i + 2].parse::<f64>() else {
                return resp::error("value is not a valid float");
            };
            i += 3;
            (lon, lat)
        }
        "FROMMEMBER" => {
            let Some(pos) = ctx.store.geopos(key, &args[i + 1]) else {
                return resp::error("could not find the requested member");
            };
            i += 2;
            pos
        }
        _ => return resp::error("syntax error"),
    };

    if args.get(i).map(|s| s.to_uppercase()).as_deref() != Some("BYRADIUS") {
        return resp::error("syntax error");
    }

    let Ok(radius) = args[i + 1].parse::<f64>() else {
        return resp::error("value is not a valid float");
    };
    let radius_m = geo::to_metres(radius, args[i + 2].as_str());
    i += 3;

    let descending = args.get(i).map(|s| s.to_uppercase() == "DESC").unwrap_or(false);
    let mut hits = ctx.store.geosearch_radius(key, center_lon, center_lat, radius_m);

    if descending {
        hits.sort_by(|a, b| b.1.total_cmp(&a.1));
    } else {
        hits.sort_by(|a, b| a.1.total_cmp(&b.1));
    }

    resp::array(hits.into_iter().map(|(m, _)| resp::bulk_string(&m)).collect())
}

// ── Stream commands ───────────────────────────────────────────────────────────

fn cmd_xadd(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 4 {
        return resp::error("wrong number of arguments for 'xadd' command");
    }
    let fields: Vec<(String, String)> = args[3..]
        .chunks(2)
        .filter(|c| c.len() == 2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    match ctx.store.xadd(&args[1], &args[2], fields) {
        Ok(id) => resp::bulk_string(&id),
        Err(error) => resp::error(&error.to_string()),
    }
}

fn cmd_xrange(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 4 {
        return resp::error("wrong number of arguments for 'xrange' command");
    }
    let count = args.get(5).and_then(|c| c.parse().ok());
    let entries = ctx.store.xrange(&args[1], &args[2], &args[3], count);
    encode_stream_entries(entries)
}

fn cmd_xread(args: &[String], ctx: &Ctx) -> Bytes {
    let Some((count, keys, ids)) = parse_xread_args(args) else {
        return resp::error("wrong number of arguments for 'xread' command");
    };
    let results = ctx.store.xread(&keys, &ids);
    if results.is_empty() {
        return resp::null_array();
    }
    encode_xread_results(results, count)
}

// ── Pub/sub commands ──────────────────────────────────────────────────────────

fn cmd_publish(args: &[String], ctx: &Ctx) -> Bytes {
    if args.len() < 3 {
        return resp::error("wrong number of arguments for 'publish' command");
    }
    resp::integer(ctx.pubsub.publish(&args[1], &args[2]))
}

fn cmd_subscribe(
    args: &[String],
    session: &mut Session,
    ctx: &Ctx,
    pubsub_tx: &mpsc::UnboundedSender<Bytes>,
) -> Bytes {
    args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
        if !session.is_subscribed_to(channel) {
            ctx.pubsub.subscribe(channel, pubsub_tx.clone());
        }
        let count = session.subscribe_to(channel);
        out.extend_from_slice(&resp::array(vec![
            resp::bulk_string("subscribe"),
            resp::bulk_string(channel),
            resp::integer(count),
        ]));
        out
    }).freeze()
}

fn cmd_unsubscribe(
    args: &[String],
    session: &mut Session,
    ctx: &Ctx,
    pubsub_tx: &mpsc::UnboundedSender<Bytes>,
) -> Bytes {
    args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
        ctx.pubsub.unsubscribe(channel, pubsub_tx);
        let count = session.unsubscribe_from(channel);
        out.extend_from_slice(&resp::array(vec![
            resp::bulk_string("unsubscribe"),
            resp::bulk_string(channel),
            resp::integer(count),
        ]));
        out
    }).freeze()
}

// ── Transaction commands ──────────────────────────────────────────────────────

fn cmd_watch(args: &[String], session: &mut Session, ctx: &Ctx) -> Bytes {
    if session.is_tx_active() {
        return resp::error("WATCH inside MULTI is not allowed");
    }
    if args.len() < 2 {
        return resp::error("wrong number of arguments for 'watch' command");
    }
    for key in args.iter().skip(1) {
        session.watch(key, ctx.store.key_version(key));
    }
    resp::simple_string("OK")
}

fn cmd_exec(args: &[String], session: &mut Session, ctx: &Ctx) -> Bytes {
    if !session.is_tx_active() {
        return resp::error("EXEC without MULTI");
    }
    let watched: Vec<(String, u64)> = session.watched_versions()
        .iter()
        .map(|(key, version)| (key.clone(), *version))
        .collect();
    let queue = session.execute_tx();
    session.unwatch();

    let dirty = watched.iter().any(|(key, version)| ctx.store.key_version(key) != *version);
    if dirty {
        return resp::null_array();
    }

    // Dispatch each queued command; pubsub_tx isn't needed during EXEC
    let (dummy_tx, _) = mpsc::unbounded_channel();
    let mut dummy_session = Session::new(true);
    let results: Vec<Bytes> = queue.iter()
        .map(|cmd_args| {
            let cmd = cmd_args[0].to_uppercase();
            dispatch(&cmd, cmd_args, &mut dummy_session, ctx, &dummy_tx)
        })
        .collect();

    // Ignore unused args warning — needed for signature consistency
    let _ = args;

    resp::array(results)
}

// ── CONFIG command ────────────────────────────────────────────────────────────

fn cmd_config(args: &[String], ctx: &Ctx) -> Bytes {
    let sub = args.get(1).map(|s| s.to_uppercase());
    match sub.as_deref() {
        Some("GET") => {
            let Some(param) = args.get(2) else {
                return resp::error("wrong number of arguments for 'config get' command");
            };
            let param = param.to_lowercase();
            let mut pairs: Vec<Bytes> = vec![];
            if param == "dir" || param == "*" {
                if let Some(dir) = &ctx.config.dir {
                    pairs.push(resp::bulk_string("dir"));
                    pairs.push(resp::bulk_string(dir));
                }
            }
            if param == "dbfilename" || param == "*" {
                if let Some(name) = &ctx.config.dbfilename {
                    pairs.push(resp::bulk_string("dbfilename"));
                    pairs.push(resp::bulk_string(name));
                }
            }
            resp::array(pairs)
        }
        _ => resp::error("unknown CONFIG subcommand"),
    }
}

// ── INFO command ──────────────────────────────────────────────────────────────

fn build_info_response(config: &Config, repl: &ReplicationState, args: &[String]) -> Bytes {
    let section = args.get(1).map(|s| s.to_lowercase());
    let show_replication = section.as_deref().map_or(true, |s| s == "replication" || s == "all");

    if !show_replication {
        return resp::bulk_string("");
    }

    let info = if repl.role == "master" {
        format!(
            "# Replication\r\nrole:master\r\nconnected_slaves:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\nsecond_repl_offset:-1\r\n",
            repl.replica_count(),
            repl.replid,
            repl.current_offset(),
        )
    } else {
        let host = config.replicaof.as_ref().map(|(h, _)| h.as_str()).unwrap_or("?");
        let port = config.replicaof.as_ref().map(|(_, p)| *p).unwrap_or(0);
        format!(
            "# Replication\r\nrole:slave\r\nmaster_host:{}\r\nmaster_port:{}\r\nmaster_link_status:up\r\nmaster_last_io_seconds_ago:0\r\nmaster_sync_in_progress:0\r\nslave_repl_offset:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
            host,
            port,
            repl.current_offset(),
            repl.replid,
            repl.current_offset(),
        )
    };

    resp::bulk_string(&info)
}

// ── WAIT command ──────────────────────────────────────────────────────────────

async fn handle_wait(args: &[String], repl: &ReplicationState) -> Bytes {
    let num_needed: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let timeout_ms: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    let replica_count = repl.replica_count();
    if replica_count == 0 {
        return resp::integer(0);
    }

    let min_offset = repl.current_offset();
    if min_offset == 0 {
        return resp::integer(replica_count as i64);
    }

    repl.send_getack_to_all();

    if timeout_ms == 0 {
        return resp::integer(repl.acked_count(min_offset) as i64);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let count = repl.acked_count(min_offset);
        if count >= num_needed || tokio::time::Instant::now() >= deadline {
            return resp::integer(count as i64);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// ── Blocking commands ─────────────────────────────────────────────────────────

async fn handle_blpop(
    args: &[String],
    ctx: &Ctx,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> anyhow::Result<()> {
    if args.len() < 3 {
        writer.write_all(&resp::error("wrong number of arguments for 'blpop' command")).await?;
        return Ok(());
    }

    let key = &args[1];
    let timeout_secs: f64 = args.last().and_then(|s| s.parse().ok()).unwrap_or(0.0);

    match ctx.store.blpop_or_wait(key) {
        Ok(value) => {
            let response = resp::array(vec![resp::bulk_string(key), resp::bulk_string(&value)]);
            writer.write_all(&response).await?;
        }
        Err(receiver) => {
            let result = if timeout_secs == 0.0 {
                receiver.await.ok()
            } else {
                let duration = Duration::from_secs_f64(timeout_secs);
                tokio::time::timeout(duration, receiver).await.ok().and_then(|r| r.ok())
            };
            match result {
                Some(value) => {
                    let response = resp::array(vec![resp::bulk_string(key), resp::bulk_string(&value)]);
                    writer.write_all(&response).await?;
                }
                None => writer.write_all(&resp::null_array()).await?,
            }
        }
    }

    Ok(())
}

async fn handle_xread_block(
    args: &[String],
    ctx: &Ctx,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> anyhow::Result<()> {
    let Some(parsed) = parse_xread_block_args(args) else {
        writer.write_all(&resp::error("syntax error")).await?;
        return Ok(());
    };

    let block_ms = parsed.block_ms.unwrap_or(0);

    let resolved_ids: Vec<String> = parsed.keys.iter().zip(parsed.ids.iter())
        .map(|(key, id)| {
            if id == "$" {
                let (ms, seq) = ctx.store.stream_last_id(key);
                format!("{}-{}", ms, seq)
            } else {
                id.clone()
            }
        })
        .collect();

    let immediate = ctx.store.xread(&parsed.keys, &resolved_ids);
    if !immediate.is_empty() {
        writer.write_all(&encode_xread_results(immediate, parsed.count)).await?;
        return Ok(());
    }

    let (sender, mut receiver) = mpsc::unbounded_channel::<StreamNotification>();

    for (key, id_str) in parsed.keys.iter().zip(resolved_ids.iter()) {
        let after_id = parse_stream_id_pair(id_str);
        if let Some(entries) = ctx.store.xread_blocking(key, after_id, sender.clone()) {
            let results = vec![(key.clone(), entries)];
            writer.write_all(&encode_xread_results(results, parsed.count)).await?;
            return Ok(());
        }
    }

    let notification = if block_ms == 0 {
        receiver.recv().await
    } else {
        let duration = Duration::from_millis(block_ms);
        tokio::time::timeout(duration, receiver.recv()).await.ok().flatten()
    };

    match notification {
        Some((key, entry_id, fields)) => {
            let results = vec![(key, vec![(entry_id, fields)])];
            writer.write_all(&encode_xread_results(results, parsed.count)).await?;
        }
        None => {
            writer.write_all(&resp::null_array()).await?;
        }
    }

    Ok(())
}

struct XReadBlockArgs {
    count: Option<usize>,
    block_ms: Option<u64>,
    keys: Vec<String>,
    ids: Vec<String>,
}

fn parse_xread_block_args(args: &[String]) -> Option<XReadBlockArgs> {
    let mut count = None;
    let mut block_ms = None;
    let mut i = 1;

    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "COUNT" => {
                count = args.get(i + 1)?.parse().ok();
                i += 2;
            }
            "BLOCK" => {
                block_ms = Some(args.get(i + 1)?.parse::<u64>().ok()?);
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return None,
        }
    }

    let remaining = &args[i..];
    if remaining.len() < 2 || remaining.len() % 2 != 0 {
        return None;
    }
    let mid = remaining.len() / 2;

    Some(XReadBlockArgs {
        count,
        block_ms,
        keys: remaining[..mid].to_vec(),
        ids: remaining[mid..].to_vec(),
    })
}

fn parse_stream_id_pair(id: &str) -> (u64, u64) {
    id.split_once('-')
        .and_then(|(ms, seq)| Some((ms.parse().ok()?, seq.parse().ok()?)))
        .unwrap_or((0, 0))
}

// ── Stream encoding helpers ───────────────────────────────────────────────────

pub fn encode_xread_results(results: Vec<(String, Vec<StreamEntry>)>, count: Option<usize>) -> Bytes {
    let streams: Vec<Bytes> = results.into_iter().map(|(key, entries)| {
        let limit = count.unwrap_or(usize::MAX);
        let entries_encoded = encode_stream_entries(entries.into_iter().take(limit).collect());
        resp::array(vec![resp::bulk_string(&key), entries_encoded])
    }).collect();
    resp::array(streams)
}

fn encode_stream_entries(entries: Vec<StreamEntry>) -> Bytes {
    let encoded: Vec<Bytes> = entries.into_iter().map(|(id, fields)| {
        let fields_arr: Vec<Bytes> = fields.into_iter()
            .flat_map(|(k, v)| [resp::bulk_string(&k), resp::bulk_string(&v)])
            .collect();
        resp::array(vec![resp::bulk_string(&id), resp::array(fields_arr)])
    }).collect();
    resp::array(encoded)
}

// ── XREAD non-blocking args parser ───────────────────────────────────────────

fn parse_xread_args(args: &[String]) -> Option<(Option<usize>, Vec<String>, Vec<String>)> {
    let mut count = None;
    let mut i = 1;

    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "COUNT" => {
                count = args.get(i + 1)?.parse().ok();
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return None,
        }
    }

    let remaining = &args[i..];
    if remaining.len() < 2 || remaining.len() % 2 != 0 {
        return None;
    }
    let mid = remaining.len() / 2;
    Some((count, remaining[..mid].to_vec(), remaining[mid..].to_vec()))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn encode_command(args: &[String]) -> Bytes {
    resp::array(args.iter().map(|s| resp::bulk_string(s)).collect())
}

fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET" | "DEL" | "INCR" | "DECR" | "MSET"
            | "RPUSH" | "LPUSH" | "LPOP" | "RPOP" | "LREM"
            | "ZADD" | "ZREM" | "GEOADD" | "XADD"
    )
}

fn is_pubsub_command(cmd: &str) -> bool {
    matches!(cmd, "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT" | "RESET")
}
