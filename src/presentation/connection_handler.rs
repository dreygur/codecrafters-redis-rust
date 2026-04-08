use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::application::ports::{AclPort, PubSubPort, StorePort};
use crate::domain::entities::Session;
use crate::infrastructure::config::Config;
use crate::infrastructure::networking::resp::RespEncoder;
use crate::infrastructure::replication::ReplicationState;
use crate::presentation::command::router::CommandRouter;
use super::blocking_commands::{handle_blpop, handle_xread_block};
use super::command_handlers::{
    handle_auth, handle_exec, handle_subscribe, handle_unsubscribe, handle_watch,
};
use super::replica_server;

const BUF_SIZE: usize = 512;

pub async fn handle_connection(
    stream: tokio::net::TcpStream,
    store: Arc<dyn StorePort>,
    pubsub: Arc<dyn PubSubPort>,
    acl: Arc<dyn AclPort>,
    config: Arc<Config>,
    repl: Arc<ReplicationState>,
) -> anyhow::Result<()> {
    let is_nopass = acl.is_nopass();
    let router = CommandRouter::new(Arc::clone(&store), acl, pubsub, Arc::clone(&config));
    let mut session = Session::new(is_nopass);
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    let (mut reader, mut writer) = stream.into_split();
    let mut buf = [0u8; BUF_SIZE];

    loop {
        tokio::select! {
            result = reader.read(&mut buf) => {
                let n = match result? {
                    0 => { println!("Client disconnected"); break; }
                    n => n,
                };

                let Some(args) = RespEncoder::parse(&buf[..n]) else { continue; };
                let cmd = args[0].to_uppercase();

                let is_repl_cmd = matches!(cmd.as_str(), "REPLCONF" | "PSYNC");
                if !session.is_authenticated() && cmd != "AUTH" && !is_repl_cmd {
                    writer.write_all(&RespEncoder::raw_error("NOAUTH Authentication required.")).await?;
                    continue;
                }

                if cmd == "PSYNC" {
                    drop(rx);
                    replica_server::run(reader, writer, repl).await?;
                    return Ok(());
                }

                if cmd == "REPLCONF" {
                    writer.write_all(&RespEncoder::simple_string("OK")).await?;
                    continue;
                }

                if cmd == "INFO" {
                    writer.write_all(&handle_info(&config, &repl, &args)).await?;
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
                    writer.write_all(&RespEncoder::error(&msg)).await?;
                    continue;
                }

                if session.is_tx_active() && !matches!(cmd.as_str(), "MULTI" | "EXEC" | "DISCARD" | "WATCH") {
                    session.enqueue(args.to_vec());
                    writer.write_all(&RespEncoder::simple_string("QUEUED")).await?;
                    continue;
                }

                if cmd == "BLPOP" {
                    handle_blpop(&args, &router, &mut writer).await?;
                    continue;
                }

                if cmd == "XREAD" && args.iter().any(|a| a.eq_ignore_ascii_case("BLOCK")) {
                    handle_xread_block(&args, &router, &mut writer).await?;
                    continue;
                }

                let response = handle_command(&cmd, &args, &mut session, &router, &tx);
                writer.write_all(&response).await?;

                if repl.role == "master" && is_write_command(&cmd) {
                    repl.propagate(encode_command(&args));
                }
            }
            Some(msg) = rx.recv() => {
                writer.write_all(&msg).await?;
            }
        }
    }

    Ok(())
}

fn handle_info(config: &Config, repl: &ReplicationState, args: &[String]) -> Bytes {
    let section = args.get(1).map(|s| s.to_lowercase());
    let show_repl = section.as_deref().map_or(true, |s| s == "replication" || s == "all");

    if !show_repl {
        return RespEncoder::bulk_string("");
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
            host, port, repl.current_offset(), repl.replid, repl.current_offset(),
        )
    };

    RespEncoder::bulk_string(&info)
}

async fn handle_wait(args: &[String], repl: &ReplicationState) -> Bytes {
    let num_needed: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let timeout_ms: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    let replica_count = repl.replica_count();
    if replica_count == 0 {
        return RespEncoder::integer(0);
    }

    let min_offset = repl.current_offset();
    if min_offset == 0 {
        return RespEncoder::integer(replica_count as i64);
    }

    repl.send_getack_to_all();

    if timeout_ms == 0 {
        return RespEncoder::integer(repl.acked_count(min_offset) as i64);
    }

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        let count = repl.acked_count(min_offset);
        if count >= num_needed || tokio::time::Instant::now() >= deadline {
            return RespEncoder::integer(count as i64);
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

fn encode_command(args: &[String]) -> Bytes {
    RespEncoder::array(args.iter().map(|s| RespEncoder::bulk_string(s)).collect())
}

fn is_write_command(cmd: &str) -> bool {
    matches!(cmd,
        "SET" | "DEL" | "INCR" | "DECR" | "MSET" |
        "RPUSH" | "LPUSH" | "LPOP" | "RPOP" | "LREM" |
        "ZADD" | "ZREM" | "GEOADD" | "XADD"
    )
}

fn is_pubsub_command(cmd: &str) -> bool {
    matches!(cmd, "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT" | "RESET")
}

fn handle_command(
    cmd: &str,
    args: &[String],
    session: &mut Session,
    router: &CommandRouter,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> Bytes {
    match cmd {
        "AUTH" => handle_auth(args, session, router),

        "ACL" if args.get(1).map(|s| s.to_uppercase()).as_deref() == Some("WHOAMI") => {
            RespEncoder::bulk_string(session.current_user())
        }

        "WATCH" => handle_watch(args, session, router),

        "UNWATCH" => {
            session.unwatch();
            RespEncoder::simple_string("OK")
        }

        "MULTI" => {
            if session.begin_tx() {
                RespEncoder::simple_string("OK")
            } else {
                RespEncoder::error("MULTI calls can not be nested")
            }
        }

        "EXEC" => handle_exec(session, router),

        "DISCARD" => {
            if session.discard_tx() {
                session.unwatch();
                RespEncoder::simple_string("OK")
            } else {
                RespEncoder::error("DISCARD without MULTI")
            }
        }

        "SUBSCRIBE" => handle_subscribe(args, session, router, tx),
        "UNSUBSCRIBE" => handle_unsubscribe(args, session, router, tx),

        "PING" if session.is_subscribed() => RespEncoder::array(vec![
            RespEncoder::bulk_string("pong"),
            RespEncoder::bulk_string(""),
        ]),

        _ => router.dispatch(args),
    }
}
