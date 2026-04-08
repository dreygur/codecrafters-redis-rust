use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::application::ports::{AclPort, PubSubPort, StorePort};
use crate::domain::entities::Session;
use crate::infrastructure::networking::resp::RespEncoder;
use crate::presentation::command::router::CommandRouter;
use super::session_commands::{
    handle_auth, handle_exec, handle_subscribe, handle_unsubscribe, handle_watch,
};

const BUF_SIZE: usize = 512;

pub async fn handle_connection(
    stream: tokio::net::TcpStream,
    store: Arc<dyn StorePort>,
    pubsub: Arc<dyn PubSubPort>,
    acl: Arc<dyn AclPort>,
) -> anyhow::Result<()> {
    let is_nopass = acl.is_nopass();
    let router = CommandRouter::new(store, acl, pubsub);
    let mut session = Session::new(is_nopass);
    let (tx, mut rx) = mpsc::unbounded_channel::<bytes::Bytes>();
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

                if !session.is_authenticated() && cmd != "AUTH" {
                    writer.write_all(&RespEncoder::raw_error("NOAUTH Authentication required.")).await?;
                    continue;
                }

                if session.is_subscribed() && !is_pubsub_allowed(&cmd) {
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

                let response = dispatch_cmd(&cmd, &args, &mut session, &router, &tx);
                writer.write_all(&response).await?;
            }
            Some(msg) = rx.recv() => {
                writer.write_all(&msg).await?;
            }
        }
    }

    Ok(())
}

fn is_pubsub_allowed(cmd: &str) -> bool {
    matches!(cmd, "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT" | "RESET")
}

async fn handle_blpop(
    args: &[String],
    router: &CommandRouter,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> anyhow::Result<()> {
    if args.len() < 3 {
        writer.write_all(&RespEncoder::error("wrong number of arguments for 'blpop' command")).await?;
        return Ok(());
    }
    let key = &args[1];
    let timeout_secs: f64 = args.last().and_then(|s| s.parse().ok()).unwrap_or(0.0);
    match router.blpop_or_wait(key) {
        Ok(val) => {
            writer.write_all(&blpop_resp(key, &val)).await?;
        }
        Err(blpop_rx) => {
            let val_opt = if timeout_secs == 0.0 {
                blpop_rx.await.ok()
            } else {
                let dur = std::time::Duration::from_secs_f64(timeout_secs);
                tokio::time::timeout(dur, blpop_rx).await.ok().and_then(|r| r.ok())
            };
            match val_opt {
                Some(val) => writer.write_all(&blpop_resp(key, &val)).await?,
                None => writer.write_all(&RespEncoder::null_array()).await?,
            }
        }
    }
    Ok(())
}

fn blpop_resp(key: &str, val: &str) -> bytes::Bytes {
    RespEncoder::array(vec![RespEncoder::bulk_string(key), RespEncoder::bulk_string(val)])
}

fn dispatch_cmd(
    cmd: &str,
    args: &[String],
    session: &mut Session,
    router: &CommandRouter,
    tx: &mpsc::UnboundedSender<bytes::Bytes>,
) -> bytes::Bytes {
    match cmd {
        "AUTH" => handle_auth(args, session, router),
        "ACL" if args.get(1).map(|s| s.to_uppercase()).as_deref() == Some("WHOAMI") => {
            RespEncoder::bulk_string(session.current_user())
        }
        "WATCH" => handle_watch(args, session, router),
        "UNWATCH" => { session.unwatch(); RespEncoder::simple_string("OK") }
        "MULTI" => {
            if session.begin_tx() { RespEncoder::simple_string("OK") }
            else { RespEncoder::error("MULTI calls can not be nested") }
        }
        "EXEC" => handle_exec(session, router),
        "DISCARD" => {
            if session.discard_tx() { session.unwatch(); RespEncoder::simple_string("OK") }
            else { RespEncoder::error("DISCARD without MULTI") }
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
