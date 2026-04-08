mod application;
mod domain;
mod infrastructure;
mod presentation;

use std::sync::Arc;

use application::ports::{AclPort, PubSubPort, StorePort};
use domain::entities::Session;
use infrastructure::networking::resp::RespEncoder;
use infrastructure::persistence::in_memory_store::InMemoryStore;
use infrastructure::services::acl_service::AclService;
use infrastructure::services::pubsub_service::PubSubService;
use presentation::command::router::CommandRouter;

const ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let requirepass = args
        .windows(2)
        .find(|w| w[0] == "--requirepass")
        .map(|w| w[1].clone());

    let store: Arc<dyn StorePort> = Arc::new(InMemoryStore::new());
    let pubsub: Arc<dyn PubSubPort> = Arc::new(PubSubService::new());
    let acl: Arc<dyn AclPort> = Arc::new(AclService::new());

    if let Some(password) = requirepass {
        acl.set_default_password(password);
    }

    let listener = tokio::net::TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {}", ADDR);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let store = Arc::clone(&store);
                let pubsub = Arc::clone(&pubsub);
                let acl = Arc::clone(&acl);
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
    store: Arc<dyn StorePort>,
    pubsub: Arc<dyn PubSubPort>,
    acl: Arc<dyn AclPort>,
) -> anyhow::Result<()> {
    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    const BUF_SIZE: usize = 512;

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

                if session.is_subscribed()
                    && !matches!(cmd.as_str(), "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT" | "RESET")
                {
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
                    if args.len() < 3 {
                        writer.write_all(&RespEncoder::error("wrong number of arguments for 'blpop' command")).await?;
                        continue;
                    }
                    // args: BLPOP key [key ...] timeout  — support single key for now
                    let key = args[1].clone();
                    match router.blpop_or_wait(&key) {
                        Ok(val) => {
                            let response = RespEncoder::array(vec![
                                RespEncoder::bulk_string(&key),
                                RespEncoder::bulk_string(&val),
                            ]);
                            writer.write_all(&response).await?;
                        }
                        Err(blpop_rx) => {
                            tokio::select! {
                                result = blpop_rx => {
                                    match result {
                                        Ok(val) => {
                                            let response = RespEncoder::array(vec![
                                                RespEncoder::bulk_string(&key),
                                                RespEncoder::bulk_string(&val),
                                            ]);
                                            writer.write_all(&response).await?;
                                        }
                                        Err(_) => {}
                                    }
                                }
                                Some(msg) = rx.recv() => {
                                    writer.write_all(&msg).await?;
                                }
                            }
                        }
                    }
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
                        if router.authenticate(username, password) {
                            session.authenticate(username.to_string());
                            RespEncoder::simple_string("OK")
                        } else {
                            RespEncoder::raw_error("WRONGPASS invalid username-password pair or user is disabled.")
                        }
                    }

                    "ACL" if args.get(1).map(|s| s.to_uppercase()).as_deref() == Some("WHOAMI") => {
                        RespEncoder::bulk_string(session.current_user())
                    }

                    "WATCH" => {
                        if session.is_tx_active() {
                            RespEncoder::error("WATCH inside MULTI is not allowed")
                        } else if args.len() < 2 {
                            RespEncoder::error("wrong number of arguments for 'watch' command")
                        } else {
                            for key in args.iter().skip(1) {
                                session.watch(key, router.key_version(key));
                            }
                            RespEncoder::simple_string("OK")
                        }
                    }

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

                    "EXEC" => {
                        if !session.is_tx_active() {
                            RespEncoder::error("EXEC without MULTI")
                        } else {
                            let watched: Vec<(String, u64)> = session.watched_versions()
                                .iter()
                                .map(|(k, v)| (k.clone(), *v))
                                .collect();
                            let queue = session.execute_tx();
                            session.unwatch();
                            router.exec_transaction(&watched, queue)
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

                    "SUBSCRIBE" => {
                        args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
                            if !session.is_subscribed_to(channel) {
                                router.subscribe(channel, tx.clone());
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
                            router.unsubscribe(channel, &tx);
                            let count = session.unsubscribe(channel);
                            out.extend_from_slice(&RespEncoder::array(vec![
                                RespEncoder::bulk_string("unsubscribe"),
                                RespEncoder::bulk_string(channel),
                                RespEncoder::integer(count),
                            ]));
                            out
                        }).freeze()
                    }

                    "PING" if session.is_subscribed() => RespEncoder::array(vec![
                        RespEncoder::bulk_string("pong"),
                        RespEncoder::bulk_string(""),
                    ]),

                    _ => router.dispatch(&args),
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
