use anyhow::Result;
use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};

use crate::{
    acl::AclService,
    command::{dispatch, subscribe_response, unsubscribe_response},
    error::RedisError,
    pubsub::PubSubService,
    resp,
    session::Session,
    store::StoreService,
};

const BUF_SIZE: usize = 512;

pub async fn handle(
    stream: TcpStream,
    store: StoreService,
    pubsub: PubSubService,
    acl: AclService,
) -> Result<()> {
    let mut buf = [0u8; BUF_SIZE];
    let mut session = Session::new(acl.is_nopass());
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
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

                let Some(args) = resp::parse(&buf[..n]) else {
                    continue;
                };

                let cmd = args[0].to_uppercase();

                // --- authentication guard ---
                if !session.is_authenticated() && cmd != "AUTH" {
                    writer
                        .write_all(&resp::raw_error("NOAUTH Authentication required."))
                        .await?;
                    continue;
                }

                // --- subscribed mode guard ---
                if session.is_subscribed()
                    && !matches!(
                        cmd.as_str(),
                        "SUBSCRIBE"
                            | "UNSUBSCRIBE"
                            | "PSUBSCRIBE"
                            | "PUNSUBSCRIBE"
                            | "PING"
                            | "QUIT"
                            | "RESET"
                    )
                {
                    let msg = format!(
                        "Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                        args[0].to_lowercase()
                    );
                    writer.write_all(&resp::error(&msg)).await?;
                    continue;
                }

                let response = match cmd.as_str() {
                    // -- auth --
                    "AUTH" => {
                        // AUTH password  |  AUTH username password
                        let (username, password) = match args.len() {
                            2 => ("default", args[1].as_str()),
                            3 => (args[1].as_str(), args[2].as_str()),
                            _ => {
                                writer
                                    .write_all(&resp::error(
                                        &RedisError::WrongArgCount.to_string(),
                                    ))
                                    .await?;
                                continue;
                            }
                        };
                        if acl.authenticate(username, password) {
                            session.authenticate(username.to_string());
                            resp::simple_string("OK")
                        } else {
                            resp::raw_error(
                                "WRONGPASS invalid username-password pair or user is disabled.",
                            )
                        }
                    }

                    // -- ACL subcommands --
                    "ACL" => match args.get(1).map(|s| s.to_uppercase()).as_deref() {
                        Some("WHOAMI") => resp::bulk_string(session.current_user()),
                        Some("GETUSER") => {
                            let Some(username) = args.get(2).map(String::as_str) else {
                                writer
                                    .write_all(&resp::error(
                                        &RedisError::WrongArgCount.to_string(),
                                    ))
                                    .await?;
                                continue;
                            };
                            match (acl.user_flags(username), acl.user_passwords(username)) {
                                (Some(flags), Some(passwords)) => resp::array(vec![
                                    resp::bulk_string("flags"),
                                    resp::array(
                                        flags.iter().map(|f| resp::bulk_string(f)).collect(),
                                    ),
                                    resp::bulk_string("passwords"),
                                    resp::array(
                                        passwords.iter().map(|p| resp::bulk_string(p)).collect(),
                                    ),
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
                        _ => resp::error("unknown ACL subcommand"),
                    },

                    // -- watch --
                    "WATCH" => {
                        if session.is_tx_active() {
                            resp::error("WATCH inside MULTI is not allowed")
                        } else if args.len() < 2 {
                            resp::error(&RedisError::WrongArgCount.to_string())
                        } else {
                            for key in args.iter().skip(1) {
                                session.watch(key, store.key_version(key));
                            }
                            resp::simple_string("OK")
                        }
                    }
                    "UNWATCH" => {
                        session.unwatch();
                        resp::simple_string("OK")
                    }

                    // -- pub/sub --
                    "SUBSCRIBE" => {
                        args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
                            if !session.is_subscribed_to(channel) {
                                pubsub.subscribe(channel, tx.clone());
                            }
                            out.extend_from_slice(&subscribe_response(
                                channel,
                                session.subscribe(channel),
                            ));
                            out
                        }).freeze()
                    }
                    "UNSUBSCRIBE" => {
                        args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
                            pubsub.unsubscribe(channel, &tx);
                            out.extend_from_slice(&unsubscribe_response(
                                channel,
                                session.unsubscribe(channel),
                            ));
                            out
                        }).freeze()
                    }
                    "PUBLISH" => {
                        let channel = args.get(1).map(String::as_str).unwrap_or("");
                        let message = args.get(2).map(String::as_str).unwrap_or("");
                        resp::integer(pubsub.publish(channel, message))
                    }

                    // -- transactions --
                    "MULTI" => {
                        if session.begin_tx() {
                            resp::simple_string("OK")
                        } else {
                            resp::error(&RedisError::NestedMulti.to_string())
                        }
                    }
                    "EXEC" => {
                        if !session.is_tx_active() {
                            resp::error(&RedisError::ExecWithoutMulti.to_string())
                        } else {
                            let dirty = session
                                .watched_versions()
                                .iter()
                                .any(|(key, &ver)| store.key_version(key) != ver);
                            let queue = session.execute_tx();
                            session.unwatch();
                            if dirty {
                                resp::null_array()
                            } else {
                                let results =
                                    queue.iter().map(|cmd| dispatch(cmd, &store)).collect();
                                resp::array(results)
                            }
                        }
                    }
                    "DISCARD" => {
                        if session.discard_tx() {
                            session.unwatch();
                            resp::simple_string("OK")
                        } else {
                            resp::error(&RedisError::DiscardWithoutMulti.to_string())
                        }
                    }

                    // -- PING in subscribed mode --
                    "PING" if session.is_subscribed() => resp::array(vec![
                        resp::bulk_string("pong"),
                        resp::bulk_string(""),
                    ]),

                    // -- everything else --
                    _ => {
                        if session.is_tx_active() {
                            session.enqueue(args);
                            resp::simple_string("QUEUED")
                        } else {
                            dispatch(&args, &store)
                        }
                    }
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
