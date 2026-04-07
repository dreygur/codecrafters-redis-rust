use anyhow::Result;
use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    command::{dispatch, subscribe_response},
    resp,
    session::Session,
    store::StoreService,
};

const BUF_SIZE: usize = 512;

pub async fn handle(mut stream: TcpStream, store: StoreService) -> Result<()> {
    let mut buf = [0u8; BUF_SIZE];
    let mut session = Session::new();

    loop {
        match stream.read(&mut buf).await? {
            0 => {
                println!("Client disconnected");
                break;
            }
            n => {
                let Some(args) = resp::parse(&buf[..n]) else {
                    continue;
                };

                let cmd = args[0].to_uppercase();
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
                    stream.write_all(&resp::error(&msg)).await?;
                    continue;
                }

                let response = match cmd.as_str() {
                    "SUBSCRIBE" => args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
                        out.extend_from_slice(&subscribe_response(channel, session.subscribe(channel)));
                        out
                    }).freeze(),
                    "MULTI" => {
                        if session.begin_tx() {
                            Bytes::from_static(b"+OK\r\n")
                        } else {
                            resp::error("MULTI calls can not be nested")
                        }
                    }
                    "EXEC" => {
                        if !session.is_tx_active() {
                            resp::error("EXEC without MULTI")
                        } else {
                            let queue = session.execute_tx();
                            let results = queue.iter().map(|cmd| dispatch(cmd, &store)).collect();
                            resp::array(results)
                        }
                    }
                    "DISCARD" => {
                        if session.discard_tx() {
                            Bytes::from_static(b"+OK\r\n")
                        } else {
                            resp::error("DISCARD without MULTI")
                        }
                    }
                    _ => {
                        if session.is_tx_active() {
                            session.enqueue(args);
                            Bytes::from_static(b"+QUEUED\r\n")
                        } else {
                            dispatch(&args, &store)
                        }
                    }
                };

                stream.write_all(&response).await?;
            }
        }
    }

    Ok(())
}
