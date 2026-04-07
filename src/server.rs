use std::{
    io::{Read, Result, Write},
    net::TcpStream,
};

use crate::{command::dispatch, resp, store::Store, transaction::TxState};

const BUF_SIZE: usize = 512;

pub fn handle(mut stream: TcpStream, store: Store) -> Result<()> {
    let mut buf = [0; BUF_SIZE];
    let mut tx = TxState::new();

    loop {
        match stream.read(&mut buf)? {
            0 => {
                println!("Client disconnected");
                break;
            }
            n => {
                let Some(args) = resp::parse(&buf[..n]) else {
                    continue;
                };

                let response = match args[0].to_uppercase().as_str() {
                    "MULTI" => {
                        if tx.is_active() {
                            resp::error("MULTI calls can not be nested")
                        } else {
                            tx.begin();
                            b"+OK\r\n".to_vec()
                        }
                    }
                    "EXEC" => {
                        if !tx.is_active() {
                            resp::error("EXEC without MULTI")
                        } else {
                            let queue = tx.take_queue();
                            let results: Vec<Vec<u8>> =
                                queue.iter().map(|cmd| dispatch(cmd, &store)).collect();
                            resp::array(results)
                        }
                    }
                    "DISCARD" => {
                        if !tx.is_active() {
                            resp::error("DISCARD without MULTI")
                        } else {
                            tx.reset();
                            b"+OK\r\n".to_vec()
                        }
                    }
                    _ => {
                        if tx.is_active() {
                            tx.enqueue(args);
                            b"+QUEUED\r\n".to_vec()
                        } else {
                            dispatch(&args, &store)
                        }
                    }
                };

                stream.write_all(&response)?;
            }
        }
    }

    Ok(())
}
