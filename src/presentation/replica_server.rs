use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::infrastructure::networking::resp::RespEncoder;
use crate::infrastructure::replication::ReplicationState;

const EMPTY_RDB: &[u8] = &[
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
    0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72,
    0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30,
    0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73,
    0xc0, 0x40,
    0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65,
    0xc2, 0x6d, 0x08, 0xbc, 0x65,
    0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d,
    0xc2, 0xb0, 0xc4, 0x10, 0x00,
    0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65,
    0xc0, 0x00,
    0xff,
    0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
];

pub async fn run(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    repl: Arc<ReplicationState>,
) -> anyhow::Result<()> {
    let fullresync = format!("+FULLRESYNC {} {}\r\n", repl.replid, repl.current_offset());
    writer.write_all(fullresync.as_bytes()).await?;

    let rdb_header = format!("${}\r\n", EMPTY_RDB.len());
    writer.write_all(rdb_header.as_bytes()).await?;
    writer.write_all(EMPTY_RDB).await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    let acked_offset = repl.add_replica(tx);

    let mut buf = [0u8; 512];

    loop {
        tokio::select! {
            Some(data) = rx.recv() => {
                writer.write_all(&data).await?;
            }
            result = reader.read(&mut buf) => {
                match result? {
                    0 => break,
                    n => {
                        if let Some(args) = RespEncoder::parse(&buf[..n]) {
                            if args.len() >= 3
                                && args[0].eq_ignore_ascii_case("REPLCONF")
                                && args[1].eq_ignore_ascii_case("ACK")
                            {
                                if let Ok(offset) = args[2].parse::<u64>() {
                                    acked_offset.store(offset, Ordering::SeqCst);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
