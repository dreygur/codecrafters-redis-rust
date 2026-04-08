use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::application::ports::StorePort;
use crate::infrastructure::replication::ReplicationState;

pub async fn run(
    master_host: String,
    master_port: u16,
    my_port: u16,
    store: Arc<dyn StorePort>,
    repl: Arc<ReplicationState>,
) {
    let addr = format!("{}:{}", master_host, master_port);
    let stream = match tokio::net::TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => { eprintln!("replica: connect failed: {e}"); return; }
    };
    if let Err(e) = handshake_and_receive(stream, my_port, store, repl).await {
        eprintln!("replica: {e}");
    }
}

async fn handshake_and_receive(
    stream: tokio::net::TcpStream,
    my_port: u16,
    store: Arc<dyn StorePort>,
    _repl: Arc<ReplicationState>,
) -> anyhow::Result<()> {
    let (mut reader, mut writer) = stream.into_split();

    writer.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    read_simple_response(&mut reader).await?;

    let port_str = my_port.to_string();
    let msg = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port_str.len(), port_str
    );
    writer.write_all(msg.as_bytes()).await?;
    read_simple_response(&mut reader).await?;

    writer.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await?;
    read_simple_response(&mut reader).await?;

    writer.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
    read_simple_response(&mut reader).await?;

    skip_rdb(&mut reader).await?;

    let mut carry: Vec<u8> = vec![];
    let mut buf = [0u8; 4096];
    let mut offset: u64 = 0;

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 { break; }

        carry.extend_from_slice(&buf[..n]);

        loop {
            match try_parse_command(&carry) {
                Some((args, consumed)) => {
                    carry.drain(..consumed);

                    if args[0].eq_ignore_ascii_case("REPLCONF")
                        && args.get(1).map_or(false, |s| s.eq_ignore_ascii_case("GETACK"))
                    {
                        let ack = format!(
                            "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                            offset.to_string().len(), offset
                        );
                        offset += consumed as u64;
                        writer.write_all(ack.as_bytes()).await?;
                    } else {
                        offset += consumed as u64;
                        apply_command(&args, &store);
                    }
                }
                None => break,
            }
        }
    }

    Ok(())
}

async fn read_simple_response(reader: &mut tokio::net::tcp::OwnedReadHalf) -> anyhow::Result<()> {
    let mut byte = [0u8; 1];
    loop {
        reader.read_exact(&mut byte).await?;
        if byte[0] == b'\n' { return Ok(()); }
    }
}

async fn skip_rdb(reader: &mut tokio::net::tcp::OwnedReadHalf) -> anyhow::Result<()> {
    // Read "$<len>\r\n"
    let mut header: Vec<u8> = vec![];
    let mut byte = [0u8; 1];
    loop {
        reader.read_exact(&mut byte).await?;
        header.push(byte[0]);
        if header.ends_with(b"\r\n") { break; }
    }
    let len: usize = String::from_utf8_lossy(&header)
        .trim()
        .strip_prefix('$')
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Read and discard RDB bytes
    let mut remaining = len;
    let mut buf = [0u8; 512];
    while remaining > 0 {
        let to_read = remaining.min(buf.len());
        let n = reader.read(&mut buf[..to_read]).await?;
        if n == 0 { break; }
        remaining -= n;
    }
    Ok(())
}

fn try_parse_command(data: &[u8]) -> Option<(Vec<String>, usize)> {
    if data.is_empty() || data[0] != b'*' { return None; }

    let cr = data.iter().position(|&b| b == b'\r')?;
    let count: usize = std::str::from_utf8(&data[1..cr]).ok()?.parse().ok()?;

    let mut pos = cr + 2;
    let mut args = Vec::with_capacity(count);

    for _ in 0..count {
        if pos >= data.len() || data[pos] != b'$' { return None; }
        let cr2 = data[pos..].iter().position(|&b| b == b'\r')?;
        let len: usize = std::str::from_utf8(&data[pos + 1..pos + cr2]).ok()?.parse().ok()?;
        pos += cr2 + 2;
        if pos + len + 2 > data.len() { return None; }
        args.push(String::from_utf8_lossy(&data[pos..pos + len]).to_string());
        pos += len + 2;
    }

    Some((args, pos))
}

fn apply_command(args: &[String], store: &Arc<dyn StorePort>) {
    if args.is_empty() { return; }
    match args[0].to_uppercase().as_str() {
        "SET" if args.len() >= 3 => {
            let ttl = parse_set_ttl(args);
            store.set(args[1].clone(), args[2].clone(), ttl);
        }
        "DEL" if args.len() >= 2 => {
            // no-op: store doesn't have del yet
        }
        _ => {}
    }
}

fn parse_set_ttl(args: &[String]) -> Option<u64> {
    let mut i = 3;
    while i + 1 < args.len() {
        match args[i].to_uppercase().as_str() {
            "PX" => return args[i + 1].parse().ok(),
            "EX" => return args[i + 1].parse::<u64>().ok().map(|s| s * 1000),
            _ => {}
        }
        i += 1;
    }
    None
}
