use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::store::Store;

// ── Replication state (used by both master and slave) ─────────────────────────

pub struct ReplicaHandle {
    pub sender: mpsc::UnboundedSender<Bytes>,
    pub acked_offset: Arc<AtomicU64>,
}

pub struct ReplicationState {
    pub role: &'static str,
    pub replid: String,
    pub offset: AtomicU64,
    pub replicas: Mutex<Vec<ReplicaHandle>>,
}

impl ReplicationState {
    pub fn new_master() -> Self {
        Self {
            role: "master",
            replid: generate_replid(),
            offset: AtomicU64::new(0),
            replicas: Mutex::new(vec![]),
        }
    }

    pub fn new_slave() -> Self {
        Self {
            role: "slave",
            replid: "?".to_string(),
            offset: AtomicU64::new(0),
            replicas: Mutex::new(vec![]),
        }
    }

    pub fn add_replica(&self, sender: mpsc::UnboundedSender<Bytes>) -> Arc<AtomicU64> {
        let acked = Arc::new(AtomicU64::new(0));
        self.replicas.lock().unwrap().push(ReplicaHandle {
            sender,
            acked_offset: Arc::clone(&acked),
        });
        acked
    }

    pub fn propagate(&self, data: Bytes) {
        let len = data.len() as u64;
        let mut replicas = self.replicas.lock().unwrap();
        replicas.retain(|r| r.sender.send(data.clone()).is_ok());
        drop(replicas);
        self.offset.fetch_add(len, Ordering::SeqCst);
    }

    pub fn current_offset(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.lock().unwrap().len()
    }

    pub fn acked_count(&self, min_offset: u64) -> usize {
        self.replicas.lock().unwrap().iter()
            .filter(|r| r.acked_offset.load(Ordering::SeqCst) >= min_offset)
            .count()
    }

    pub fn send_getack_to_all(&self) {
        let getack = Bytes::from_static(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
        let replicas = self.replicas.lock().unwrap();
        for replica in replicas.iter() {
            let _ = replica.sender.send(getack.clone());
        }
    }
}

fn generate_replid() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let mut hasher = Sha256::new();
    hasher.update(nanos.to_le_bytes());
    hasher.update(b"redis-replid-seed");
    let result = hasher.finalize();
    result[..20].iter().map(|b| format!("{:02x}", b)).collect()
}

// ── Master-side: accept a replica connection after PSYNC ──────────────────────

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

pub async fn run_replica_server(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    repl: Arc<ReplicationState>,
) -> anyhow::Result<()> {
    let fullresync = format!("+FULLRESYNC {} {}\r\n", repl.replid, repl.current_offset());
    writer.write_all(fullresync.as_bytes()).await?;

    let rdb_header = format!("${}\r\n", EMPTY_RDB.len());
    writer.write_all(rdb_header.as_bytes()).await?;
    writer.write_all(EMPTY_RDB).await?;

    let (sender, mut receiver) = mpsc::unbounded_channel::<Bytes>();
    let acked_offset = repl.add_replica(sender);

    let mut buf = [0u8; 512];

    loop {
        tokio::select! {
            Some(data) = receiver.recv() => {
                writer.write_all(&data).await?;
            }
            result = reader.read(&mut buf) => {
                match result? {
                    0 => break,
                    n => {
                        if let Some(args) = crate::resp::parse(&buf[..n]) {
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

// ── Slave-side: connect to master, handshake, receive commands ────────────────

pub async fn run_replica_client(
    master_host: String,
    master_port: u16,
    my_port: u16,
    store: Arc<Store>,
    repl: Arc<ReplicationState>,
) {
    let addr = format!("{}:{}", master_host, master_port);
    let stream = match tokio::net::TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(error) => {
            eprintln!("replica: connect failed: {error}");
            return;
        }
    };
    if let Err(error) = replica_handshake_and_sync(stream, my_port, store, repl).await {
        eprintln!("replica: {error}");
    }
}

async fn replica_handshake_and_sync(
    stream: tokio::net::TcpStream,
    my_port: u16,
    store: Arc<Store>,
    _repl: Arc<ReplicationState>,
) -> anyhow::Result<()> {
    let (mut reader, mut writer) = stream.into_split();

    writer.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    read_one_line(&mut reader).await?;

    let port_str = my_port.to_string();
    let msg = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port_str.len(),
        port_str
    );
    writer.write_all(msg.as_bytes()).await?;
    read_one_line(&mut reader).await?;

    writer.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await?;
    read_one_line(&mut reader).await?;

    writer.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
    read_one_line(&mut reader).await?;

    skip_rdb(&mut reader).await?;

    let mut carry: Vec<u8> = vec![];
    let mut buf = [0u8; 4096];
    let mut offset: u64 = 0;

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
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
                            offset.to_string().len(),
                            offset
                        );
                        offset += consumed as u64;
                        writer.write_all(ack.as_bytes()).await?;
                    } else {
                        offset += consumed as u64;
                        apply_propagated_command(&args, &store);
                    }
                }
                None => break,
            }
        }
    }

    Ok(())
}

async fn read_one_line(reader: &mut tokio::net::tcp::OwnedReadHalf) -> anyhow::Result<()> {
    let mut byte = [0u8; 1];
    loop {
        reader.read_exact(&mut byte).await?;
        if byte[0] == b'\n' {
            return Ok(());
        }
    }
}

async fn skip_rdb(reader: &mut tokio::net::tcp::OwnedReadHalf) -> anyhow::Result<()> {
    let mut header: Vec<u8> = vec![];
    let mut byte = [0u8; 1];
    loop {
        reader.read_exact(&mut byte).await?;
        header.push(byte[0]);
        if header.ends_with(b"\r\n") {
            break;
        }
    }
    let len: usize = String::from_utf8_lossy(&header)
        .trim()
        .strip_prefix('$')
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut remaining = len;
    let mut buf = [0u8; 512];
    while remaining > 0 {
        let to_read = remaining.min(buf.len());
        let n = reader.read(&mut buf[..to_read]).await?;
        if n == 0 {
            break;
        }
        remaining -= n;
    }
    Ok(())
}

fn try_parse_command(data: &[u8]) -> Option<(Vec<String>, usize)> {
    if data.is_empty() || data[0] != b'*' {
        return None;
    }
    let cr = data.iter().position(|&b| b == b'\r')?;
    let count: usize = std::str::from_utf8(&data[1..cr]).ok()?.parse().ok()?;

    let mut pos = cr + 2;
    let mut args = Vec::with_capacity(count);

    for _ in 0..count {
        if pos >= data.len() || data[pos] != b'$' {
            return None;
        }
        let cr2 = data[pos..].iter().position(|&b| b == b'\r')?;
        let len: usize = std::str::from_utf8(&data[pos + 1..pos + cr2]).ok()?.parse().ok()?;
        pos += cr2 + 2;
        if pos + len + 2 > data.len() {
            return None;
        }
        args.push(String::from_utf8_lossy(&data[pos..pos + len]).to_string());
        pos += len + 2;
    }

    Some((args, pos))
}

fn apply_propagated_command(args: &[String], store: &Arc<Store>) {
    if args.is_empty() {
        return;
    }
    match args[0].to_uppercase().as_str() {
        "SET" if args.len() >= 3 => {
            let ttl = parse_set_ttl(args);
            store.set(args[1].clone(), args[2].clone(), ttl);
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
