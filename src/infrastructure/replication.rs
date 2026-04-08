use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

pub struct ReplicaHandle {
    pub tx: mpsc::UnboundedSender<Bytes>,
    pub acked_offset: Arc<AtomicU64>,
}

pub struct ReplicationState {
    pub role: &'static str,
    pub replid: String,
    pub offset: AtomicU64,
    pub replicas: Mutex<Vec<ReplicaHandle>>,
}

impl ReplicationState {
    pub fn master() -> Arc<Self> {
        Arc::new(Self {
            role: "master",
            replid: generate_replid(),
            offset: AtomicU64::new(0),
            replicas: Mutex::new(vec![]),
        })
    }

    pub fn slave() -> Arc<Self> {
        Arc::new(Self {
            role: "slave",
            replid: "?".to_string(),
            offset: AtomicU64::new(0),
            replicas: Mutex::new(vec![]),
        })
    }

    pub fn add_replica(&self, tx: mpsc::UnboundedSender<Bytes>) -> Arc<AtomicU64> {
        let acked = Arc::new(AtomicU64::new(0));
        self.replicas.lock().unwrap().push(ReplicaHandle {
            tx,
            acked_offset: Arc::clone(&acked),
        });
        acked
    }

    pub fn propagate(&self, data: Bytes) {
        let len = data.len() as u64;
        let mut replicas = self.replicas.lock().unwrap();
        replicas.retain(|r| r.tx.send(data.clone()).is_ok());
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
        for r in replicas.iter() {
            let _ = r.tx.send(getack.clone());
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
