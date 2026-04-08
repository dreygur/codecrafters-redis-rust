use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::application::ports::StorePort;

pub fn load(path: &str, store: &Arc<dyn StorePort>) {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => return,
    };
    if data.len() < 9 || &data[..5] != b"REDIS" {
        return;
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut pos = 9; // skip "REDIS0011"
    let mut expiry_ms: Option<u64> = None;

    while pos < data.len() {
        let byte = data[pos];
        pos += 1;

        match byte {
            0xFA => {
                let (_, p) = read_string(&data, pos);
                let (_, p2) = read_string(&data, p);
                pos = p2;
            }
            0xFE => {
                let (_, p) = read_length(&data, pos);
                pos = p;
            }
            0xFB => {
                let (_, p) = read_length(&data, pos);
                let (_, p2) = read_length(&data, p);
                pos = p2;
            }
            0xFC => {
                if pos + 8 <= data.len() {
                    let ms = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    expiry_ms = Some(ms);
                    pos += 8;
                }
            }
            0xFD => {
                if pos + 4 <= data.len() {
                    let secs = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    expiry_ms = Some(secs as u64 * 1000);
                    pos += 4;
                }
            }
            0xFF => break,
            0 => {
                let (key, p) = read_string(&data, pos);
                let (value, p2) = read_string(&data, p);
                pos = p2;

                let ttl = match expiry_ms.take() {
                    Some(exp) if exp <= now_ms => continue,
                    Some(exp) => Some(exp - now_ms),
                    None => None,
                };

                store.set(key, value, ttl);
            }
            _ => {
                expiry_ms = None;
                break;
            }
        }
    }
}

fn read_length(data: &[u8], pos: usize) -> (u64, usize) {
    if pos >= data.len() {
        return (0, pos);
    }
    let b = data[pos];
    match b >> 6 {
        0 => ((b & 0x3F) as u64, pos + 1),
        1 => {
            if pos + 1 >= data.len() { return (0, pos + 1); }
            (((b & 0x3F) as u64) << 8 | data[pos + 1] as u64, pos + 2)
        }
        2 => {
            if pos + 5 > data.len() { return (0, pos + 1); }
            let n = u32::from_be_bytes(data[pos + 1..pos + 5].try_into().unwrap()) as u64;
            (n, pos + 5)
        }
        _ => (0, pos + 1),
    }
}

fn read_string(data: &[u8], pos: usize) -> (String, usize) {
    if pos >= data.len() {
        return (String::new(), pos);
    }
    let b = data[pos];
    if b >> 6 == 3 {
        return read_int_encoded(data, pos);
    }
    let (len, new_pos) = read_length(data, pos);
    let end = new_pos + len as usize;
    if end > data.len() {
        return (String::new(), data.len());
    }
    (String::from_utf8_lossy(&data[new_pos..end]).to_string(), end)
}

fn read_int_encoded(data: &[u8], pos: usize) -> (String, usize) {
    match data[pos] & 0x3F {
        0 => {
            if pos + 1 >= data.len() { return (String::new(), pos + 1); }
            ((data[pos + 1] as i8).to_string(), pos + 2)
        }
        1 => {
            if pos + 2 >= data.len() { return (String::new(), pos + 1); }
            let n = i16::from_le_bytes([data[pos + 1], data[pos + 2]]);
            (n.to_string(), pos + 3)
        }
        2 => {
            if pos + 4 >= data.len() { return (String::new(), pos + 1); }
            let n = i32::from_le_bytes(data[pos + 1..pos + 5].try_into().unwrap());
            (n.to_string(), pos + 5)
        }
        _ => (String::new(), pos + 1),
    }
}
