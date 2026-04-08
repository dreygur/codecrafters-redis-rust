use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::domain::XAddError;

pub(super) type StreamEntry = (String, Vec<(String, String)>);

pub(super) fn resolve_id(
    id: &str,
    key: &str,
    data: &HashMap<String, Vec<StreamEntry>>,
) -> Result<(u64, u64), XAddError> {
    if id == "*" || id.ends_with("-*") {
        let ms = if id == "*" {
            current_ms()
        } else {
            id.trim_end_matches("-*").parse().map_err(|_| XAddError::InvalidFormat)?
        };
        let seq = next_seq_for_ms(key, ms, data);
        // 0-0 is never valid; when ms=0 and no prior entries, start at 0-1
        let seq = if ms == 0 && seq == 0 { 1 } else { seq };
        return Ok((ms, seq));
    }
    parse_entry_id(id)
}

pub(super) fn parse_entry_id(id: &str) -> Result<(u64, u64), XAddError> {
    let (ms_str, seq_str) = id.split_once('-').ok_or(XAddError::InvalidFormat)?;
    let ms = ms_str.parse().map_err(|_| XAddError::InvalidFormat)?;
    let seq = seq_str.parse().map_err(|_| XAddError::InvalidFormat)?;
    Ok((ms, seq))
}

pub(super) fn parse_range_start(id: &str) -> (u64, u64) {
    if let Some((ms_str, seq_str)) = id.split_once('-') {
        let ms = ms_str.parse().unwrap_or(0);
        let seq = seq_str.parse().unwrap_or(0);
        return (ms, seq);
    }
    (id.parse().unwrap_or(0), 0)
}

pub(super) fn parse_range_end(id: &str) -> (u64, u64) {
    if let Some((ms_str, seq_str)) = id.split_once('-') {
        let ms = ms_str.parse().unwrap_or(u64::MAX);
        let seq = seq_str.parse().unwrap_or(u64::MAX);
        return (ms, seq);
    }
    (id.parse().unwrap_or(u64::MAX), u64::MAX)
}

fn next_seq_for_ms(key: &str, ms: u64, data: &HashMap<String, Vec<StreamEntry>>) -> u64 {
    data.get(key)
        .and_then(|s| s.last())
        .and_then(|(id, _)| parse_entry_id(id).ok())
        .map(|(last_ms, last_seq)| if last_ms == ms { last_seq + 1 } else { 0 })
        .unwrap_or(0)
}

fn current_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
