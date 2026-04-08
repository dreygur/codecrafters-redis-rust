use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::domain::XAddError;

type StreamEntry = (String, Vec<(String, String)>);

pub(super) struct StreamStore {
    data: Arc<Mutex<HashMap<String, Vec<StreamEntry>>>>,
}

impl StreamStore {
    pub(super) fn new() -> Self {
        Self { data: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub(super) fn xadd(
        &self,
        key: &str,
        id: &str,
        fields: Vec<(String, String)>,
    ) -> Result<String, XAddError> {
        let (ms, seq) = parse_entry_id(id)?;

        if ms == 0 && seq == 0 {
            return Err(XAddError::ZeroId);
        }

        let final_id = format!("{}-{}", ms, seq);
        let mut map = self.data.lock().unwrap();
        let stream = map.entry(key.to_string()).or_default();

        if let Some((last_id, _)) = stream.last() {
            let (last_ms, last_seq) = parse_entry_id(last_id).unwrap();
            if ms < last_ms || (ms == last_ms && seq <= last_seq) {
                return Err(XAddError::NotIncremental);
            }
        }

        stream.push((final_id.clone(), fields));
        Ok(final_id)
    }

    pub(super) fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let map = self.data.lock().unwrap();
        let Some(stream) = map.get(key) else { return vec![]; };

        let start_id = if start == "-" { (0u64, 0u64) } else { parse_entry_id(start).unwrap_or((0, 0)) };
        let end_id = if end == "+" { (u64::MAX, u64::MAX) } else { parse_entry_id(end).unwrap_or((u64::MAX, u64::MAX)) };

        stream.iter()
            .filter(|(id, _)| {
                let parsed = parse_entry_id(id).unwrap_or((0, 0));
                parsed >= start_id && parsed <= end_id
            })
            .take(count.unwrap_or(usize::MAX))
            .map(|(id, fields)| (id.clone(), fields.clone()))
            .collect()
    }

    pub(super) fn xread(
        &self,
        keys: &[String],
        ids: &[String],
    ) -> Vec<(String, Vec<StreamEntry>)> {
        let map = self.data.lock().unwrap();
        let mut results = vec![];

        for (key, start_id) in keys.iter().zip(ids.iter()) {
            let Some(stream) = map.get(key) else { continue; };
            let after = parse_entry_id(start_id).unwrap_or((0, 0));

            let entries: Vec<StreamEntry> = stream.iter()
                .filter(|(id, _)| parse_entry_id(id).unwrap_or((0, 0)) > after)
                .map(|(id, fields)| (id.clone(), fields.clone()))
                .collect();

            if !entries.is_empty() {
                results.push((key.clone(), entries));
            }
        }

        results
    }

    pub(super) fn contains(&self, key: &str) -> bool {
        self.data.lock().unwrap().contains_key(key)
    }
}

fn parse_entry_id(id: &str) -> Result<(u64, u64), XAddError> {
    let (ms_str, seq_str) = id.split_once('-').ok_or(XAddError::InvalidFormat)?;
    let ms = ms_str.parse().map_err(|_| XAddError::InvalidFormat)?;
    let seq = seq_str.parse().map_err(|_| XAddError::InvalidFormat)?;
    Ok((ms, seq))
}

impl Clone for StreamStore {
    fn clone(&self) -> Self {
        Self { data: Arc::clone(&self.data) }
    }
}
