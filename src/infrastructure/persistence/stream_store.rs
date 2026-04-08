use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type StreamEntry = (String, Vec<(String, String)>);

pub(super) struct StreamStore {
    data: Arc<Mutex<HashMap<String, Vec<StreamEntry>>>>,
}

impl StreamStore {
    pub(super) fn new() -> Self {
        Self { data: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub(super) fn xadd(&self, key: &str, id: &str, fields: Vec<(String, String)>) -> Option<String> {
        if id.is_empty() || id.contains('*') { return None; }
        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 { return None; }
        let timestamp: u64 = parts[0].parse().ok()?;
        let seq: u64 = parts[1].parse().ok()?;
        if timestamp == 0 && seq == 0 { return None; }

        let final_id = format!("{}-{}", timestamp, seq);
        let mut map = self.data.lock().unwrap();
        let stream = map.entry(key.to_string()).or_default();

        if let Some(pos) = stream.iter().position(|(eid, _)| eid > &final_id) {
            stream.insert(pos, (final_id.clone(), fields));
        } else {
            stream.push((final_id.clone(), fields));
        }
        Some(final_id)
    }

    pub(super) fn xrange(
        &self, key: &str, start: &str, end: &str, count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let map = self.data.lock().unwrap();
        let Some(stream) = map.get(key) else { return vec![]; };
        let start_id = if start == "-" { "".to_string() } else { start.to_string() };
        let end_id = if end == "+" { std::char::MAX.to_string() } else { end.to_string() };
        stream.iter()
            .filter(|(id, _)| id >= &start_id && id <= &end_id)
            .take(count.unwrap_or(usize::MAX))
            .map(|(id, fields)| (id.clone(), fields.clone()))
            .collect()
    }

    pub(super) fn xread(
        &self, keys: &[String], ids: &[String],
    ) -> Vec<(String, Vec<StreamEntry>)> {
        let map = self.data.lock().unwrap();
        let mut results = vec![];
        for (key, start_id) in keys.iter().zip(ids.iter()) {
            let Some(stream) = map.get(key) else { continue; };
            let entries: Vec<StreamEntry> = stream.iter()
                .filter(|(id, _)| id > start_id)
                .map(|(id, fields)| (id.clone(), fields.clone()))
                .collect();
            if !entries.is_empty() { results.push((key.clone(), entries)); }
        }
        results
    }

    pub(super) fn contains(&self, key: &str) -> bool {
        self.data.lock().unwrap().contains_key(key)
    }
}

impl Clone for StreamStore {
    fn clone(&self) -> Self {
        Self { data: Arc::clone(&self.data) }
    }
}
