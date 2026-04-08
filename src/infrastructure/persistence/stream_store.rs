use std::{collections::HashMap, sync::{Arc, Mutex}};

use tokio::sync::mpsc;

use crate::domain::XAddError;
use super::stream_id::{parse_entry_id, parse_range_end, parse_range_start, resolve_id, StreamEntry};

type StreamNotification = (String, String, Vec<(String, String)>);

struct StreamWaiter {
    after: (u64, u64),
    tx: mpsc::UnboundedSender<StreamNotification>,
}

struct StreamStoreInner {
    data: HashMap<String, Vec<StreamEntry>>,
    waiters: HashMap<String, Vec<StreamWaiter>>,
}

impl StreamStoreInner {
    fn new() -> Self {
        Self { data: HashMap::new(), waiters: HashMap::new() }
    }

    fn notify_waiters(&mut self, key: &str, new_id: (u64, u64), entry_id: &str, fields: &[(String, String)]) {
        let Some(key_waiters) = self.waiters.get_mut(key) else { return; };
        key_waiters.retain(|waiter| {
            if new_id > waiter.after {
                let _ = waiter.tx.send((key.to_string(), entry_id.to_string(), fields.to_vec()));
                false
            } else {
                true
            }
        });
    }
}

pub(super) struct StreamStore {
    inner: Arc<Mutex<StreamStoreInner>>,
}

impl StreamStore {
    pub(super) fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(StreamStoreInner::new())) }
    }

    pub(super) fn xadd(
        &self,
        key: &str,
        id: &str,
        fields: Vec<(String, String)>,
    ) -> Result<String, XAddError> {
        let mut inner = self.inner.lock().unwrap();
        let (ms, seq) = resolve_id(id, key, &inner.data)?;

        if ms == 0 && seq == 0 {
            return Err(XAddError::ZeroId);
        }

        if let Some((last_id, _)) = inner.data.get(key).and_then(|s| s.last()) {
            let (last_ms, last_seq) = parse_entry_id(last_id).unwrap();
            if ms < last_ms || (ms == last_ms && seq <= last_seq) {
                return Err(XAddError::NotIncremental);
            }
        }

        let final_id = format!("{}-{}", ms, seq);
        inner.data.entry(key.to_string()).or_default().push((final_id.clone(), fields.clone()));
        inner.notify_waiters(key, (ms, seq), &final_id, &fields);

        Ok(final_id)
    }

    pub(super) fn last_entry_id(&self, key: &str) -> (u64, u64) {
        self.inner.lock().unwrap()
            .data.get(key)
            .and_then(|s| s.last())
            .and_then(|(id, _)| parse_entry_id(id).ok())
            .unwrap_or((0, 0))
    }

    pub(super) fn xread_blocking(
        &self,
        key: &str,
        after_id: (u64, u64),
        tx: mpsc::UnboundedSender<StreamNotification>,
    ) -> Option<Vec<StreamEntry>> {
        let mut inner = self.inner.lock().unwrap();

        let entries: Vec<StreamEntry> = inner.data.get(key)
            .map(|stream| {
                stream.iter()
                    .filter(|(id, _)| parse_entry_id(id).unwrap_or((0, 0)) > after_id)
                    .map(|(id, f)| (id.clone(), f.clone()))
                    .collect()
            })
            .unwrap_or_default();

        if !entries.is_empty() {
            return Some(entries);
        }

        inner.waiters.entry(key.to_string()).or_default().push(StreamWaiter {
            after: after_id,
            tx,
        });
        None
    }

    pub(super) fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let inner = self.inner.lock().unwrap();
        let Some(stream) = inner.data.get(key) else { return vec![]; };

        let start_id = if start == "-" { (0u64, 0u64) } else { parse_range_start(start) };
        let end_id = if end == "+" { (u64::MAX, u64::MAX) } else { parse_range_end(end) };

        stream.iter()
            .filter(|(id, _)| {
                let parsed = parse_entry_id(id).unwrap_or((0, 0));
                parsed >= start_id && parsed <= end_id
            })
            .take(count.unwrap_or(usize::MAX))
            .map(|(id, fields)| (id.clone(), fields.clone()))
            .collect()
    }

    pub(super) fn xread(&self, keys: &[String], ids: &[String]) -> Vec<(String, Vec<StreamEntry>)> {
        let inner = self.inner.lock().unwrap();
        let mut results = vec![];

        for (key, start_id) in keys.iter().zip(ids.iter()) {
            let Some(stream) = inner.data.get(key) else { continue; };
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
        self.inner.lock().unwrap().data.contains_key(key)
    }
}

impl Clone for StreamStore {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}
