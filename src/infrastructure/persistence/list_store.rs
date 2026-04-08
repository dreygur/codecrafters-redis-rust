use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

struct ListsState {
    data: HashMap<String, Vec<String>>,
    waiters: HashMap<String, VecDeque<oneshot::Sender<String>>>,
}

impl ListsState {
    fn new() -> Self {
        Self { data: HashMap::new(), waiters: HashMap::new() }
    }

    fn notify_waiters(&mut self, key: &str) {
        loop {
            let has_waiter = self.waiters.get(key).map(|w| !w.is_empty()).unwrap_or(false);
            let has_data = self.data.get(key).map(|l| !l.is_empty()).unwrap_or(false);
            if !has_waiter || !has_data { break; }
            let sender = self.waiters.get_mut(key).unwrap().pop_front().unwrap();
            let val = self.data.get_mut(key).unwrap().remove(0);
            if sender.send(val).is_err() { continue; }
            break;
        }
    }
}

pub(super) struct ListStore {
    state: Arc<Mutex<ListsState>>,
}

impl ListStore {
    pub(super) fn new() -> Self {
        Self { state: Arc::new(Mutex::new(ListsState::new())) }
    }

    pub(super) fn rpush(&self, key: &str, values: Vec<String>) -> i64 {
        let mut state = self.state.lock().unwrap();
        let list = state.data.entry(key.to_string()).or_default();
        list.extend(values);
        let len = list.len() as i64;
        state.notify_waiters(key);
        len
    }

    pub(super) fn lpush(&self, key: &str, values: Vec<String>) -> i64 {
        let mut state = self.state.lock().unwrap();
        let list = state.data.entry(key.to_string()).or_default();
        for v in values { list.insert(0, v); }
        let len = list.len() as i64;
        state.notify_waiters(key);
        len
    }

    pub(super) fn lpop(&self, key: &str) -> Option<String> {
        let mut state = self.state.lock().unwrap();
        let list = state.data.get_mut(key)?;
        if list.is_empty() { return None; }
        Some(list.remove(0))
    }

    pub(super) fn lpop_count(&self, key: &str, count: usize) -> Vec<String> {
        let mut state = self.state.lock().unwrap();
        let Some(list) = state.data.get_mut(key) else { return vec![]; };
        let n = count.min(list.len());
        list.drain(..n).collect()
    }

    pub(super) fn rpop(&self, key: &str) -> Option<String> {
        self.state.lock().unwrap().data.get_mut(key)?.pop()
    }

    pub(super) fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        let state = self.state.lock().unwrap();
        let Some(list) = state.data.get(key) else { return vec![]; };
        let len = list.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };
        if start > stop || start >= len { return vec![]; }
        list[start.max(0) as usize..(stop + 1).min(len) as usize].to_vec()
    }

    pub(super) fn lrem(&self, key: &str, count: i64, value: &str) -> i64 {
        let mut state = self.state.lock().unwrap();
        let Some(list) = state.data.get_mut(key) else { return 0; };
        if count == 0 {
            let before = list.len();
            list.retain(|v| v != value);
            (before - list.len()) as i64
        } else if count > 0 {
            let mut removed = 0i64;
            let mut i = 0;
            while i < list.len() && removed < count {
                if list[i] == value { list.remove(i); removed += 1; } else { i += 1; }
            }
            removed
        } else {
            let mut removed = 0i64;
            let mut i = list.len() as isize - 1;
            while i >= 0 && removed < -count {
                if list[i as usize] == value { list.remove(i as usize); removed += 1; }
                i -= 1;
            }
            removed
        }
    }

    pub(super) fn llen(&self, key: &str) -> i64 {
        self.state.lock().unwrap().data.get(key).map(|l| l.len() as i64).unwrap_or(0)
    }

    pub(super) fn contains(&self, key: &str) -> bool {
        self.state.lock().unwrap().data.contains_key(key)
    }

    pub(super) fn blpop_or_wait(&self, key: &str) -> Result<String, oneshot::Receiver<String>> {
        let mut state = self.state.lock().unwrap();
        if let Some(list) = state.data.get_mut(key) {
            if !list.is_empty() { return Ok(list.remove(0)); }
        }
        let (tx, rx) = oneshot::channel();
        state.waiters.entry(key.to_string()).or_default().push_back(tx);
        Err(rx)
    }
}

impl Clone for ListStore {
    fn clone(&self) -> Self {
        Self { state: Arc::clone(&self.state) }
    }
}
