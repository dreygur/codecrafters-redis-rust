use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("value is not an integer or out of range")]
    NotAnInteger,
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("The ID specified in XADD must be greater than 0-0")]
    ZeroId,
    #[error("The ID specified in XADD is equal or smaller than the target stream top item")]
    NotIncremental,
    #[error("Invalid stream ID format")]
    InvalidFormat,
}

// ── String entry with optional TTL ────────────────────────────────────────────

struct Entry {
    value: String,
    expires_at: Option<Instant>,
}

impl Entry {
    fn new(value: String, ttl_millis: Option<u64>) -> Self {
        let expires_at = ttl_millis.map(|ms| Instant::now() + Duration::from_millis(ms));
        Self { value, expires_at }
    }

    fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |t| Instant::now() > t)
    }
}

// ── Sorted set ────────────────────────────────────────────────────────────────

struct SortedSet {
    members: HashMap<String, f64>,
}

impl SortedSet {
    fn new() -> Self {
        Self { members: HashMap::new() }
    }

    fn add(&mut self, score: f64, member: String) -> bool {
        let is_new = !self.members.contains_key(&member);
        self.members.insert(member, score);
        is_new
    }

    fn rank(&self, member: &str) -> Option<usize> {
        self.sorted().iter().position(|(m, _)| *m == member)
    }

    fn score(&self, member: &str) -> Option<f64> {
        self.members.get(member).copied()
    }

    fn card(&self) -> usize {
        self.members.len()
    }

    fn range(&self, start: i64, stop: i64) -> Vec<String> {
        let sorted = self.sorted();
        let len = sorted.len();
        if len == 0 {
            return vec![];
        }
        let start = resolve_index(start, len);
        let stop = resolve_index(stop, len).min(len - 1);
        if start > stop {
            return vec![];
        }
        sorted[start..=stop].iter().map(|(m, _)| m.to_string()).collect()
    }

    fn all(&self) -> Vec<(String, f64)> {
        self.sorted().into_iter().map(|(m, s)| (m.to_string(), s)).collect()
    }

    fn remove(&mut self, member: &str) -> bool {
        self.members.remove(member).is_some()
    }

    fn sorted(&self) -> Vec<(&str, f64)> {
        let mut members: Vec<(&str, f64)> = self.members.iter().map(|(m, &s)| (m.as_str(), s)).collect();
        members.sort_by(|(m1, s1), (m2, s2)| s1.total_cmp(s2).then(m1.cmp(m2)));
        members
    }
}

fn resolve_index(idx: i64, len: usize) -> usize {
    if idx < 0 {
        (len as i64 + idx).max(0) as usize
    } else {
        idx as usize
    }
}

// ── List internal state (data + BLPOP waiters) ────────────────────────────────

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
            if !has_waiter || !has_data {
                break;
            }
            let sender = self.waiters.get_mut(key).unwrap().pop_front().unwrap();
            let value = self.data.get_mut(key).unwrap().remove(0);
            if sender.send(value).is_err() {
                continue;
            }
            break;
        }
    }
}

// ── Stream types and internal state ───────────────────────────────────────────

pub type StreamEntry = (String, Vec<(String, String)>);
pub type StreamNotification = (String, String, Vec<(String, String)>);

struct StreamWaiter {
    after: (u64, u64),
    sender: mpsc::UnboundedSender<StreamNotification>,
}

struct StreamsState {
    data: HashMap<String, Vec<StreamEntry>>,
    waiters: HashMap<String, Vec<StreamWaiter>>,
}

impl StreamsState {
    fn new() -> Self {
        Self { data: HashMap::new(), waiters: HashMap::new() }
    }

    fn notify_waiters(&mut self, key: &str, new_id: (u64, u64), entry_id: &str, fields: &[(String, String)]) {
        let Some(key_waiters) = self.waiters.get_mut(key) else { return; };
        key_waiters.retain(|waiter| {
            if new_id > waiter.after {
                let _ = waiter.sender.send((key.to_string(), entry_id.to_string(), fields.to_vec()));
                false
            } else {
                true
            }
        });
    }
}

// ── Store ─────────────────────────────────────────────────────────────────────

pub struct Store {
    strings: Mutex<HashMap<String, Entry>>,
    lists: Mutex<ListsState>,
    zsets: Mutex<HashMap<String, SortedSet>>,
    streams: Mutex<StreamsState>,
    key_versions: Mutex<HashMap<String, u64>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            strings: Mutex::new(HashMap::new()),
            lists: Mutex::new(ListsState::new()),
            zsets: Mutex::new(HashMap::new()),
            streams: Mutex::new(StreamsState::new()),
            key_versions: Mutex::new(HashMap::new()),
        }
    }

    fn bump_version(&self, key: &str) {
        *self.key_versions.lock().unwrap().entry(key.to_string()).or_insert(0) += 1;
    }

    // ── String ops ────────────────────────────────────────────────────────────

    pub fn get(&self, key: &str) -> Option<String> {
        let mut map = self.strings.lock().unwrap();
        match map.get(key) {
            Some(entry) if entry.is_expired() => {
                map.remove(key);
                None
            }
            Some(entry) => Some(entry.value.clone()),
            None => None,
        }
    }

    pub fn set(&self, key: String, value: String, ttl_millis: Option<u64>) {
        self.strings.lock().unwrap().insert(key.clone(), Entry::new(value, ttl_millis));
        self.bump_version(&key);
    }

    pub fn incr(&self, key: &str) -> Result<i64, StoreError> {
        let mut map = self.strings.lock().unwrap();
        let current = match map.get(key) {
            Some(entry) if entry.is_expired() => {
                map.remove(key);
                0i64
            }
            Some(entry) => entry.value.parse::<i64>().map_err(|_| StoreError::NotAnInteger)?,
            None => 0i64,
        };
        let new_value = current + 1;
        map.insert(key.to_string(), Entry::new(new_value.to_string(), None));
        drop(map);
        self.bump_version(key);
        Ok(new_value)
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let mut map = self.strings.lock().unwrap();
        map.retain(|_, entry| !entry.is_expired());
        if pattern == "*" {
            return map.keys().cloned().collect();
        }
        map.keys().filter(|k| glob_match(pattern, k)).cloned().collect()
    }

    pub fn get_type(&self, key: &str) -> &'static str {
        if self.strings.lock().unwrap().contains_key(key) {
            return "string";
        }
        if self.zsets.lock().unwrap().contains_key(key) {
            return "zset";
        }
        if self.lists.lock().unwrap().data.contains_key(key) {
            return "list";
        }
        if self.streams.lock().unwrap().data.contains_key(key) {
            return "stream";
        }
        "none"
    }

    pub fn key_version(&self, key: &str) -> u64 {
        *self.key_versions.lock().unwrap().get(key).unwrap_or(&0)
    }

    // ── List ops ──────────────────────────────────────────────────────────────

    pub fn rpush(&self, key: &str, values: Vec<String>) -> i64 {
        let mut state = self.lists.lock().unwrap();
        let list = state.data.entry(key.to_string()).or_default();
        list.extend(values);
        let len = list.len() as i64;
        state.notify_waiters(key);
        drop(state);
        self.bump_version(key);
        len
    }

    pub fn lpush(&self, key: &str, values: Vec<String>) -> i64 {
        let mut state = self.lists.lock().unwrap();
        let list = state.data.entry(key.to_string()).or_default();
        for value in values {
            list.insert(0, value);
        }
        let len = list.len() as i64;
        state.notify_waiters(key);
        drop(state);
        self.bump_version(key);
        len
    }

    pub fn lpop(&self, key: &str) -> Option<String> {
        let mut state = self.lists.lock().unwrap();
        let list = state.data.get_mut(key)?;
        if list.is_empty() {
            return None;
        }
        let value = list.remove(0);
        drop(state);
        self.bump_version(key);
        Some(value)
    }

    pub fn lpop_count(&self, key: &str, count: usize) -> Vec<String> {
        let mut state = self.lists.lock().unwrap();
        let Some(list) = state.data.get_mut(key) else { return vec![]; };
        let n = count.min(list.len());
        let values: Vec<String> = list.drain(..n).collect();
        drop(state);
        if !values.is_empty() {
            self.bump_version(key);
        }
        values
    }

    pub fn rpop(&self, key: &str) -> Option<String> {
        let value = self.lists.lock().unwrap().data.get_mut(key)?.pop()?;
        self.bump_version(key);
        Some(value)
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        let state = self.lists.lock().unwrap();
        let Some(list) = state.data.get(key) else { return vec![]; };
        let len = list.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };
        if start > stop || start >= len {
            return vec![];
        }
        list[start.max(0) as usize..(stop + 1).min(len) as usize].to_vec()
    }

    pub fn lrem(&self, key: &str, count: i64, value: &str) -> i64 {
        let mut state = self.lists.lock().unwrap();
        let Some(list) = state.data.get_mut(key) else { return 0; };
        let removed = if count == 0 {
            let before = list.len();
            list.retain(|v| v != value);
            (before - list.len()) as i64
        } else if count > 0 {
            let mut removed = 0i64;
            let mut i = 0;
            while i < list.len() && removed < count {
                if list[i] == value {
                    list.remove(i);
                    removed += 1;
                } else {
                    i += 1;
                }
            }
            removed
        } else {
            let mut removed = 0i64;
            let mut i = list.len() as isize - 1;
            while i >= 0 && removed < -count {
                if list[i as usize] == value {
                    list.remove(i as usize);
                    removed += 1;
                }
                i -= 1;
            }
            removed
        };
        drop(state);
        if removed > 0 {
            self.bump_version(key);
        }
        removed
    }

    pub fn llen(&self, key: &str) -> i64 {
        self.lists.lock().unwrap().data.get(key).map(|l| l.len() as i64).unwrap_or(0)
    }

    pub fn blpop_or_wait(&self, key: &str) -> Result<String, oneshot::Receiver<String>> {
        let mut state = self.lists.lock().unwrap();
        if let Some(list) = state.data.get_mut(key) {
            if !list.is_empty() {
                return Ok(list.remove(0));
            }
        }
        let (sender, receiver) = oneshot::channel();
        state.waiters.entry(key.to_string()).or_default().push_back(sender);
        Err(receiver)
    }

    // ── Sorted set ops ────────────────────────────────────────────────────────

    pub fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> i64 {
        let mut map = self.zsets.lock().unwrap();
        let zset = map.entry(key.to_string()).or_insert_with(SortedSet::new);
        let count = pairs.into_iter().filter(|(score, member)| zset.add(*score, member.clone())).count() as i64;
        drop(map);
        self.bump_version(key);
        count
    }

    pub fn zrank(&self, key: &str, member: &str) -> Option<i64> {
        self.zsets.lock().unwrap().get(key)?.rank(member).map(|r| r as i64)
    }

    pub fn zrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        self.zsets.lock().unwrap().get(key).map(|zs| zs.range(start, stop)).unwrap_or_default()
    }

    pub fn zcard(&self, key: &str) -> i64 {
        self.zsets.lock().unwrap().get(key).map(|zs| zs.card() as i64).unwrap_or(0)
    }

    pub fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        self.zsets.lock().unwrap().get(key)?.score(member)
    }

    pub fn zrem(&self, key: &str, members: &[String]) -> i64 {
        let mut map = self.zsets.lock().unwrap();
        let Some(zset) = map.get_mut(key) else { return 0; };
        let removed = members.iter().filter(|m| zset.remove(m)).count() as i64;
        drop(map);
        if removed > 0 {
            self.bump_version(key);
        }
        removed
    }

    // ── Geo ops (stored in zsets using geohash scores) ────────────────────────

    pub fn geoadd(&self, key: &str, lon: f64, lat: f64, member: String) -> bool {
        let score = crate::geo::encode(lon, lat);
        let mut map = self.zsets.lock().unwrap();
        let is_new = map.entry(key.to_string()).or_insert_with(SortedSet::new).add(score, member);
        drop(map);
        self.bump_version(key);
        is_new
    }

    pub fn geopos(&self, key: &str, member: &str) -> Option<(f64, f64)> {
        let score = self.zsets.lock().unwrap().get(key)?.score(member)?;
        Some(crate::geo::decode(score))
    }

    pub fn geodist(&self, key: &str, m1: &str, m2: &str) -> Option<f64> {
        let (lon1, lat1) = self.geopos(key, m1)?;
        let (lon2, lat2) = self.geopos(key, m2)?;
        Some(crate::geo::distance_m(lon1, lat1, lon2, lat2))
    }

    pub fn geosearch_radius(&self, key: &str, lon: f64, lat: f64, radius_m: f64) -> Vec<(String, f64)> {
        let map = self.zsets.lock().unwrap();
        let Some(zset) = map.get(key) else { return vec![]; };
        zset.all().into_iter().filter_map(|(member, score)| {
            let (mlon, mlat) = crate::geo::decode(score);
            let dist = crate::geo::distance_m(lon, lat, mlon, mlat);
            (dist <= radius_m).then_some((member, dist))
        }).collect()
    }

    // ── Stream ops ────────────────────────────────────────────────────────────

    pub fn xadd(&self, key: &str, id: &str, fields: Vec<(String, String)>) -> Result<String, StreamError> {
        let mut state = self.streams.lock().unwrap();
        let (ms, seq) = resolve_stream_id(id, key, &state.data)?;
        if ms == 0 && seq == 0 {
            return Err(StreamError::ZeroId);
        }
        if let Some((last_id, _)) = state.data.get(key).and_then(|s| s.last()) {
            let (last_ms, last_seq) = parse_stream_id(last_id).unwrap();
            if ms < last_ms || (ms == last_ms && seq <= last_seq) {
                return Err(StreamError::NotIncremental);
            }
        }
        let final_id = format!("{}-{}", ms, seq);
        state.data.entry(key.to_string()).or_default().push((final_id.clone(), fields.clone()));
        state.notify_waiters(key, (ms, seq), &final_id, &fields);
        drop(state);
        self.bump_version(key);
        Ok(final_id)
    }

    pub fn xrange(&self, key: &str, start: &str, end: &str, count: Option<usize>) -> Vec<StreamEntry> {
        let state = self.streams.lock().unwrap();
        let Some(stream) = state.data.get(key) else { return vec![]; };
        let start_id = if start == "-" { (0u64, 0u64) } else { parse_range_start(start) };
        let end_id = if end == "+" { (u64::MAX, u64::MAX) } else { parse_range_end(end) };
        stream.iter()
            .filter(|(id, _)| {
                let parsed = parse_stream_id(id).unwrap_or((0, 0));
                parsed >= start_id && parsed <= end_id
            })
            .take(count.unwrap_or(usize::MAX))
            .map(|(id, fields)| (id.clone(), fields.clone()))
            .collect()
    }

    pub fn xread(&self, keys: &[String], ids: &[String]) -> Vec<(String, Vec<StreamEntry>)> {
        let state = self.streams.lock().unwrap();
        let mut results = vec![];
        for (key, start_id) in keys.iter().zip(ids.iter()) {
            let Some(stream) = state.data.get(key) else { continue; };
            let after = parse_stream_id(start_id).unwrap_or((0, 0));
            let entries: Vec<StreamEntry> = stream.iter()
                .filter(|(id, _)| parse_stream_id(id).unwrap_or((0, 0)) > after)
                .map(|(id, fields)| (id.clone(), fields.clone()))
                .collect();
            if !entries.is_empty() {
                results.push((key.clone(), entries));
            }
        }
        results
    }

    pub fn stream_last_id(&self, key: &str) -> (u64, u64) {
        self.streams.lock().unwrap()
            .data.get(key)
            .and_then(|s| s.last())
            .and_then(|(id, _)| parse_stream_id(id).ok())
            .unwrap_or((0, 0))
    }

    pub fn xread_blocking(
        &self,
        key: &str,
        after_id: (u64, u64),
        sender: mpsc::UnboundedSender<StreamNotification>,
    ) -> Option<Vec<StreamEntry>> {
        let mut state = self.streams.lock().unwrap();
        let entries: Vec<StreamEntry> = state.data.get(key)
            .map(|stream| {
                stream.iter()
                    .filter(|(id, _)| parse_stream_id(id).unwrap_or((0, 0)) > after_id)
                    .map(|(id, fields)| (id.clone(), fields.clone()))
                    .collect()
            })
            .unwrap_or_default();
        if !entries.is_empty() {
            return Some(entries);
        }
        state.waiters.entry(key.to_string()).or_default().push(StreamWaiter {
            after: after_id,
            sender,
        });
        None
    }
}

// ── Glob pattern matching ─────────────────────────────────────────────────────

fn glob_match(pattern: &str, string: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = string.chars().collect();
    let mut dp = vec![vec![false; s.len() + 1]; p.len() + 1];
    dp[0][0] = true;
    for i in 1..=p.len() {
        if p[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }
    for i in 1..=p.len() {
        for j in 1..=s.len() {
            dp[i][j] = if p[i - 1] == '*' {
                dp[i - 1][j] || dp[i][j - 1]
            } else if p[i - 1] == '?' || p[i - 1] == s[j - 1] {
                dp[i - 1][j - 1]
            } else {
                false
            };
        }
    }
    dp[p.len()][s.len()]
}

// ── Stream ID helpers ─────────────────────────────────────────────────────────

fn resolve_stream_id(
    id: &str,
    key: &str,
    data: &HashMap<String, Vec<StreamEntry>>,
) -> Result<(u64, u64), StreamError> {
    if id == "*" || id.ends_with("-*") {
        let ms = if id == "*" {
            current_ms()
        } else {
            id.trim_end_matches("-*").parse().map_err(|_| StreamError::InvalidFormat)?
        };
        let seq = next_seq_for_ms(key, ms, data);
        let seq = if ms == 0 && seq == 0 { 1 } else { seq };
        return Ok((ms, seq));
    }
    parse_stream_id(id)
}

pub fn parse_stream_id(id: &str) -> Result<(u64, u64), StreamError> {
    let (ms_str, seq_str) = id.split_once('-').ok_or(StreamError::InvalidFormat)?;
    let ms = ms_str.parse().map_err(|_| StreamError::InvalidFormat)?;
    let seq = seq_str.parse().map_err(|_| StreamError::InvalidFormat)?;
    Ok((ms, seq))
}

fn parse_range_start(id: &str) -> (u64, u64) {
    if let Some((ms_str, seq_str)) = id.split_once('-') {
        return (ms_str.parse().unwrap_or(0), seq_str.parse().unwrap_or(0));
    }
    (id.parse().unwrap_or(0), 0)
}

fn parse_range_end(id: &str) -> (u64, u64) {
    if let Some((ms_str, seq_str)) = id.split_once('-') {
        return (ms_str.parse().unwrap_or(u64::MAX), seq_str.parse().unwrap_or(u64::MAX));
    }
    (id.parse().unwrap_or(u64::MAX), u64::MAX)
}

fn next_seq_for_ms(key: &str, ms: u64, data: &HashMap<String, Vec<StreamEntry>>) -> u64 {
    data.get(key)
        .and_then(|s| s.last())
        .and_then(|(id, _)| parse_stream_id(id).ok())
        .map(|(last_ms, last_seq)| if last_ms == ms { last_seq + 1 } else { 0 })
        .unwrap_or(0)
}

fn current_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
