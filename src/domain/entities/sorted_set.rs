use std::collections::HashMap;

pub struct SortedSet {
    members: HashMap<String, f64>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
        }
    }

    pub fn add(&mut self, score: f64, member: String) -> bool {
        let is_new = !self.members.contains_key(&member);
        self.members.insert(member, score);
        is_new
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        self.sorted().iter().position(|(m, _)| *m == member)
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.members.get(member).copied()
    }

    pub fn card(&self) -> usize {
        self.members.len()
    }

    pub fn range(&self, start: i64, stop: i64) -> Vec<String> {
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
        sorted[start..=stop]
            .iter()
            .map(|(m, _)| m.to_string())
            .collect()
    }

    pub fn all(&self) -> Vec<(String, f64)> {
        self.sorted()
            .into_iter()
            .map(|(m, s)| (m.to_string(), s))
            .collect()
    }

    pub fn remove(&mut self, member: &str) -> bool {
        self.members.remove(member).is_some()
    }

    fn sorted(&self) -> Vec<(&str, f64)> {
        let mut v: Vec<(&str, f64)> = self.members.iter().map(|(m, &s)| (m.as_str(), s)).collect();
        v.sort_by(|(m1, s1), (m2, s2)| s1.total_cmp(s2).then(m1.cmp(m2)));
        v
    }
}

fn resolve_index(idx: i64, len: usize) -> usize {
    if idx < 0 {
        (len as i64 + idx).max(0) as usize
    } else {
        idx as usize
    }
}
