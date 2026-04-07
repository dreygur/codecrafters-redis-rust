#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyVersion {
    key: String,
    version: u64,
}

impl KeyVersion {
    pub fn new(key: String, version: u64) -> Self {
        Self { key, version }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}
