use std::collections::HashMap;
use std::sync::Mutex;

use sha2::{Digest, Sha256};

struct User {
    enabled: bool,
    nopass: bool,
    passwords: Vec<String>,
}

impl User {
    fn default_user() -> Self {
        Self {
            enabled: true,
            nopass: true,
            passwords: vec![],
        }
    }
}

pub struct Acl {
    users: Mutex<HashMap<String, User>>,
}

impl Acl {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), User::default_user());
        Self {
            users: Mutex::new(users),
        }
    }

    pub fn is_nopass(&self) -> bool {
        self.users
            .lock()
            .unwrap()
            .get("default")
            .map_or(false, |user| user.nopass)
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        let users = self.users.lock().unwrap();
        let Some(user) = users.get(username) else {
            return false;
        };
        if !user.enabled {
            return false;
        }
        if user.nopass {
            return true;
        }
        let hashed = hash_password(password);
        user.passwords.iter().any(|p| p == &hashed)
    }

    pub fn set_default_password(&self, password: String) {
        let mut users = self.users.lock().unwrap();
        if let Some(user) = users.get_mut("default") {
            user.nopass = false;
            user.passwords = vec![hash_password(&password)];
        }
    }

    pub fn set_user_password(&self, username: &str, password: String) -> bool {
        let mut users = self.users.lock().unwrap();
        let Some(user) = users.get_mut(username) else {
            return false;
        };
        user.nopass = false;
        user.passwords = vec![hash_password(&password)];
        true
    }

    pub fn user_flags(&self, username: &str) -> Option<Vec<String>> {
        let users = self.users.lock().unwrap();
        let user = users.get(username)?;
        let mut flags = vec![if user.enabled { "on".to_string() } else { "off".to_string() }];
        if user.nopass {
            flags.push("nopass".to_string());
        }
        Some(flags)
    }

    pub fn user_passwords(&self, username: &str) -> Option<Vec<String>> {
        let users = self.users.lock().unwrap();
        Some(users.get(username)?.passwords.clone())
    }
}

fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    format!("{:x}", hasher.finalize())
}
