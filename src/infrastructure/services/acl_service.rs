use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use sha2::{Digest, Sha256};

use crate::application::ports::AclPort;
use crate::domain::entities::User;

pub struct AclService {
    users: Arc<Mutex<HashMap<String, User>>>,
}

impl AclService {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), User::default_user());
        Self {
            users: Arc::new(Mutex::new(users)),
        }
    }

    fn hash_password(password: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn set_user_password(&self, username: &str, password: String) -> bool {
        let mut users = self.users.lock().unwrap();
        let Some(user) = users.get_mut(username) else {
            return false;
        };
        eprintln!("ACL: Setting password for user '{}'", username);
        user.nopass = false;
        user.passwords = vec![Self::hash_password(&password)];
        true
    }
}

impl AclPort for AclService {
    fn is_nopass(&self) -> bool {
        self.users
            .lock()
            .unwrap()
            .get("default")
            .map_or(false, |u| u.nopass)
    }

    fn set_default_password(&self, password: String) {
        let mut users = self.users.lock().unwrap();
        if let Some(user) = users.get_mut("default") {
            user.nopass = false;
            user.passwords = vec![Self::hash_password(&password)];
        }
    }

    fn set_user_password(&self, username: &str, password: String) -> bool {
        let mut users = self.users.lock().unwrap();
        let Some(user) = users.get_mut(username) else {
            return false;
        };
        user.nopass = false;
        user.passwords = vec![Self::hash_password(&password)];
        true
    }

    fn authenticate(&self, username: &str, password: &str) -> bool {
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
        let hashed = Self::hash_password(password);
        user.passwords.iter().any(|p| p == &hashed)
    }

    fn user_flags(&self, username: &str) -> Option<Vec<String>> {
        let users = self.users.lock().unwrap();
        let user = users.get(username)?;
        let mut flags = vec![if user.enabled {
            "on".to_string()
        } else {
            "off".to_string()
        }];
        if user.nopass {
            flags.push("nopass".to_string());
        }
        Some(flags)
    }

    fn user_passwords(&self, username: &str) -> Option<Vec<String>> {
        let users = self.users.lock().unwrap();
        Some(users.get(username)?.passwords.clone())
    }
}

impl Clone for AclService {
    fn clone(&self) -> Self {
        Self {
            users: self.users.clone(),
        }
    }
}
