use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[allow(dead_code)]
pub struct User {
    pub name: String,
    pub enabled: bool,
    pub nopass: bool,
    pub passwords: Vec<String>,
}

impl User {
    fn default_user() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            nopass: true,
            passwords: vec![],
        }
    }
}

#[derive(Clone)]
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

    /// Removes the nopass flag from the default user and sets its password.
    pub fn set_default_password(&self, password: String) {
        let mut users = self.users.lock().unwrap();
        if let Some(user) = users.get_mut("default") {
            user.nopass = false;
            user.passwords = vec![password];
        }
    }

    /// Returns true if the default user has the nopass flag (no auth required).
    pub fn is_nopass(&self) -> bool {
        self.users
            .lock()
            .unwrap()
            .get("default")
            .map_or(false, |u| u.nopass)
    }

    /// Returns true if the credentials are valid for the named user.
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
        user.passwords.iter().any(|p| p == password)
    }

    /// Returns the flag strings for ACL GETUSER (e.g. `["on", "nopass"]`).
    pub fn user_flags(&self, username: &str) -> Option<Vec<String>> {
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

    /// Returns the stored passwords for ACL GETUSER.
    pub fn user_passwords(&self, username: &str) -> Option<Vec<String>> {
        let users = self.users.lock().unwrap();
        Some(users.get(username)?.passwords.clone())
    }

    /// Returns true if the user exists.
    #[allow(dead_code)]
    pub fn user_exists(&self, username: &str) -> bool {
        self.users.lock().unwrap().contains_key(username)
    }
}
