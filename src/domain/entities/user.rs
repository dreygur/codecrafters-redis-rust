pub struct User {
    pub name: String,
    pub enabled: bool,
    pub nopass: bool,
    pub passwords: Vec<String>,
}

impl User {
    pub fn default_user() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            nopass: true,
            passwords: vec![],
        }
    }
}
