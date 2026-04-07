pub trait AclPort: Send + Sync {
    fn is_nopass(&self) -> bool;
    fn set_default_password(&self, password: String);
    fn set_user_password(&self, username: &str, password: String) -> bool;
    fn authenticate(&self, username: &str, password: &str) -> bool;
    fn user_flags(&self, username: &str) -> Option<Vec<String>>;
    fn user_passwords(&self, username: &str) -> Option<Vec<String>>;
}
