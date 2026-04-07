use std::sync::Arc;

use crate::application::ports::AclPort;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct AuthCommands {
    acl: Arc<dyn AclPort>,
}

impl AuthCommands {
    pub fn new(acl: Arc<dyn AclPort>) -> Self {
        Self { acl }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        self.acl.authenticate(username, password)
    }

    pub fn getuser(&self, username: &str) -> Bytes {
        match (self.acl.user_flags(username), self.acl.user_passwords(username)) {
            (Some(flags), Some(passwords)) => RespEncoder::array(vec![
                RespEncoder::bulk_string("flags"),
                RespEncoder::array(flags.iter().map(|f| RespEncoder::bulk_string(f)).collect()),
                RespEncoder::bulk_string("passwords"),
                RespEncoder::array(passwords.iter().map(|p| RespEncoder::bulk_string(p)).collect()),
                RespEncoder::bulk_string("commands"),
                RespEncoder::bulk_string("+@all"),
                RespEncoder::bulk_string("keys"),
                RespEncoder::bulk_string(""),
                RespEncoder::bulk_string("channels"),
                RespEncoder::bulk_string("*"),
                RespEncoder::bulk_string("selectors"),
                RespEncoder::array(vec![]),
            ]),
            _ => RespEncoder::null_bulk(),
        }
    }

    pub fn setuser(&self, args: &[String]) -> Bytes {
        let (Some(username), Some(password_arg)) = (args.get(2), args.get(3)) else {
            return RespEncoder::error("wrong number of arguments for 'acl setuser' command");
        };
        let password = password_arg.strip_prefix('>').unwrap_or(password_arg);
        if self.acl.set_user_password(username, password.to_string()) {
            RespEncoder::simple_string("OK")
        } else {
            RespEncoder::error("ERR user does not exist")
        }
    }
}
