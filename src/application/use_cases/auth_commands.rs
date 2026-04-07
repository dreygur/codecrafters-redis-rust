use crate::application::ports::AclPort;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct AuthCommands<A: AclPort> {
    acl: A,
}

impl<A: AclPort> AuthCommands<A> {
    pub fn new(acl: A) -> Self {
        Self { acl }
    }

    pub fn auth(&self, args: &[String]) -> Bytes {
        let (username, password) = match args.len() {
            2 => ("default", args[1].as_str()),
            3 => (args[1].as_str(), args[2].as_str()),
            _ => return RespEncoder::error("wrong number of arguments for 'auth' command"),
        };

        if self.acl.authenticate(username, password) {
            RespEncoder::simple_string("OK")
        } else {
            RespEncoder::raw_error("WRONGPASS invalid username-password pair or user is disabled.")
        }
    }

    pub fn whoami(&self, username: &str) -> Bytes {
        RespEncoder::bulk_string(username)
    }

    pub fn getuser(&self, username: &str) -> Bytes {
        match (
            self.acl.user_flags(username),
            self.acl.user_passwords(username),
        ) {
            (Some(flags), Some(passwords)) => RespEncoder::array(vec![
                RespEncoder::bulk_string("flags"),
                RespEncoder::array(flags.iter().map(|f| RespEncoder::bulk_string(f)).collect()),
                RespEncoder::bulk_string("passwords"),
                RespEncoder::array(
                    passwords
                        .iter()
                        .map(|p| RespEncoder::bulk_string(p))
                        .collect(),
                ),
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
}
