pub struct Config {
    pub port: u16,
    pub replicaof: Option<(String, u16)>,
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
}

impl Config {
    pub fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut config = Self {
            port: 6379,
            replicaof: None,
            dir: None,
            dbfilename: None,
        };
        let mut i = 1;

        while i < args.len() {
            match args[i].as_str() {
                "--port" => {
                    config.port = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(6379);
                    i += 2;
                }
                "--replicaof" => {
                    if let Some(next) = args.get(i + 1) {
                        let parts: Vec<&str> = next.splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            // "--replicaof" "host port" (single quoted arg)
                            if let Ok(port) = parts[1].parse::<u16>() {
                                config.replicaof = Some((parts[0].to_string(), port));
                            }
                            i += 2;
                        } else if let Some(port_str) = args.get(i + 2) {
                            // "--replicaof" "host" "port" (two args)
                            if let Ok(port) = port_str.parse::<u16>() {
                                config.replicaof = Some((next.clone(), port));
                            }
                            i += 3;
                        } else {
                            i += 2;
                        }
                    } else {
                        i += 1;
                    }
                }
                "--dir" => {
                    config.dir = args.get(i + 1).cloned();
                    i += 2;
                }
                "--dbfilename" => {
                    config.dbfilename = args.get(i + 1).cloned();
                    i += 2;
                }
                _ => { i += 1; }
            }
        }

        config
    }

    pub fn rdb_path(&self) -> Option<String> {
        Some(format!("{}/{}", self.dir.as_ref()?, self.dbfilename.as_ref()?))
    }
}
