#[derive(Clone, Default)]
pub struct Config {
    pub port: u16,
    pub replicaof: Option<(String, u16)>,
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
}

impl Config {
    pub fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut cfg = Self { port: 6379, ..Default::default() };
        let mut i = 1;

        while i < args.len() {
            match args[i].as_str() {
                "--port" => {
                    cfg.port = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(6379);
                    i += 2;
                }
                "--replicaof" => {
                    if let Some(next) = args.get(i + 1) {
                        let parts: Vec<&str> = next.splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            // "--replicaof" "host port" (single arg)
                            if let Ok(p) = parts[1].parse::<u16>() {
                                cfg.replicaof = Some((parts[0].to_string(), p));
                            }
                            i += 2;
                        } else if let Some(port_str) = args.get(i + 2) {
                            // "--replicaof" "host" "port" (two args)
                            if let Ok(p) = port_str.parse::<u16>() {
                                cfg.replicaof = Some((next.clone(), p));
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
                    cfg.dir = args.get(i + 1).cloned();
                    i += 2;
                }
                "--dbfilename" => {
                    cfg.dbfilename = args.get(i + 1).cloned();
                    i += 2;
                }
                _ => { i += 1; }
            }
        }

        cfg
    }

    pub fn rdb_path(&self) -> Option<String> {
        Some(format!("{}/{}", self.dir.as_ref()?, self.dbfilename.as_ref()?))
    }
}
