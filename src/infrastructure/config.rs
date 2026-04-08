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
                    if let (Some(host), Some(p)) = (args.get(i + 1), args.get(i + 2)) {
                        cfg.replicaof = p.parse().ok().map(|p| (host.clone(), p));
                    }
                    i += 3;
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
