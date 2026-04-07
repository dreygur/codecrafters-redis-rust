pub fn parse(input: &[u8]) -> Option<Vec<String>> {
    let s = std::str::from_utf8(input).ok()?;
    let mut lines = s.split("\r\n");

    let count = lines.next()?.strip_prefix('*')?.parse::<usize>().ok()?;
    let mut args = Vec::with_capacity(count);

    for _ in 0..count {
        lines.next()?.strip_prefix('$')?.parse::<usize>().ok()?;
        args.push(lines.next()?.to_string());
    }

    Some(args)
}

pub fn bulk_string(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

pub fn simple_string(s: &str) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

pub fn integer(n: i64) -> Vec<u8> {
    format!(":{}\r\n", n).into_bytes()
}

pub fn error(msg: &str) -> Vec<u8> {
    format!("-ERR {}\r\n", msg).into_bytes()
}

pub fn array(elements: Vec<Vec<u8>>) -> Vec<u8> {
    let mut resp = format!("*{}\r\n", elements.len()).into_bytes();
    for e in elements {
        resp.extend(e);
    }
    resp
}

pub const NULL_BULK: &[u8] = b"$-1\r\n";
