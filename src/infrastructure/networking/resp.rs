use bytes::{Bytes, BytesMut};

pub struct RespEncoder;

impl RespEncoder {
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

    pub fn bulk_string(s: &str) -> Bytes {
        Bytes::from(format!("${}\r\n{}\r\n", s.len(), s))
    }

    pub fn simple_string(s: &str) -> Bytes {
        Bytes::from(format!("+{}\r\n", s))
    }

    pub fn integer(n: i64) -> Bytes {
        Bytes::from(format!(":{}\r\n", n))
    }

    pub fn error(msg: &str) -> Bytes {
        Bytes::from(format!("-ERR {}\r\n", msg))
    }

    pub fn raw_error(kind_and_msg: &str) -> Bytes {
        Bytes::from(format!("-{}\r\n", kind_and_msg))
    }

    pub fn null_bulk() -> Bytes {
        Bytes::from_static(b"$-1\r\n")
    }

    pub fn null_array() -> Bytes {
        Bytes::from_static(b"*-1\r\n")
    }

    pub fn array(elements: Vec<Bytes>) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(format!("*{}\r\n", elements.len()).as_bytes());
        for e in elements {
            buf.extend_from_slice(&e);
        }
        buf.freeze()
    }
}
