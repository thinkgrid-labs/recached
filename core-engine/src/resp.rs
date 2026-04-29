#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
}

impl Value {
    /// Serializes the Value back into RESP format
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Value::SimpleString(s) => {
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Error(s) => {
                buf.extend_from_slice(b"-");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Integer(i) => {
                buf.extend_from_slice(format!(":{}\r\n", i).as_bytes());
            }
            Value::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Value::BulkString(Some(data)) => {
                buf.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Value::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            Value::Array(Some(arr)) => {
                buf.extend_from_slice(format!("*{}\r\n", arr.len()).as_bytes());
                for v in arr {
                    buf.extend_from_slice(&v.serialize());
                }
            }
        }
        buf
    }

    /// Parses a byte slice into a RESP Value, returning the Value and the number of bytes consumed
    pub fn parse(buffer: &[u8]) -> Result<(Value, usize), String> {
        if buffer.is_empty() {
            return Err("Incomplete".to_string());
        }
        match buffer[0] {
            b'+' => Self::parse_simple_string(buffer),
            b'-' => Self::parse_error(buffer),
            b':' => Self::parse_integer(buffer),
            b'$' => Self::parse_bulk_string(buffer),
            b'*' => Self::parse_array(buffer),
            _ => Err("Invalid RESP type".to_string()),
        }
    }

    fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
        for i in 0..buffer.len().saturating_sub(1) {
            if buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
                return Some((&buffer[1..i], i + 2));
            }
        }
        None
    }

    fn parse_simple_string(buffer: &[u8]) -> Result<(Value, usize), String> {
        if let Some((data, len)) = Self::read_until_crlf(buffer) {
            let s = String::from_utf8_lossy(data).into_owned();
            Ok((Value::SimpleString(s), len))
        } else {
            Err("Incomplete".to_string())
        }
    }

    fn parse_error(buffer: &[u8]) -> Result<(Value, usize), String> {
        if let Some((data, len)) = Self::read_until_crlf(buffer) {
            let s = String::from_utf8_lossy(data).into_owned();
            Ok((Value::Error(s), len))
        } else {
            Err("Incomplete".to_string())
        }
    }

    fn parse_integer(buffer: &[u8]) -> Result<(Value, usize), String> {
        if let Some((data, len)) = Self::read_until_crlf(buffer) {
            let s = String::from_utf8_lossy(data);
            let i = s.parse::<i64>().map_err(|_| "Invalid integer format")?;
            Ok((Value::Integer(i), len))
        } else {
            Err("Incomplete".to_string())
        }
    }

    fn parse_bulk_string(buffer: &[u8]) -> Result<(Value, usize), String> {
        if let Some((data, head_len)) = Self::read_until_crlf(buffer) {
            let s = String::from_utf8_lossy(data);
            let length: i64 = s.parse().map_err(|_| "Invalid bulk string length")?;

            if length == -1 {
                return Ok((Value::BulkString(None), head_len));
            }

            let length = length as usize;
            let end = head_len + length + 2; // +2 for trailing CRLF
            if buffer.len() < end {
                return Err("Incomplete".to_string());
            }

            let str_data = buffer[head_len..head_len + length].to_vec();
            Ok((Value::BulkString(Some(str_data)), end))
        } else {
            Err("Incomplete".to_string())
        }
    }

    fn parse_array(buffer: &[u8]) -> Result<(Value, usize), String> {
        if let Some((data, mut offset)) = Self::read_until_crlf(buffer) {
            let s = String::from_utf8_lossy(data);
            let count: i64 = s.parse().map_err(|_| "Invalid array length")?;

            if count == -1 {
                return Ok((Value::Array(None), offset));
            }

            let mut arr = Vec::new();
            for _ in 0..count {
                let (val, len) = Self::parse(&buffer[offset..])?;
                arr.push(val);
                offset += len;
            }

            Ok((Value::Array(Some(arr)), offset))
        } else {
            Err("Incomplete".to_string())
        }
    }
}
