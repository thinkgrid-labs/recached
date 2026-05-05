use crate::resp::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum SetExpiry {
    Ex(u64),    // seconds
    Px(u64),    // milliseconds
    Exat(u64),  // unix-time seconds
    Pxat(u64),  // unix-time milliseconds
    KeepTtl,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetCondition {
    Nx,
    Xx,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SetOptions {
    pub expiry: Option<SetExpiry>,
    pub condition: Option<SetCondition>,
    pub get: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Ping(Option<String>),
    Auth(String),
    // Strings
    Set(String, String, SetOptions),
    Get(String),
    Del(Vec<String>),
    Unlink(Vec<String>),
    Append(String, String),
    Strlen(String),
    GetSet(String, String),
    MGet(Vec<String>),
    MSet(Vec<(String, String)>),
    SetNx(String, String),
    SetEx(String, u64, String),
    PSetEx(String, u64, String),
    Incr(String),
    Decr(String),
    IncrBy(String, i64),
    DecrBy(String, i64),
    // Expiry
    Expire(String, u64),
    PExpire(String, u64),
    ExpireAt(String, u64),
    PExpireAt(String, u64),
    Ttl(String),
    PTtl(String),
    Persist(String),
    // Keys
    Exists(Vec<String>),
    Keys(String),
    Scan(u64, Option<String>, Option<usize>),
    DbSize,
    FlushDb,
    Rename(String, String),
    Type(String),
    Unknown(String),
}

impl Command {
    /// Translates a RESP Value (usually an Array of BulkStrings) into a typed Command.
    pub fn from_value(value: Value) -> Result<Command, String> {
        match value {
            Value::Array(Some(arr)) => {
                if arr.is_empty() {
                    return Err("Empty command".to_string());
                }
                let cmd_name = match &arr[0] {
                    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => return Err("Invalid command name type".to_string()),
                };

                macro_rules! need {
                    ($n:expr) => {
                        if arr.len() < $n {
                            return Err(format!(
                                "ERR wrong number of arguments for '{}' command",
                                cmd_name.to_lowercase()
                            ));
                        }
                    };
                }

                match cmd_name.as_str() {
                    "PING" => {
                        let msg = if arr.len() > 1 { extract_string(&arr[1]) } else { None };
                        Ok(Command::Ping(msg))
                    }
                    "AUTH" => {
                        need!(2);
                        Ok(Command::Auth(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "SET" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let val = extract_string(&arr[2]).unwrap_or_default();
                        let mut opts = SetOptions::default();
                        let mut i = 3usize;
                        while i < arr.len() {
                            let flag =
                                extract_string(&arr[i]).unwrap_or_default().to_uppercase();
                            match flag.as_str() {
                                "EX" => {
                                    i += 1;
                                    if i >= arr.len() {
                                        return Err("ERR syntax error".to_string());
                                    }
                                    let n = extract_int(&arr[i])?;
                                    if n <= 0 {
                                        return Err(
                                            "ERR invalid expire time in 'set' command"
                                                .to_string(),
                                        );
                                    }
                                    opts.expiry = Some(SetExpiry::Ex(n as u64));
                                }
                                "PX" => {
                                    i += 1;
                                    if i >= arr.len() {
                                        return Err("ERR syntax error".to_string());
                                    }
                                    let n = extract_int(&arr[i])?;
                                    if n <= 0 {
                                        return Err(
                                            "ERR invalid expire time in 'set' command"
                                                .to_string(),
                                        );
                                    }
                                    opts.expiry = Some(SetExpiry::Px(n as u64));
                                }
                                "EXAT" => {
                                    i += 1;
                                    if i >= arr.len() {
                                        return Err("ERR syntax error".to_string());
                                    }
                                    let n = extract_int(&arr[i])?;
                                    if n <= 0 {
                                        return Err(
                                            "ERR invalid expire time in 'set' command"
                                                .to_string(),
                                        );
                                    }
                                    opts.expiry = Some(SetExpiry::Exat(n as u64));
                                }
                                "PXAT" => {
                                    i += 1;
                                    if i >= arr.len() {
                                        return Err("ERR syntax error".to_string());
                                    }
                                    let n = extract_int(&arr[i])?;
                                    if n <= 0 {
                                        return Err(
                                            "ERR invalid expire time in 'set' command"
                                                .to_string(),
                                        );
                                    }
                                    opts.expiry = Some(SetExpiry::Pxat(n as u64));
                                }
                                "KEEPTTL" => {
                                    opts.expiry = Some(SetExpiry::KeepTtl);
                                }
                                "NX" => {
                                    opts.condition = Some(SetCondition::Nx);
                                }
                                "XX" => {
                                    opts.condition = Some(SetCondition::Xx);
                                }
                                "GET" => {
                                    opts.get = true;
                                }
                                _ => return Err("ERR syntax error".to_string()),
                            }
                            i += 1;
                        }
                        Ok(Command::Set(key, val, opts))
                    }
                    "GET" => {
                        need!(2);
                        Ok(Command::Get(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "DEL" => {
                        need!(2);
                        Ok(Command::Del(arr[1..].iter().filter_map(extract_string).collect()))
                    }
                    "UNLINK" => {
                        need!(2);
                        Ok(Command::Unlink(arr[1..].iter().filter_map(extract_string).collect()))
                    }
                    "APPEND" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let val = extract_string(&arr[2]).unwrap_or_default();
                        Ok(Command::Append(key, val))
                    }
                    "STRLEN" => {
                        need!(2);
                        Ok(Command::Strlen(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "GETSET" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let val = extract_string(&arr[2]).unwrap_or_default();
                        Ok(Command::GetSet(key, val))
                    }
                    "MGET" => {
                        need!(2);
                        Ok(Command::MGet(arr[1..].iter().filter_map(extract_string).collect()))
                    }
                    "MSET" => {
                        if arr.len() < 3 || (arr.len() - 1) % 2 != 0 {
                            return Err(
                                "ERR wrong number of arguments for 'mset' command".to_string()
                            );
                        }
                        let pairs = arr[1..]
                            .chunks(2)
                            .map(|c| {
                                (
                                    extract_string(&c[0]).unwrap_or_default(),
                                    extract_string(&c[1]).unwrap_or_default(),
                                )
                            })
                            .collect();
                        Ok(Command::MSet(pairs))
                    }
                    "SETNX" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let val = extract_string(&arr[2]).unwrap_or_default();
                        Ok(Command::SetNx(key, val))
                    }
                    "SETEX" => {
                        need!(4);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let secs = extract_int(&arr[2])?;
                        if secs <= 0 {
                            return Err(
                                "ERR invalid expire time in 'setex' command".to_string()
                            );
                        }
                        let val = extract_string(&arr[3]).unwrap_or_default();
                        Ok(Command::SetEx(key, secs as u64, val))
                    }
                    "PSETEX" => {
                        need!(4);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let ms = extract_int(&arr[2])?;
                        if ms <= 0 {
                            return Err(
                                "ERR invalid expire time in 'psetex' command".to_string()
                            );
                        }
                        let val = extract_string(&arr[3]).unwrap_or_default();
                        Ok(Command::PSetEx(key, ms as u64, val))
                    }
                    "INCR" => {
                        need!(2);
                        Ok(Command::Incr(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "DECR" => {
                        need!(2);
                        Ok(Command::Decr(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "INCRBY" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let delta = extract_int(&arr[2])?;
                        Ok(Command::IncrBy(key, delta))
                    }
                    "DECRBY" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let delta = extract_int(&arr[2])?;
                        Ok(Command::DecrBy(key, delta))
                    }
                    "EXPIRE" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let secs = extract_int(&arr[2])?;
                        if secs < 0 {
                            return Err(
                                "ERR invalid expire time in 'expire' command".to_string()
                            );
                        }
                        Ok(Command::Expire(key, secs as u64))
                    }
                    "PEXPIRE" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let ms = extract_int(&arr[2])?;
                        if ms < 0 {
                            return Err(
                                "ERR invalid expire time in 'pexpire' command".to_string()
                            );
                        }
                        Ok(Command::PExpire(key, ms as u64))
                    }
                    "EXPIREAT" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let ts = extract_int(&arr[2])?;
                        if ts < 0 {
                            return Err(
                                "ERR invalid expire time in 'expireat' command".to_string()
                            );
                        }
                        Ok(Command::ExpireAt(key, ts as u64))
                    }
                    "PEXPIREAT" => {
                        need!(3);
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let ts = extract_int(&arr[2])?;
                        if ts < 0 {
                            return Err(
                                "ERR invalid expire time in 'pexpireat' command".to_string()
                            );
                        }
                        Ok(Command::PExpireAt(key, ts as u64))
                    }
                    "TTL" => {
                        need!(2);
                        Ok(Command::Ttl(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "PTTL" => {
                        need!(2);
                        Ok(Command::PTtl(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "PERSIST" => {
                        need!(2);
                        Ok(Command::Persist(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "EXISTS" => {
                        need!(2);
                        Ok(Command::Exists(arr[1..].iter().filter_map(extract_string).collect()))
                    }
                    "KEYS" => {
                        need!(2);
                        Ok(Command::Keys(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    "SCAN" => {
                        need!(2);
                        let cursor_str = extract_string(&arr[1]).unwrap_or_default();
                        let cursor = cursor_str.parse::<u64>().map_err(|_| {
                            "ERR value is not an integer or out of range".to_string()
                        })?;
                        let mut pattern = None;
                        let mut count = None;
                        let mut i = 2usize;
                        while i < arr.len() {
                            let opt =
                                extract_string(&arr[i]).unwrap_or_default().to_uppercase();
                            match opt.as_str() {
                                "MATCH" => {
                                    i += 1;
                                    if i >= arr.len() {
                                        return Err("ERR syntax error".to_string());
                                    }
                                    pattern = extract_string(&arr[i]);
                                }
                                "COUNT" => {
                                    i += 1;
                                    if i >= arr.len() {
                                        return Err("ERR syntax error".to_string());
                                    }
                                    let n = extract_int(&arr[i])?;
                                    count = Some(n as usize);
                                }
                                _ => return Err("ERR syntax error".to_string()),
                            }
                            i += 1;
                        }
                        Ok(Command::Scan(cursor, pattern, count))
                    }
                    "DBSIZE" => Ok(Command::DbSize),
                    "FLUSHDB" => Ok(Command::FlushDb),
                    "RENAME" => {
                        need!(3);
                        let src = extract_string(&arr[1]).unwrap_or_default();
                        let dst = extract_string(&arr[2]).unwrap_or_default();
                        Ok(Command::Rename(src, dst))
                    }
                    "TYPE" => {
                        need!(2);
                        Ok(Command::Type(extract_string(&arr[1]).unwrap_or_default()))
                    }
                    _ => Ok(Command::Unknown(cmd_name.to_owned())),
                }
            }
            _ => Err("Commands must be RESP Arrays".to_string()),
        }
    }
}

fn extract_string(val: &Value) -> Option<String> {
    match val {
        Value::BulkString(Some(data)) => Some(String::from_utf8_lossy(data).into_owned()),
        Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

fn extract_int(val: &Value) -> Result<i64, String> {
    match val {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data)
            .parse::<i64>()
            .map_err(|_| "ERR value is not an integer or out of range".to_string()),
        Value::SimpleString(s) => s
            .parse::<i64>()
            .map_err(|_| "ERR value is not an integer or out of range".to_string()),
        Value::Integer(i) => Ok(*i),
        _ => Err("ERR value is not an integer or out of range".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(Some(s.as_bytes().to_vec()))
    }

    fn array(parts: &[&str]) -> Value {
        Value::Array(Some(parts.iter().map(|s| bulk(s)).collect()))
    }

    #[test]
    fn ping_no_arg() {
        let cmd = Command::from_value(array(&["PING"])).unwrap();
        assert_eq!(cmd, Command::Ping(None));
    }

    #[test]
    fn ping_with_arg() {
        let cmd = Command::from_value(array(&["PING", "hello"])).unwrap();
        assert_eq!(cmd, Command::Ping(Some("hello".to_string())));
    }

    #[test]
    fn ping_case_insensitive() {
        let cmd = Command::from_value(array(&["ping"])).unwrap();
        assert_eq!(cmd, Command::Ping(None));
    }

    #[test]
    fn auth_ok() {
        let cmd = Command::from_value(array(&["AUTH", "secret"])).unwrap();
        assert_eq!(cmd, Command::Auth("secret".to_string()));
    }

    #[test]
    fn auth_missing_password() {
        let result = Command::from_value(array(&["AUTH"]));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("wrong number of arguments"));
    }

    #[test]
    fn set_plain() {
        let cmd = Command::from_value(array(&["SET", "key", "value"])).unwrap();
        assert_eq!(
            cmd,
            Command::Set("key".to_string(), "value".to_string(), SetOptions::default())
        );
    }

    #[test]
    fn set_ex() {
        let cmd = Command::from_value(array(&["SET", "k", "v", "EX", "60"])).unwrap();
        assert_eq!(
            cmd,
            Command::Set(
                "k".to_string(),
                "v".to_string(),
                SetOptions { expiry: Some(SetExpiry::Ex(60)), ..Default::default() }
            )
        );
    }

    #[test]
    fn set_px() {
        let cmd = Command::from_value(array(&["SET", "k", "v", "PX", "5000"])).unwrap();
        assert_eq!(
            cmd,
            Command::Set(
                "k".to_string(),
                "v".to_string(),
                SetOptions { expiry: Some(SetExpiry::Px(5000)), ..Default::default() }
            )
        );
    }

    #[test]
    fn set_nx() {
        let cmd = Command::from_value(array(&["SET", "k", "v", "NX"])).unwrap();
        assert_eq!(
            cmd,
            Command::Set(
                "k".to_string(),
                "v".to_string(),
                SetOptions { condition: Some(SetCondition::Nx), ..Default::default() }
            )
        );
    }

    #[test]
    fn set_xx() {
        let cmd = Command::from_value(array(&["SET", "k", "v", "XX"])).unwrap();
        assert_eq!(
            cmd,
            Command::Set(
                "k".to_string(),
                "v".to_string(),
                SetOptions { condition: Some(SetCondition::Xx), ..Default::default() }
            )
        );
    }

    #[test]
    fn set_ex_nx_combined() {
        let cmd = Command::from_value(array(&["SET", "k", "v", "EX", "10", "NX"])).unwrap();
        assert_eq!(
            cmd,
            Command::Set(
                "k".to_string(),
                "v".to_string(),
                SetOptions {
                    expiry: Some(SetExpiry::Ex(10)),
                    condition: Some(SetCondition::Nx),
                    get: false,
                }
            )
        );
    }

    #[test]
    fn set_invalid_ex_zero() {
        let result = Command::from_value(array(&["SET", "k", "v", "EX", "0"]));
        assert!(result.is_err());
    }

    #[test]
    fn set_unknown_option() {
        let result = Command::from_value(array(&["SET", "k", "v", "BOGUS"]));
        assert!(result.is_err());
    }

    #[test]
    fn set_missing_value() {
        let result = Command::from_value(array(&["SET", "key"]));
        assert!(result.is_err());
    }

    #[test]
    fn get_ok() {
        let cmd = Command::from_value(array(&["GET", "key"])).unwrap();
        assert_eq!(cmd, Command::Get("key".to_string()));
    }

    #[test]
    fn get_missing_key() {
        let result = Command::from_value(array(&["GET"]));
        assert!(result.is_err());
    }

    #[test]
    fn del_single_key() {
        let cmd = Command::from_value(array(&["DEL", "key"])).unwrap();
        assert_eq!(cmd, Command::Del(vec!["key".to_string()]));
    }

    #[test]
    fn del_multiple_keys() {
        let cmd = Command::from_value(array(&["DEL", "a", "b", "c"])).unwrap();
        assert_eq!(
            cmd,
            Command::Del(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
    }

    #[test]
    fn del_missing_key() {
        let result = Command::from_value(array(&["DEL"]));
        assert!(result.is_err());
    }

    #[test]
    fn mget_ok() {
        let cmd = Command::from_value(array(&["MGET", "a", "b"])).unwrap();
        assert_eq!(cmd, Command::MGet(vec!["a".to_string(), "b".to_string()]));
    }

    #[test]
    fn mset_ok() {
        let cmd = Command::from_value(array(&["MSET", "a", "1", "b", "2"])).unwrap();
        assert_eq!(
            cmd,
            Command::MSet(vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ])
        );
    }

    #[test]
    fn mset_odd_args() {
        let result = Command::from_value(array(&["MSET", "a", "1", "b"]));
        assert!(result.is_err());
    }

    #[test]
    fn incr_ok() {
        let cmd = Command::from_value(array(&["INCR", "counter"])).unwrap();
        assert_eq!(cmd, Command::Incr("counter".to_string()));
    }

    #[test]
    fn incrby_ok() {
        let cmd = Command::from_value(array(&["INCRBY", "counter", "5"])).unwrap();
        assert_eq!(cmd, Command::IncrBy("counter".to_string(), 5));
    }

    #[test]
    fn expire_ok() {
        let cmd = Command::from_value(array(&["EXPIRE", "k", "60"])).unwrap();
        assert_eq!(cmd, Command::Expire("k".to_string(), 60));
    }

    #[test]
    fn expire_negative() {
        let result = Command::from_value(array(&["EXPIRE", "k", "-1"]));
        assert!(result.is_err());
    }

    #[test]
    fn ttl_ok() {
        let cmd = Command::from_value(array(&["TTL", "k"])).unwrap();
        assert_eq!(cmd, Command::Ttl("k".to_string()));
    }

    #[test]
    fn exists_multiple() {
        let cmd = Command::from_value(array(&["EXISTS", "a", "b", "a"])).unwrap();
        assert_eq!(
            cmd,
            Command::Exists(vec!["a".to_string(), "b".to_string(), "a".to_string()])
        );
    }

    #[test]
    fn dbsize_ok() {
        let cmd = Command::from_value(array(&["DBSIZE"])).unwrap();
        assert_eq!(cmd, Command::DbSize);
    }

    #[test]
    fn flushdb_ok() {
        let cmd = Command::from_value(array(&["FLUSHDB"])).unwrap();
        assert_eq!(cmd, Command::FlushDb);
    }

    #[test]
    fn scan_basic() {
        let cmd = Command::from_value(array(&["SCAN", "0"])).unwrap();
        assert_eq!(cmd, Command::Scan(0, None, None));
    }

    #[test]
    fn scan_with_match() {
        let cmd = Command::from_value(array(&["SCAN", "0", "MATCH", "user:*"])).unwrap();
        assert_eq!(cmd, Command::Scan(0, Some("user:*".to_string()), None));
    }

    #[test]
    fn scan_with_count() {
        let cmd = Command::from_value(array(&["SCAN", "0", "COUNT", "100"])).unwrap();
        assert_eq!(cmd, Command::Scan(0, None, Some(100)));
    }

    #[test]
    fn rename_ok() {
        let cmd = Command::from_value(array(&["RENAME", "src", "dst"])).unwrap();
        assert_eq!(cmd, Command::Rename("src".to_string(), "dst".to_string()));
    }

    #[test]
    fn unknown_command() {
        let cmd = Command::from_value(array(&["HSET", "key", "field", "val"])).unwrap();
        assert!(matches!(cmd, Command::Unknown(_)));
    }

    #[test]
    fn non_array_input() {
        let result = Command::from_value(Value::SimpleString("PING".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("RESP Arrays"));
    }

    #[test]
    fn empty_array() {
        let result = Command::from_value(Value::Array(Some(vec![])));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Empty command"));
    }
}
