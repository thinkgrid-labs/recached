use crate::resp::Value;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping(Option<String>),
    Auth(String),
    Set(String, String),
    Get(String),
    Del(Vec<String>),
    Unknown(String),
}

impl Command {
    /// Translates a RESP Value (usually an Array of BulkStrings) into a typed Command
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

                match cmd_name.as_str() {
                    "PING" => {
                        let msg = if arr.len() > 1 {
                            match &arr[1] {
                                Value::BulkString(Some(data)) => {
                                    Some(String::from_utf8_lossy(data).into_owned())
                                }
                                Value::SimpleString(s) => Some(s.clone()),
                                _ => None,
                            }
                        } else {
                            None
                        };
                        Ok(Command::Ping(msg))
                    }
                    "AUTH" => {
                        if arr.len() < 2 {
                            return Err(
                                "ERR wrong number of arguments for 'auth' command".to_string()
                            );
                        }
                        let pwd = extract_string(&arr[1]).unwrap_or_default();
                        Ok(Command::Auth(pwd))
                    }
                    "SET" => {
                        if arr.len() < 3 {
                            return Err(
                                "ERR wrong number of arguments for 'set' command".to_string()
                            );
                        }
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        let val = extract_string(&arr[2]).unwrap_or_default();
                        Ok(Command::Set(key, val))
                    }
                    "GET" => {
                        if arr.len() < 2 {
                            return Err(
                                "ERR wrong number of arguments for 'get' command".to_string()
                            );
                        }
                        let key = extract_string(&arr[1]).unwrap_or_default();
                        Ok(Command::Get(key))
                    }
                    "DEL" => {
                        if arr.len() < 2 {
                            return Err(
                                "ERR wrong number of arguments for 'del' command".to_string()
                            );
                        }
                        let keys = arr[1..].iter().filter_map(extract_string).collect();
                        Ok(Command::Del(keys))
                    }
                    _ => Ok(Command::Unknown(cmd_name)),
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
    fn set_ok() {
        let cmd = Command::from_value(array(&["SET", "key", "value"])).unwrap();
        assert_eq!(cmd, Command::Set("key".to_string(), "value".to_string()));
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
