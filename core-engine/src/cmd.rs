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
