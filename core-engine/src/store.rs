use crate::cmd::Command;
use crate::resp::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct KeyValueStore {
    data: Arc<RwLock<HashMap<String, String>>>,
    max_keys: Option<usize>,
}

impl Default for KeyValueStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_keys: None,
        }
    }

    pub fn with_max_keys(max: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_keys: Some(max),
        }
    }

    /// Executes a given Command and returns the RESP Value to respond with
    pub fn execute(&self, cmd: Command) -> Value {
        match cmd {
            Command::Ping(msg) => {
                if let Some(m) = msg {
                    Value::BulkString(Some(m.into_bytes()))
                } else {
                    Value::SimpleString("PONG".to_string())
                }
            }
            Command::Auth(_) => {
                // Reached only when no password is configured; server interceptor handles the rest.
                Value::SimpleString("OK".to_string())
            }
            Command::Set(key, val) => {
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                if let Some(max) = self.max_keys
                    && lock.len() >= max
                    && !lock.contains_key(&key)
                {
                    return Value::Error("ERR max keys limit reached".to_string());
                }
                lock.insert(key, val);
                Value::SimpleString("OK".to_string())
            }
            Command::Get(key) => {
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                match lock.get(&key) {
                    Some(val) => Value::BulkString(Some(val.clone().into_bytes())),
                    None => Value::BulkString(None),
                }
            }
            Command::Del(keys) => {
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                let count = keys
                    .into_iter()
                    .filter(|k| lock.remove(k).is_some())
                    .count();
                Value::Integer(count as i64)
            }
            Command::Unknown(name) => Value::Error(format!("ERR unknown command '{}'", name)),
        }
    }
}
