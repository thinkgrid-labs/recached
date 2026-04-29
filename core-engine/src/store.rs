use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::resp::Value;
use crate::cmd::Command;

#[derive(Clone, Default)]
pub struct KeyValueStore {
    // For the MVP, a simple RwLock HashMap. 
    // In future phases, this can be sharded for extreme multi-core throughput.
    data: Arc<RwLock<HashMap<String, String>>>,
}

impl KeyValueStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
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
                // If this hits the store, it means the server interceptor let it through 
                // (e.g., already authenticated or no password required).
                Value::SimpleString("OK".to_string())
            }
            Command::Set(key, val) => {
                let mut lock = self.data.write().unwrap();
                lock.insert(key, val);
                Value::SimpleString("OK".to_string())
            }
            Command::Get(key) => {
                let lock = self.data.read().unwrap();
                if let Some(val) = lock.get(&key) {
                    Value::BulkString(Some(val.clone().into_bytes()))
                } else {
                    Value::BulkString(None) // Null bulk string indicates key not found
                }
            }
            Command::Del(keys) => {
                let mut lock = self.data.write().unwrap();
                let mut count = 0;
                for key in keys {
                    if lock.remove(&key).is_some() {
                        count += 1;
                    }
                }
                Value::Integer(count)
            }
            Command::Unknown(name) => {
                Value::Error(format!("ERR unknown command '{}'", name))
            }
        }
    }
}
