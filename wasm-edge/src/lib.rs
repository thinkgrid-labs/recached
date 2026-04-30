use core_engine::cmd::Command;
use core_engine::resp::Value;
use core_engine::store::KeyValueStore;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use web_sys::{MessageEvent, WebSocket};

/// Encodes parts as a RESP bulk-string array, e.g. `["SET","k","v"]` → `*3\r\n$3\r\nSET\r\n…`.
fn to_resp(parts: &[&str]) -> String {
    let mut s = format!("*{}\r\n", parts.len());
    for part in parts {
        s.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    s
}

#[wasm_bindgen]
pub struct RecachedCache {
    store: Arc<KeyValueStore>,
    ws: Option<WebSocket>,
    // Held here so it is dropped (and the JS callback unregistered) when connect() is called again.
    _onmessage: Option<Closure<dyn FnMut(MessageEvent)>>,
}

impl Default for RecachedCache {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl RecachedCache {
    #[wasm_bindgen(constructor)]
    pub fn new() -> RecachedCache {
        RecachedCache {
            store: Arc::new(KeyValueStore::new()),
            ws: None,
            _onmessage: None,
        }
    }

    /// Connect to the native Recached backend via WebSockets.
    /// Calling this a second time cleanly replaces the previous connection.
    pub fn connect(&mut self, url: &str) -> Result<(), JsValue> {
        let ws = WebSocket::new(url)?;
        let store_clone = Arc::clone(&self.store);

        // Incoming messages from the server are RESP-encoded mutation commands (SET / DEL).
        let onmessage = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(text) = e.data().dyn_into::<js_sys::JsString>() {
                let s = String::from(text);
                if let Ok((value, _)) = Value::parse(s.as_bytes()) {
                    if let Ok(cmd) = Command::from_value(value) {
                        match cmd {
                            Command::Set(_, _) | Command::Del(_) => {
                                store_clone.execute(cmd);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

        // Store the closure in the struct — this keeps it alive and drops the old one.
        self._onmessage = Some(onmessage);
        self.ws = Some(ws);
        Ok(())
    }

    /// Send an AUTH command to the server. The response arrives asynchronously via onmessage.
    pub fn auth(&self, password: &str) -> String {
        if let Some(ws) = &self.ws {
            if ws.ready_state() == WebSocket::OPEN {
                let _ = ws.send_with_str(&to_resp(&["AUTH", password]));
            }
        }
        "OK".to_string()
    }

    /// Set a key-value pair locally and sync to the server.
    pub fn set(&self, key: &str, value: &str) -> String {
        let resp = self
            .store
            .execute(Command::Set(key.to_string(), value.to_string()));

        if let Some(ws) = &self.ws {
            if ws.ready_state() == WebSocket::OPEN {
                let _ = ws.send_with_str(&to_resp(&["SET", key, value]));
            }
        }

        match resp {
            Value::SimpleString(s) => s,
            Value::Error(e) => e,
            _ => "ERR".to_string(),
        }
    }

    /// Get a value from the local store (zero latency).
    pub fn get(&self, key: &str) -> Option<String> {
        match self.store.execute(Command::Get(key.to_string())) {
            Value::BulkString(Some(data)) => Some(String::from_utf8_lossy(&data).into_owned()),
            _ => None,
        }
    }

    /// Delete a key locally and sync to the server.
    pub fn del(&self, key: &str) -> i32 {
        let resp = self.store.execute(Command::Del(vec![key.to_string()]));

        if let Some(ws) = &self.ws {
            if ws.ready_state() == WebSocket::OPEN {
                let _ = ws.send_with_str(&to_resp(&["DEL", key]));
            }
        }

        match resp {
            Value::Integer(i) => i as i32,
            _ => 0,
        }
    }
}
