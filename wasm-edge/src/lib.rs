use wasm_bindgen::prelude::*;
use core_engine::store::KeyValueStore;
use core_engine::cmd::Command;
use core_engine::resp::Value;
use std::sync::Arc;
use web_sys::{WebSocket, MessageEvent};

#[wasm_bindgen]
pub struct RecachedCache {
    store: Arc<KeyValueStore>,
    ws: Option<WebSocket>,
}

#[wasm_bindgen]
impl RecachedCache {
    #[wasm_bindgen(constructor)]
    pub fn new() -> RecachedCache {
        RecachedCache {
            store: Arc::new(KeyValueStore::new()),
            ws: None,
        }
    }

    /// Connect to the native Recached backend via WebSockets
    pub fn connect(&mut self, url: &str) -> Result<(), JsValue> {
        let ws = WebSocket::new(url)?;
        let store_clone = Arc::clone(&self.store);
        
        // Listen for incoming synced commands from the server
        let onmessage_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(text) = e.data().dyn_into::<js_sys::JsString>() {
                let text_str = String::from(text);
                let parts: Vec<&str> = text_str.splitn(3, ' ').collect();
                if !parts.is_empty() {
                    match parts[0].to_uppercase().as_str() {
                        "SET" if parts.len() == 3 => {
                            let cmd = Command::Set(parts[1].to_string(), parts[2].to_string());
                            store_clone.execute(cmd);
                        }
                        "DEL" if parts.len() == 2 => {
                            let cmd = Command::Del(vec![parts[1].to_string()]);
                            store_clone.execute(cmd);
                        }
                        _ => {}
                    }
                }
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
        onmessage_callback.forget(); // Keep the closure alive
        
        self.ws = Some(ws);
        Ok(())
    }

    /// Authenticate with the server if RECACHED_PASSWORD is set
    pub fn auth(&self, password: &str) -> String {
        if let Some(ws) = &self.ws {
            if ws.ready_state() == WebSocket::OPEN {
                let _ = ws.send_with_str(&format!("AUTH {}", password));
            }
        }
        "OK".to_string()
    }

    /// Set a key-value pair and sync to server
    pub fn set(&self, key: &str, value: &str) -> String {
        let cmd = Command::Set(key.to_string(), value.to_string());
        let resp = self.store.execute(cmd);
        
        // Sync to server if connected
        if let Some(ws) = &self.ws {
            if ws.ready_state() == WebSocket::OPEN {
                let _ = ws.send_with_str(&format!("SET {} {}", key, value));
            }
        }
        
        match resp {
            Value::SimpleString(s) => s,
            Value::Error(e) => e,
            _ => "ERR".to_string(),
        }
    }

    /// Get a value locally (zero latency)
    pub fn get(&self, key: &str) -> Option<String> {
        let cmd = Command::Get(key.to_string());
        let resp = self.store.execute(cmd);
        match resp {
            Value::BulkString(Some(data)) => Some(String::from_utf8_lossy(&data).to_string()),
            _ => None,
        }
    }

    /// Delete a key and sync to server
    pub fn del(&self, key: &str) -> i32 {
        let cmd = Command::Del(vec![key.to_string()]);
        let resp = self.store.execute(cmd);
        
        if let Some(ws) = &self.ws {
            if ws.ready_state() == WebSocket::OPEN {
                let _ = ws.send_with_str(&format!("DEL {}", key));
            }
        }
        
        match resp {
            Value::Integer(i) => i as i32,
            _ => 0,
        }
    }
}
