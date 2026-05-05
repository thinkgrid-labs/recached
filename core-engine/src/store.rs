use crate::cmd::{Command, SetCondition, SetExpiry};
use crate::resp::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct Entry {
    value: String,
    expires_at_ms: Option<u64>,
}

impl Entry {
    fn new(value: String) -> Self {
        Self { value, expires_at_ms: None }
    }

    fn with_expiry(value: String, expires_at_ms: u64) -> Self {
        Self { value, expires_at_ms: Some(expires_at_ms) }
    }

    fn is_expired(&self, now: u64) -> bool {
        self.expires_at_ms.map_or(false, |exp| now >= exp)
    }
}

#[derive(Clone)]
pub struct KeyValueStore {
    data: Arc<RwLock<HashMap<String, Entry>>>,
    max_keys: Option<usize>,
}

impl Default for KeyValueStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueStore {
    pub fn new() -> Self {
        Self { data: Arc::new(RwLock::new(HashMap::new())), max_keys: None }
    }

    pub fn with_max_keys(max: usize) -> Self {
        Self { data: Arc::new(RwLock::new(HashMap::new())), max_keys: Some(max) }
    }

    /// Removes all expired keys. Called periodically by the native server.
    pub fn sweep_expired(&self) {
        let now = now_ms();
        let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
        lock.retain(|_, entry| !entry.is_expired(now));
    }

    /// Executes a Command and returns the RESP Value to respond with.
    pub fn execute(&self, cmd: Command) -> Value {
        match cmd {
            Command::Ping(msg) => match msg {
                Some(m) => Value::BulkString(Some(m.into_bytes())),
                None => Value::SimpleString("PONG".to_string()),
            },

            Command::Auth(_) => Value::SimpleString("OK".to_string()),

            Command::Set(key, val, opts) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());

                let (existing_val, existing_ttl) = match lock.get(&key) {
                    Some(e) if !e.is_expired(now) => (Some(e.value.clone()), e.expires_at_ms),
                    _ => (None, None),
                };
                let key_exists = existing_val.is_some();

                let condition_met = match &opts.condition {
                    Some(SetCondition::Nx) => !key_exists,
                    Some(SetCondition::Xx) => key_exists,
                    None => true,
                };

                if !condition_met {
                    return if opts.get {
                        existing_val
                            .map(|v| Value::BulkString(Some(v.into_bytes())))
                            .unwrap_or(Value::BulkString(None))
                    } else {
                        Value::BulkString(None)
                    };
                }

                if let Some(max) = self.max_keys
                    && lock.len() >= max
                    && !lock.contains_key(&key)
                {
                    return Value::Error("ERR max keys limit reached".to_string());
                }

                let expires_at_ms = match &opts.expiry {
                    None => None,
                    Some(SetExpiry::Ex(s)) => Some(now.saturating_add(s.saturating_mul(1000))),
                    Some(SetExpiry::Px(ms)) => Some(now.saturating_add(*ms)),
                    Some(SetExpiry::Exat(ts)) => Some(ts.saturating_mul(1000)),
                    Some(SetExpiry::Pxat(ts_ms)) => Some(*ts_ms),
                    Some(SetExpiry::KeepTtl) => existing_ttl,
                };

                lock.insert(key, Entry { value: val, expires_at_ms });

                if opts.get {
                    existing_val
                        .map(|v| Value::BulkString(Some(v.into_bytes())))
                        .unwrap_or(Value::BulkString(None))
                } else {
                    Value::SimpleString("OK".to_string())
                }
            }

            Command::Get(key) => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                match lock.get(&key) {
                    Some(e) if !e.is_expired(now) => {
                        Value::BulkString(Some(e.value.clone().into_bytes()))
                    }
                    _ => Value::BulkString(None),
                }
            }

            Command::Del(keys) | Command::Unlink(keys) => {
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                let count = keys.into_iter().filter(|k| lock.remove(k).is_some()).count();
                Value::Integer(count as i64)
            }

            Command::Append(key, suffix) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                let entry = lock.entry(key).or_insert_with(|| Entry::new(String::new()));
                if entry.is_expired(now) {
                    entry.value = String::new();
                    entry.expires_at_ms = None;
                }
                entry.value.push_str(&suffix);
                Value::Integer(entry.value.len() as i64)
            }

            Command::Strlen(key) => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                match lock.get(&key) {
                    Some(e) if !e.is_expired(now) => Value::Integer(e.value.len() as i64),
                    _ => Value::Integer(0),
                }
            }

            Command::GetSet(key, new_val) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                let old = lock
                    .get(&key)
                    .filter(|e| !e.is_expired(now))
                    .map(|e| Value::BulkString(Some(e.value.clone().into_bytes())))
                    .unwrap_or(Value::BulkString(None));
                lock.insert(key, Entry::new(new_val));
                old
            }

            Command::MGet(keys) => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                let results = keys
                    .iter()
                    .map(|k| match lock.get(k) {
                        Some(e) if !e.is_expired(now) => {
                            Value::BulkString(Some(e.value.clone().into_bytes()))
                        }
                        _ => Value::BulkString(None),
                    })
                    .collect();
                Value::Array(Some(results))
            }

            Command::MSet(pairs) => {
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                if let Some(max) = self.max_keys {
                    let new_count =
                        pairs.iter().filter(|(k, _)| !lock.contains_key(k)).count();
                    if lock.len() + new_count > max {
                        return Value::Error("ERR max keys limit reached".to_string());
                    }
                }
                for (k, v) in pairs {
                    lock.insert(k, Entry::new(v));
                }
                Value::SimpleString("OK".to_string())
            }

            Command::SetNx(key, val) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                let exists = lock.get(&key).map_or(false, |e| !e.is_expired(now));
                if exists {
                    return Value::Integer(0);
                }
                if let Some(max) = self.max_keys
                    && lock.len() >= max
                    && !lock.contains_key(&key)
                {
                    return Value::Error("ERR max keys limit reached".to_string());
                }
                lock.insert(key, Entry::new(val));
                Value::Integer(1)
            }

            Command::SetEx(key, secs, val) => {
                let now = now_ms();
                let expires_at_ms = now.saturating_add(secs.saturating_mul(1000));
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                if let Some(max) = self.max_keys
                    && lock.len() >= max
                    && !lock.contains_key(&key)
                {
                    return Value::Error("ERR max keys limit reached".to_string());
                }
                lock.insert(key, Entry::with_expiry(val, expires_at_ms));
                Value::SimpleString("OK".to_string())
            }

            Command::PSetEx(key, ms, val) => {
                let now = now_ms();
                let expires_at_ms = now.saturating_add(ms);
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                if let Some(max) = self.max_keys
                    && lock.len() >= max
                    && !lock.contains_key(&key)
                {
                    return Value::Error("ERR max keys limit reached".to_string());
                }
                lock.insert(key, Entry::with_expiry(val, expires_at_ms));
                Value::SimpleString("OK".to_string())
            }

            Command::Incr(key) => incr_by(&self.data, key, 1),
            Command::Decr(key) => incr_by(&self.data, key, -1),
            Command::IncrBy(key, delta) => incr_by(&self.data, key, delta),
            Command::DecrBy(key, delta) => incr_by(&self.data, key, -delta),

            Command::Expire(key, secs) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                match lock.get_mut(&key) {
                    Some(e) if !e.is_expired(now) => {
                        e.expires_at_ms = Some(now.saturating_add(secs.saturating_mul(1000)));
                        Value::Integer(1)
                    }
                    _ => Value::Integer(0),
                }
            }

            Command::PExpire(key, ms) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                match lock.get_mut(&key) {
                    Some(e) if !e.is_expired(now) => {
                        e.expires_at_ms = Some(now.saturating_add(ms));
                        Value::Integer(1)
                    }
                    _ => Value::Integer(0),
                }
            }

            Command::ExpireAt(key, ts_secs) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                match lock.get_mut(&key) {
                    Some(e) if !e.is_expired(now) => {
                        e.expires_at_ms = Some(ts_secs.saturating_mul(1000));
                        Value::Integer(1)
                    }
                    _ => Value::Integer(0),
                }
            }

            Command::PExpireAt(key, ts_ms) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                match lock.get_mut(&key) {
                    Some(e) if !e.is_expired(now) => {
                        e.expires_at_ms = Some(ts_ms);
                        Value::Integer(1)
                    }
                    _ => Value::Integer(0),
                }
            }

            Command::Ttl(key) => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                match lock.get(&key) {
                    None => Value::Integer(-2),
                    Some(e) if e.is_expired(now) => Value::Integer(-2),
                    Some(e) => match e.expires_at_ms {
                        None => Value::Integer(-1),
                        Some(exp) => Value::Integer(((exp - now) / 1000) as i64),
                    },
                }
            }

            Command::PTtl(key) => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                match lock.get(&key) {
                    None => Value::Integer(-2),
                    Some(e) if e.is_expired(now) => Value::Integer(-2),
                    Some(e) => match e.expires_at_ms {
                        None => Value::Integer(-1),
                        Some(exp) => Value::Integer((exp - now) as i64),
                    },
                }
            }

            Command::Persist(key) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                match lock.get_mut(&key) {
                    Some(e) if !e.is_expired(now) && e.expires_at_ms.is_some() => {
                        e.expires_at_ms = None;
                        Value::Integer(1)
                    }
                    Some(e) if !e.is_expired(now) => Value::Integer(0),
                    _ => Value::Integer(0),
                }
            }

            Command::Exists(keys) => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                let count = keys
                    .iter()
                    .filter(|k| lock.get(*k).map_or(false, |e| !e.is_expired(now)))
                    .count();
                Value::Integer(count as i64)
            }

            Command::Keys(pattern) => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                let mut keys: Vec<Value> = lock
                    .iter()
                    .filter(|(k, e)| !e.is_expired(now) && glob_match(&pattern, k))
                    .map(|(k, _)| Value::BulkString(Some(k.as_bytes().to_vec())))
                    .collect();
                keys.sort_unstable_by(|a, b| {
                    let ka = if let Value::BulkString(Some(d)) = a { d.as_slice() } else { &[] };
                    let kb = if let Value::BulkString(Some(d)) = b { d.as_slice() } else { &[] };
                    ka.cmp(kb)
                });
                Value::Array(Some(keys))
            }

            Command::Scan(cursor, pattern, _count) => {
                if cursor != 0 {
                    return Value::Array(Some(vec![
                        Value::BulkString(Some(b"0".to_vec())),
                        Value::Array(Some(vec![])),
                    ]));
                }
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                let pat = pattern.as_deref().unwrap_or("*");
                let keys: Vec<Value> = lock
                    .iter()
                    .filter(|(k, e)| !e.is_expired(now) && glob_match(pat, k))
                    .map(|(k, _)| Value::BulkString(Some(k.as_bytes().to_vec())))
                    .collect();
                Value::Array(Some(vec![
                    Value::BulkString(Some(b"0".to_vec())),
                    Value::Array(Some(keys)),
                ]))
            }

            Command::DbSize => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                let count = lock.values().filter(|e| !e.is_expired(now)).count();
                Value::Integer(count as i64)
            }

            Command::FlushDb => {
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                lock.clear();
                Value::SimpleString("OK".to_string())
            }

            Command::Rename(src, dst) => {
                let now = now_ms();
                let mut lock = self.data.write().unwrap_or_else(|e| e.into_inner());
                match lock.remove(&src) {
                    None => Value::Error("ERR no such key".to_string()),
                    Some(e) if e.is_expired(now) => Value::Error("ERR no such key".to_string()),
                    Some(entry) => {
                        lock.insert(dst, entry);
                        Value::SimpleString("OK".to_string())
                    }
                }
            }

            Command::Type(key) => {
                let now = now_ms();
                let lock = self.data.read().unwrap_or_else(|e| e.into_inner());
                match lock.get(&key) {
                    Some(e) if !e.is_expired(now) => {
                        Value::SimpleString("string".to_string())
                    }
                    _ => Value::SimpleString("none".to_string()),
                }
            }

            Command::Unknown(name) => Value::Error(format!("ERR unknown command '{}'", name)),
        }
    }
}

fn incr_by(data: &Arc<RwLock<HashMap<String, Entry>>>, key: String, delta: i64) -> Value {
    let now = now_ms();
    let mut lock = data.write().unwrap_or_else(|e| e.into_inner());
    let entry = lock.entry(key).or_insert_with(|| Entry::new("0".to_string()));
    if entry.is_expired(now) {
        entry.value = "0".to_string();
        entry.expires_at_ms = None;
    }
    match entry.value.parse::<i64>() {
        Err(_) => Value::Error("ERR value is not an integer or out of range".to_string()),
        Ok(current) => match current.checked_add(delta) {
            None => Value::Error("ERR increment or decrement would overflow".to_string()),
            Some(new_val) => {
                entry.value = new_val.to_string();
                Value::Integer(new_val)
            }
        },
    }
}

fn glob_match(pattern: &str, s: &str) -> bool {
    glob_helper(pattern.as_bytes(), s.as_bytes())
}

fn glob_helper(p: &[u8], s: &[u8]) -> bool {
    match (p.first(), s.first()) {
        (None, None) => true,
        (None, _) => false,
        (Some(&b'*'), None) => glob_helper(&p[1..], s),
        (Some(&b'*'), _) => glob_helper(&p[1..], s) || glob_helper(p, &s[1..]),
        (_, None) => false,
        (Some(&b'?'), _) => glob_helper(&p[1..], &s[1..]),
        (Some(&pc), Some(&sc)) if pc == sc => glob_helper(&p[1..], &s[1..]),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let store = KeyValueStore::new();
        store.execute(Command::Set(
            "k".to_string(),
            "v".to_string(),
            Default::default(),
        ));
        assert_eq!(
            store.execute(Command::Get("k".to_string())),
            Value::BulkString(Some(b"v".to_vec()))
        );
    }

    #[test]
    fn get_missing() {
        let store = KeyValueStore::new();
        assert_eq!(store.execute(Command::Get("x".to_string())), Value::BulkString(None));
    }

    #[test]
    fn del_returns_count() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("a".to_string(), "1".to_string(), Default::default()));
        store.execute(Command::Set("b".to_string(), "2".to_string(), Default::default()));
        assert_eq!(
            store.execute(Command::Del(vec!["a".to_string(), "b".to_string(), "z".to_string()])),
            Value::Integer(2)
        );
    }

    #[test]
    fn set_ex_expires() {
        let store = KeyValueStore::new();
        // Use PX 1 so it expires after 1ms
        let opts = crate::cmd::SetOptions {
            expiry: Some(SetExpiry::Px(1)),
            ..Default::default()
        };
        store.execute(Command::Set("k".to_string(), "v".to_string(), opts));
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(store.execute(Command::Get("k".to_string())), Value::BulkString(None));
    }

    #[test]
    fn ttl_no_expiry() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("k".to_string(), "v".to_string(), Default::default()));
        assert_eq!(store.execute(Command::Ttl("k".to_string())), Value::Integer(-1));
    }

    #[test]
    fn ttl_missing_key() {
        let store = KeyValueStore::new();
        assert_eq!(store.execute(Command::Ttl("x".to_string())), Value::Integer(-2));
    }

    #[test]
    fn ttl_with_expiry() {
        let store = KeyValueStore::new();
        store.execute(Command::SetEx("k".to_string(), 100, "v".to_string()));
        match store.execute(Command::Ttl("k".to_string())) {
            Value::Integer(n) => assert!(n > 90 && n <= 100, "TTL out of range: {}", n),
            other => panic!("expected integer, got {:?}", other),
        }
    }

    #[test]
    fn persist_removes_ttl() {
        let store = KeyValueStore::new();
        store.execute(Command::SetEx("k".to_string(), 60, "v".to_string()));
        assert_eq!(store.execute(Command::Persist("k".to_string())), Value::Integer(1));
        assert_eq!(store.execute(Command::Ttl("k".to_string())), Value::Integer(-1));
    }

    #[test]
    fn incr_from_zero() {
        let store = KeyValueStore::new();
        assert_eq!(store.execute(Command::Incr("c".to_string())), Value::Integer(1));
        assert_eq!(store.execute(Command::Incr("c".to_string())), Value::Integer(2));
    }

    #[test]
    fn incr_existing() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("c".to_string(), "10".to_string(), Default::default()));
        assert_eq!(store.execute(Command::IncrBy("c".to_string(), 5)), Value::Integer(15));
    }

    #[test]
    fn incr_non_integer() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("k".to_string(), "foo".to_string(), Default::default()));
        assert!(matches!(store.execute(Command::Incr("k".to_string())), Value::Error(_)));
    }

    #[test]
    fn mget_mset() {
        let store = KeyValueStore::new();
        store.execute(Command::MSet(vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]));
        assert_eq!(
            store.execute(Command::MGet(vec!["a".to_string(), "b".to_string(), "c".to_string()])),
            Value::Array(Some(vec![
                Value::BulkString(Some(b"1".to_vec())),
                Value::BulkString(Some(b"2".to_vec())),
                Value::BulkString(None),
            ]))
        );
    }

    #[test]
    fn setnx_only_sets_once() {
        let store = KeyValueStore::new();
        assert_eq!(store.execute(Command::SetNx("k".to_string(), "first".to_string())), Value::Integer(1));
        assert_eq!(store.execute(Command::SetNx("k".to_string(), "second".to_string())), Value::Integer(0));
        assert_eq!(
            store.execute(Command::Get("k".to_string())),
            Value::BulkString(Some(b"first".to_vec()))
        );
    }

    #[test]
    fn exists_counts_duplicates() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("a".to_string(), "1".to_string(), Default::default()));
        // EXISTS a a b — a exists twice, b doesn't: count = 2
        assert_eq!(
            store.execute(Command::Exists(vec!["a".to_string(), "a".to_string(), "b".to_string()])),
            Value::Integer(2)
        );
    }

    #[test]
    fn dbsize_counts_live_keys() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("a".to_string(), "1".to_string(), Default::default()));
        store.execute(Command::Set("b".to_string(), "2".to_string(), Default::default()));
        assert_eq!(store.execute(Command::DbSize), Value::Integer(2));
    }

    #[test]
    fn flushdb_clears_all() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("a".to_string(), "1".to_string(), Default::default()));
        store.execute(Command::FlushDb);
        assert_eq!(store.execute(Command::DbSize), Value::Integer(0));
    }

    #[test]
    fn rename_moves_key() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("src".to_string(), "hello".to_string(), Default::default()));
        store.execute(Command::Rename("src".to_string(), "dst".to_string()));
        assert_eq!(store.execute(Command::Get("src".to_string())), Value::BulkString(None));
        assert_eq!(
            store.execute(Command::Get("dst".to_string())),
            Value::BulkString(Some(b"hello".to_vec()))
        );
    }

    #[test]
    fn rename_missing_key_errors() {
        let store = KeyValueStore::new();
        assert!(matches!(
            store.execute(Command::Rename("nope".to_string(), "dst".to_string())),
            Value::Error(_)
        ));
    }

    #[test]
    fn type_returns_string_or_none() {
        let store = KeyValueStore::new();
        assert_eq!(
            store.execute(Command::Type("x".to_string())),
            Value::SimpleString("none".to_string())
        );
        store.execute(Command::Set("x".to_string(), "v".to_string(), Default::default()));
        assert_eq!(
            store.execute(Command::Type("x".to_string())),
            Value::SimpleString("string".to_string())
        );
    }

    #[test]
    fn keys_glob_star() {
        let store = KeyValueStore::new();
        store.execute(Command::Set("user:1".to_string(), "a".to_string(), Default::default()));
        store.execute(Command::Set("user:2".to_string(), "b".to_string(), Default::default()));
        store.execute(Command::Set("session:1".to_string(), "c".to_string(), Default::default()));
        match store.execute(Command::Keys("user:*".to_string())) {
            Value::Array(Some(keys)) => {
                assert_eq!(keys.len(), 2);
            }
            other => panic!("expected array, got {:?}", other),
        }
    }

    #[test]
    fn glob_question_mark() {
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
    }

    #[test]
    fn glob_star() {
        assert!(glob_match("h*llo", "hello"));
        assert!(glob_match("h*llo", "heeello"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("h*llo", "world"));
    }

    #[test]
    fn set_nx_condition() {
        let store = KeyValueStore::new();
        let opts_nx = crate::cmd::SetOptions {
            condition: Some(SetCondition::Nx),
            ..Default::default()
        };
        // First SET NX should succeed
        assert_eq!(
            store.execute(Command::Set("k".to_string(), "first".to_string(), opts_nx.clone())),
            Value::SimpleString("OK".to_string())
        );
        // Second SET NX should fail (key exists)
        assert_eq!(
            store.execute(Command::Set("k".to_string(), "second".to_string(), opts_nx)),
            Value::BulkString(None)
        );
        assert_eq!(
            store.execute(Command::Get("k".to_string())),
            Value::BulkString(Some(b"first".to_vec()))
        );
    }

    #[test]
    fn sweep_removes_expired() {
        let store = KeyValueStore::new();
        let opts = crate::cmd::SetOptions {
            expiry: Some(SetExpiry::Px(1)),
            ..Default::default()
        };
        store.execute(Command::Set("k".to_string(), "v".to_string(), opts));
        std::thread::sleep(std::time::Duration::from_millis(5));
        store.sweep_expired();
        // After sweep, DBSIZE should be 0
        assert_eq!(store.execute(Command::DbSize), Value::Integer(0));
    }
}
