use core_engine::cmd::Command;
use core_engine::resp::Value;
use core_engine::store::KeyValueStore;
use futures_util::{SinkExt, StreamExt};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Semaphore};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

// ── tunables ────────────────────────────────────────────────────────────────

const TCP_READ_BUFFER_BYTES: usize = 4096;
const BROADCAST_CHANNEL_CAPACITY: usize = 512;
const MAX_CONNECTIONS: usize = 1024;
const MAX_AUTH_FAILURES: u32 = 5;

// ── connection identity ──────────────────────────────────────────────────────

// TCP clients broadcast with id=0; WS clients get ids ≥ 1.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

fn next_conn_id() -> u64 {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Encodes a list of string parts as a RESP bulk-string array.
fn resp_command(parts: &[&str]) -> String {
    let mut s = format!("*{}\r\n", parts.len());
    for part in parts {
        s.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    s
}

/// Handles an AUTH attempt. Returns `(disconnect, resp_bytes)`.
///
/// `disconnect` is true when the failure count hits MAX_AUTH_FAILURES.
fn process_auth(
    provided: &str,
    expected: &Arc<Option<String>>,
    is_authenticated: &mut bool,
    failures: &mut u32,
) -> (bool, Vec<u8>) {
    match expected.as_ref() {
        Some(pwd) if provided == pwd => {
            *is_authenticated = true;
            *failures = 0;
            (false, b"+OK\r\n".to_vec())
        }
        Some(_) => {
            *failures += 1;
            if *failures >= MAX_AUTH_FAILURES {
                (true, b"-ERR too many authentication failures\r\n".to_vec())
            } else {
                (false, b"-ERR invalid password\r\n".to_vec())
            }
        }
        None => (
            false,
            b"-ERR Client sent AUTH, but no password is set\r\n".to_vec(),
        ),
    }
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // ── auth ──────────────────────────────────────────────────────────────
    let password = std::env::var("RECACHED_PASSWORD").ok();
    let global_password = Arc::new(password);

    if global_password.is_some() {
        info!("Authentication ENABLED. Clients must send 'AUTH <password>'.");
    } else {
        warn!("Authentication DISABLED. Set RECACHED_PASSWORD to enable.");
    }

    // ── IP allowlist ──────────────────────────────────────────────────────
    let allowed_ips: Option<Arc<Vec<IpAddr>>> = std::env::var("RECACHED_ALLOW_IPS").ok().map(|s| {
        let ips: Vec<IpAddr> = s
            .split(',')
            .filter_map(|raw| {
                let trimmed = raw.trim();
                match IpAddr::from_str(trimmed) {
                    Ok(ip) => Some(ip),
                    Err(_) => {
                        warn!("RECACHED_ALLOW_IPS: ignoring invalid entry '{}'", trimmed);
                        None
                    }
                }
            })
            .collect();
        Arc::new(ips)
    });

    if let Some(ips) = &allowed_ips {
        info!("IP allowlist ENABLED: {:?}", ips);
    } else {
        warn!("IP allowlist DISABLED. Accepting all connections.");
    }

    // ── store ─────────────────────────────────────────────────────────────
    let max_keys = std::env::var("RECACHED_MAX_KEYS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());

    let store = Arc::new(match max_keys {
        Some(n) => {
            info!("Key limit set to {}", n);
            KeyValueStore::with_max_keys(n)
        }
        None => KeyValueStore::new(),
    });

    // ── broadcast channel ─────────────────────────────────────────────────
    // Carries (sender_conn_id, resp_encoded_mutation). WS receivers skip their own messages.
    let (tx, _rx) = broadcast::channel::<(u64, String)>(BROADCAST_CHANNEL_CAPACITY);

    // ── connection limiter ────────────────────────────────────────────────
    let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));

    // ── listeners ─────────────────────────────────────────────────────────
    let tcp_listener = TcpListener::bind("127.0.0.1:6379").await?;
    info!("TCP server listening on 127.0.0.1:6379");

    let ws_listener = TcpListener::bind("127.0.0.1:6380").await?;
    info!("WebSocket server listening on 127.0.0.1:6380");

    let store_tcp = Arc::clone(&store);
    let tx_tcp = tx.clone();
    let pass_tcp = Arc::clone(&global_password);
    let allowed_tcp = allowed_ips.clone();
    let sem_tcp = Arc::clone(&semaphore);

    tokio::spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Ok((socket, addr)) => {
                    if let Some(allowed) = &allowed_tcp {
                        if !allowed.contains(&addr.ip()) {
                            debug!("TCP: rejected IP {}", addr.ip());
                            continue;
                        }
                    }
                    let permit = match Arc::clone(&sem_tcp).try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            warn!("TCP: connection limit reached, dropping {}", addr);
                            continue;
                        }
                    };
                    let s = Arc::clone(&store_tcp);
                    let t = tx_tcp.clone();
                    let p = Arc::clone(&pass_tcp);
                    tokio::spawn(async move {
                        let _permit = permit;
                        handle_tcp(socket, s, t, p).await;
                    });
                }
                Err(e) => warn!("TCP accept error: {}", e),
            }
        }
    });

    loop {
        match ws_listener.accept().await {
            Ok((socket, addr)) => {
                if let Some(allowed) = &allowed_ips {
                    if !allowed.contains(&addr.ip()) {
                        debug!("WS: rejected IP {}", addr.ip());
                        continue;
                    }
                }
                let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("WS: connection limit reached, dropping {}", addr);
                        continue;
                    }
                };
                let s = Arc::clone(&store);
                let t = tx.clone();
                let p = Arc::clone(&global_password);
                let id = next_conn_id();
                tokio::spawn(async move {
                    let _permit = permit;
                    handle_ws(socket, s, t, p, id).await;
                });
            }
            Err(e) => warn!("WS accept error: {}", e),
        }
    }
}

// ── TCP handler ───────────────────────────────────────────────────────────────

async fn handle_tcp(
    mut socket: TcpStream,
    store: Arc<KeyValueStore>,
    tx: broadcast::Sender<(u64, String)>,
    password: Arc<Option<String>>,
) {
    let mut buf = Vec::<u8>::new();
    let mut read_buf = [0u8; TCP_READ_BUFFER_BYTES];
    let mut is_authenticated = password.is_none();
    let mut auth_failures: u32 = 0;

    loop {
        match socket.read(&mut read_buf).await {
            Ok(0) => break, // clean disconnect
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);

                // Drain all complete RESP messages from the buffer.
                loop {
                    match Value::parse(&buf) {
                        Ok((value, consumed)) => {
                            buf.drain(..consumed);

                            let cmd = match Command::from_value(value) {
                                Ok(c) => c,
                                Err(e) => {
                                    let resp = Value::Error(e).serialize();
                                    if let Err(we) = socket.write_all(&resp).await {
                                        warn!("TCP write error: {}", we);
                                    }
                                    continue;
                                }
                            };

                            // Auth interceptor
                            if let Command::Auth(ref pwd) = cmd {
                                let (disconnect, resp) = process_auth(
                                    pwd,
                                    &password,
                                    &mut is_authenticated,
                                    &mut auth_failures,
                                );
                                if let Err(e) = socket.write_all(&resp).await {
                                    warn!("TCP write error: {}", e);
                                }
                                if disconnect {
                                    return;
                                }
                                continue;
                            }

                            if !is_authenticated {
                                if let Err(e) = socket
                                    .write_all(b"-NOAUTH Authentication required.\r\n")
                                    .await
                                {
                                    warn!("TCP write error: {}", e);
                                }
                                continue;
                            }

                            // Build RESP-encoded broadcast message before executing.
                            let broadcast_msg = match &cmd {
                                Command::Set(k, v) => Some(resp_command(&["SET", k, v])),
                                Command::Del(keys) => {
                                    let mut parts = vec!["DEL"];
                                    let key_refs: Vec<&str> =
                                        keys.iter().map(|s| s.as_str()).collect();
                                    parts.extend_from_slice(&key_refs);
                                    Some(resp_command(&parts))
                                }
                                _ => None,
                            };

                            // Broadcast first so peers are notified before local state advances.
                            if let Some(ref msg) = broadcast_msg {
                                if let Err(e) = tx.send((0, msg.clone())) {
                                    debug!("TCP broadcast had no WS receivers: {}", e);
                                }
                            }

                            let response = store.execute(cmd);
                            if let Err(e) = socket.write_all(&response.serialize()).await {
                                warn!("TCP write error: {}", e);
                                return;
                            }
                        }
                        Err(ref e) if e == "Incomplete" => break, // need more bytes
                        Err(e) => {
                            warn!("TCP protocol error: {}", e);
                            if let Err(we) = socket.write_all(b"-ERR Protocol error\r\n").await {
                                warn!("TCP write error: {}", we);
                            }
                            buf.clear();
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("TCP read error: {}", e);
                break;
            }
        }
    }
}

// ── WebSocket handler ─────────────────────────────────────────────────────────

async fn handle_ws(
    socket: TcpStream,
    store: Arc<KeyValueStore>,
    tx: broadcast::Sender<(u64, String)>,
    password: Arc<Option<String>>,
    conn_id: u64,
) {
    let ws_stream = match accept_async(socket).await {
        Ok(ws) => ws,
        Err(e) => {
            warn!("WS handshake failed on conn {}: {}", conn_id, e);
            return;
        }
    };

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let mut rx = tx.subscribe();
    let mut is_authenticated = password.is_none();
    let mut auth_failures: u32 = 0;

    loop {
        tokio::select! {
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let (value, _) = match Value::parse(text.as_bytes()) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("WS RESP parse error on conn {}: {}", conn_id, e);
                                let err = Value::Error(format!("ERR Protocol error: {}", e)).serialize();
                                if let Err(we) = ws_sender.send(Message::Text(
                                    String::from_utf8_lossy(&err).into_owned().into(),
                                )).await {
                                    warn!("WS send error on conn {}: {}", conn_id, we);
                                    break;
                                }
                                continue;
                            }
                        };

                        let cmd = match Command::from_value(value) {
                            Ok(c) => c,
                            Err(e) => {
                                let err = Value::Error(e).serialize();
                                if let Err(we) = ws_sender.send(Message::Text(
                                    String::from_utf8_lossy(&err).into_owned().into(),
                                )).await {
                                    warn!("WS send error on conn {}: {}", conn_id, we);
                                    break;
                                }
                                continue;
                            }
                        };

                        if let Command::Auth(ref pwd) = cmd {
                            let (disconnect, resp) = process_auth(
                                pwd,
                                &password,
                                &mut is_authenticated,
                                &mut auth_failures,
                            );
                            if let Err(e) = ws_sender.send(Message::Text(
                                String::from_utf8_lossy(&resp).into_owned().into(),
                            )).await {
                                warn!("WS send error on conn {}: {}", conn_id, e);
                                break;
                            }
                            if disconnect {
                                break;
                            }
                            continue;
                        }

                        if !is_authenticated {
                            let resp = Value::Error("NOAUTH Authentication required.".to_string()).serialize();
                            if let Err(e) = ws_sender.send(Message::Text(
                                String::from_utf8_lossy(&resp).into_owned().into(),
                            )).await {
                                warn!("WS send error on conn {}: {}", conn_id, e);
                                break;
                            }
                            continue;
                        }

                        // Re-encode the validated mutation as the canonical broadcast payload.
                        let broadcast_msg = match &cmd {
                            Command::Set(k, v) => Some(resp_command(&["SET", k, v])),
                            Command::Del(keys) => {
                                let mut parts = vec!["DEL"];
                                let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                                parts.extend_from_slice(&key_refs);
                                Some(resp_command(&parts))
                            }
                            _ => None,
                        };

                        if let Some(ref b_msg) = broadcast_msg {
                            if let Err(e) = tx.send((conn_id, b_msg.clone())) {
                                debug!("WS broadcast on conn {} had no receivers: {}", conn_id, e);
                            }
                        }

                        let response = store.execute(cmd);
                        if let Err(e) = ws_sender.send(Message::Text(
                            String::from_utf8_lossy(&response.serialize()).into_owned().into(),
                        )).await {
                            warn!("WS send error on conn {}: {}", conn_id, e);
                            break;
                        }
                    }
                    Some(Ok(_)) => {} // Ping / Pong / Close handled by tungstenite
                    Some(Err(e)) => {
                        warn!("WS error on conn {}: {}", conn_id, e);
                        break;
                    }
                    None => break,
                }
            }
            result = rx.recv() => {
                match result {
                    Ok((sender_id, msg)) if sender_id != conn_id => {
                        if let Err(e) = ws_sender.send(Message::Text(msg.into())).await {
                            warn!("WS broadcast send error on conn {}: {}", conn_id, e);
                            break;
                        }
                    }
                    Ok(_) => {} // own message — skip to avoid double-apply in wasm-edge
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WS conn {} lagged, missed {} messages, resubscribing", conn_id, n);
                        rx = tx.subscribe();
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}
