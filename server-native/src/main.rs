use core_engine::cmd::Command;
use core_engine::resp::Value;
use core_engine::store::KeyValueStore;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read optional password from environment variable
    let password = std::env::var("RECACHED_PASSWORD").ok();
    let global_password = Arc::new(password);

    if global_password.is_some() {
        println!("🔒 Authentication is ENABLED. Clients must send 'AUTH <password>'.");
    } else {
        println!("⚠️ Authentication is DISABLED. (Set RECACHED_PASSWORD to enable).");
    }

    let allowed_ips: Option<Arc<Vec<String>>> = std::env::var("RECACHED_ALLOW_IPS")
        .ok()
        .map(|s| Arc::new(s.split(',').map(|ip| ip.trim().to_string()).collect()));

    if let Some(ips) = &allowed_ips {
        println!("🛡️  IP Allowlist is ENABLED. Authorized IPs: {:?}", ips);
    } else {
        println!("⚠️  IP Allowlist is DISABLED. Accepting all connections to localhost.");
    }

    let store = Arc::new(KeyValueStore::new());
    let (tx, _rx) = broadcast::channel::<String>(100);

    let tcp_listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("🔥 Recached TCP Server listening on port 6379...");

    let ws_listener = TcpListener::bind("127.0.0.1:6380").await?;
    println!("🌐 Recached WebSocket Server listening on port 6380...");

    let store_tcp = Arc::clone(&store);
    let tx_tcp = tx.clone();
    let pass_tcp = Arc::clone(&global_password);
    let allowed_tcp = allowed_ips.clone();

    // Spawn the traditional TCP listener thread
    tokio::spawn(async move {
        loop {
            if let Ok((socket, addr)) = tcp_listener.accept().await {
                if let Some(allowed) = &allowed_tcp {
                    if !allowed.contains(&addr.ip().to_string()) {
                        continue; // Drop unauthorized IP instantly
                    }
                }
                let s = Arc::clone(&store_tcp);
                let t = tx_tcp.clone();
                let p = Arc::clone(&pass_tcp);
                tokio::spawn(handle_tcp(socket, s, t, p));
            }
        }
    });

    // Run the WebSocket listener on the main thread
    loop {
        if let Ok((socket, addr)) = ws_listener.accept().await {
            if let Some(allowed) = &allowed_ips {
                if !allowed.contains(&addr.ip().to_string()) {
                    continue; // Drop unauthorized IP instantly
                }
            }
            let s = Arc::clone(&store);
            let t = tx.clone();
            let p = Arc::clone(&global_password);
            tokio::spawn(handle_ws(socket, s, t, p));
        }
    }
}

async fn handle_tcp(
    mut socket: TcpStream,
    store: Arc<KeyValueStore>,
    tx: broadcast::Sender<String>,
    password: Arc<Option<String>>,
) {
    let mut buffer = [0; 4096];
    let mut is_authenticated = password.is_none();

    loop {
        match socket.read(&mut buffer).await {
            Ok(0) => break, // Client disconnected
            Ok(n) => {
                let input = &buffer[0..n];
                if let Ok((value, _)) = Value::parse(input) {
                    if let Ok(cmd) = Command::from_value(value) {
                        // Authentication Interceptor
                        if let Command::Auth(pwd) = &cmd {
                            if let Some(expected) = &*password {
                                if pwd == expected {
                                    is_authenticated = true;
                                    let _ = socket.write_all(b"+OK\r\n").await;
                                } else {
                                    let _ = socket.write_all(b"-ERR invalid password\r\n").await;
                                }
                            } else {
                                let _ = socket
                                    .write_all(b"-ERR Client sent AUTH, but no password is set\r\n")
                                    .await;
                            }
                            continue;
                        }

                        // Block unauthorized commands
                        if !is_authenticated {
                            let _ = socket
                                .write_all(b"-NOAUTH Authentication required.\r\n")
                                .await;
                            continue;
                        }

                        // Extract command info before executing to broadcast it
                        let broadcast_msg = match &cmd {
                            Command::Set(k, v) => Some(format!("SET {} {}", k, v)),
                            Command::Del(keys) => Some(format!("DEL {}", keys.join(" "))),
                            _ => None,
                        };

                        let response = store.execute(cmd);

                        // Broadcast if it was a mutation
                        if let Some(msg) = broadcast_msg {
                            let _ = tx.send(msg);
                        }

                        let _ = socket.write_all(&response.serialize()).await;
                    }
                }
            }
            Err(_) => break,
        }
    }
}

async fn handle_ws(
    socket: TcpStream,
    store: Arc<KeyValueStore>,
    tx: broadcast::Sender<String>,
    password: Arc<Option<String>>,
) {
    let ws_stream = match accept_async(socket).await {
        Ok(ws) => ws,
        Err(_) => return, // Failed WS handshake
    };

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let mut rx = tx.subscribe();
    let mut is_authenticated = password.is_none();

    loop {
        tokio::select! {
            // Receive commands from the browser
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let parts: Vec<&str> = text.splitn(3, ' ').collect();
                        if parts.is_empty() { continue; }

                        let mut broadcast_msg = None;
                        let cmd = match parts[0].to_uppercase().as_str() {
                            "AUTH" if parts.len() == 2 => {
                                Some(Command::Auth(parts[1].to_string()))
                            }
                            "SET" if parts.len() == 3 => {
                                broadcast_msg = Some(text.to_string());
                                Some(Command::Set(parts[1].to_string(), parts[2].to_string()))
                            }
                            "DEL" if parts.len() == 2 => {
                                broadcast_msg = Some(text.to_string());
                                Some(Command::Del(vec![parts[1].to_string()]))
                            }
                            _ => None,
                        };

                        if let Some(c) = cmd {
                            // Authentication Interceptor
                            if let Command::Auth(pwd) = &c {
                                if let Some(expected) = &*password {
                                    if pwd == expected {
                                        is_authenticated = true;
                                        let _ = ws_sender.send(Message::Text("OK".to_string())).await;
                                    } else {
                                        let _ = ws_sender.send(Message::Text("ERR invalid password".to_string())).await;
                                    }
                                } else {
                                    let _ = ws_sender.send(Message::Text("ERR Client sent AUTH, but no password is set".to_string())).await;
                                }
                                continue;
                            }

                            // Block unauthorized commands
                            if !is_authenticated {
                                let _ = ws_sender.send(Message::Text("NOAUTH Authentication required.".to_string())).await;
                                continue;
                            }

                            store.execute(c);
                            // Broadcast the change to all *other* clients
                            if let Some(b_msg) = broadcast_msg {
                                let _ = tx.send(b_msg);
                            }
                        }
                    }
                    _ => break, // Client disconnected or error
                }
            }
            // Receive broadcasted changes from the server channel
            Ok(msg) = rx.recv() => {
                // Echo the change back out to this WebSocket
                let _ = ws_sender.send(Message::Text(msg)).await;
            }
        }
    }
}
