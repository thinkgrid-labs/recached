<div align="center">
  <h1>Recached ⚡</h1>
  <p><b>The Blazing Fast, Multi-Core, Local-First Redis Alternative written in Rust.</b></p>
  
  <!-- Badges -->
  <a href="#"><img src="https://img.shields.io/badge/Language-Rust-orange.svg" alt="Rust"></a>
  <a href="#"><img src="https://img.shields.io/badge/Architecture-Multi--Core-blue.svg" alt="Multi-Core"></a>
  <a href="#"><img src="https://img.shields.io/badge/Ecosystem-WebAssembly-yellow.svg" alt="Wasm"></a>
  <a href="#"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT"></a>
</div>

---

**RECACHED** (*Rust-Engineered CACHE Daemon*) is a next-generation in-memory data store. It is designed to be a 100% drop-in replacement for Redis that solves the single-threaded bottleneck of traditional caches, while seamlessly extending the database directly into the browser via WebAssembly (Wasm).

Whether you are scaling massive backend infrastructure or building real-time, local-first web applications, Recached provides unmatched performance and developer experience.

> [!WARNING]
> **Status: Active Development**  
> Recached is currently in its MVP phase and is **not yet ready for production workloads**. The core architecture is completely functional, but many advanced Redis commands (like HashMaps, TTLs, and persistence) are still being implemented. Use it for prototyping, local-first web apps, and experimentation!

## 🚀 Key Features

- **Multi-Core by Default:** Traditional Redis is strictly single-threaded. Recached leverages Rust's `tokio` runtime to instantly spawn asynchronous threads for every connection, utilizing 100% of your CPU cores without complex clustering.
- **Drop-in Redis Replacement:** Speaks the standard RESP (REdis Serialization Protocol). You do not need to change a single line of your application code or install new client libraries.
- **Local-First WebAssembly (Wasm):** Recached compiles down to a lightweight `.wasm` package. Run the database locally inside the browser or on Edge networks (like Cloudflare Workers) with zero network latency.
- **Real-Time WebSocket Sync:** Features a dual-port architecture that broadcasts state changes instantly between the Native Server and Wasm browser clients.
- **Production-Ready Security:** Built-in IP Allowlisting and strict connection-level `AUTH` password tracking.

---

## 📦 Installation

Recached distributes as a single, dependency-free binary. 

### 1. Docker (Recommended for Production)
Deploy the ultra-lightweight Recached container securely:
```bash
docker run -p 6379:6379 -p 6380:6380 ghcr.io/thinkgrid-labs/recached:latest
```

### 2. Homebrew (macOS)
```bash
brew tap thinkgrid-labs/recached
brew install recached
```

### 3. Cargo (Rust Developers)
```bash
cargo install recached
recached-server
```

---

## 💻 How to Use Recached

Because of its decoupled architecture, Recached serves two completely different ecosystems perfectly:

### Use Case A: The Backend Developer
Stop worrying about single-thread bottlenecks. Just point your existing Redis clients to Recached.

```javascript
// Node.js Example using standard Redis libraries
import Redis from 'ioredis';

// Connects to Recached exactly like Redis!
const redis = new Redis('redis://127.0.0.1:6379');

await redis.set('user:1', 'John Doe');
console.log(await redis.get('user:1'));
```

### Use Case B: The Frontend / Edge Developer
Import the database directly into the browser. Achieve zero-latency reads with automatic background syncing to the server.

```javascript
import init, { RecachedCache } from 'recached-edge';

async function start() {
    await init(); // Initialize WebAssembly
    
    const cache = new RecachedCache();
    
    // Connect to the Recached Native Server via WebSockets
    cache.connect("ws://127.0.0.1:6380");

    // The cache is instantly available in local memory!
    cache.set("theme", "dark");
    console.log(cache.get("theme")); 
}
```

---

## 🔒 Security Configuration

Recached is secure by default (binding only to localhost). For production deployments, lock down your database using standard environment variables:

```bash
# Enforce database passwords and drop unauthorized IPs instantly
RECACHED_PASSWORD="super_secret_password" \
RECACHED_ALLOW_IPS="127.0.0.1,10.0.0.55" \
recached-server
```

---

## 🏗️ Internal Architecture

Recached is built as a highly decoupled Cargo workspace to enforce strict boundaries between the state machine and the network:

1. **`core-engine`**: A strictly `no_std`-compatible, zero-dependency data store. Features a custom, zero-copy RESP parser and a highly concurrent `Arc<RwLock>` Key-Value store.
2. **`server-native`**: The multi-threaded TCP and WebSocket backend that wraps the core engine.
3. **`wasm-edge`**: The Javascript bindings that compile the core engine into the browser.

## 🗺️ Roadmap

Recached is actively evolving. Here are the features planned for upcoming releases:

- **Advanced Data Types:** Full support for HashMaps (`HSET`, `HGET`), Lists, and Sets.
- **Data Persistence & TTL:** Key expiration capabilities (`EXPIRE`) and background snapshotting to disk (`BGSAVE`) for disaster recovery.
- **Extreme Optimizations:** Migrating the core engine from `RwLock` to a sharded `DashMap` to maximize performance on highly-concurrent, write-heavy workloads.

## 🤝 Contributing
Recached is an open-source initiative. We welcome pull requests for new RESP commands (HashMaps, Lists, TTLs), performance optimizations, and client libraries.
