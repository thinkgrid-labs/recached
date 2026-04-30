<div align="center">
  <img src="recached.png" alt="Recached" width="200" />
  <h1>Recached ⚡</h1>
  <p><b>The Blazing Fast, Multi-Core, Local-First Redis Alternative written in Rust.</b></p>
  
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
> Recached is in active development and is **not yet ready for production workloads**. The core architecture is fully functional and hardened, but full Redis command coverage (data types, TTL, persistence, pub/sub, transactions) is still being implemented. Use it today for prototyping, local-first web apps, and experimentation.

---

## 🚀 Key Features

- **Multi-Core by Default:** Traditional Redis is strictly single-threaded. Recached leverages Rust's `tokio` runtime to handle every connection on a dedicated async task, utilizing 100% of your CPU cores without clustering.
- **Drop-in Redis Replacement:** Speaks the standard RESP (REdis Serialization Protocol). You do not need to change a single line of your application code or install new client libraries.
- **Local-First WebAssembly (Wasm):** Recached compiles to a lightweight `.wasm` package. Run the database locally inside the browser or on Edge networks (Cloudflare Workers, Deno Deploy) with zero network latency.
- **Real-Time WebSocket Sync:** Dual-port architecture that broadcasts state changes instantly between the Native Server and Wasm browser clients using RESP over WebSockets.
- **Production-Ready Security:** Built-in IP allowlisting, `AUTH` password enforcement with brute-force protection, connection limiting, and structured `tracing` logs on every error path.

---

## 📦 Installation

Recached distributes as a single, dependency-free binary.

### Docker (Recommended)
```bash
docker run -p 6379:6379 -p 6380:6380 ghcr.io/thinkgrid-labs/recached:latest
```

### Homebrew (macOS)
```bash
brew tap thinkgrid-labs/recached
brew install recached
```

### Cargo
```bash
cargo install recached
recached-server
```

---

## 💻 Usage

### Backend (Redis-compatible TCP — port 6379)

Point any standard Redis client directly at Recached — no code changes required.

```javascript
import Redis from 'ioredis';

const redis = new Redis('redis://127.0.0.1:6379');

await redis.set('user:1', 'John Doe');
console.log(await redis.get('user:1')); // "John Doe"
```

### Frontend / Edge (WebAssembly — port 6380)

Import the cache into the browser for zero-latency local reads with automatic background sync to the server.

```javascript
import init, { RecachedCache } from 'recached-edge';

await init();

const cache = new RecachedCache();
cache.connect("ws://127.0.0.1:6380");

cache.set("theme", "dark");       // instant local write + syncs to server
console.log(cache.get("theme"));  // "dark" — read from local WASM memory
```

---

## 🔒 Security Configuration

Recached binds to `localhost` only by default. Lock it down further for production:

```bash
RECACHED_PASSWORD="super_secret_password" \
RECACHED_ALLOW_IPS="127.0.0.1,10.0.0.55" \
RECACHED_MAX_KEYS="1000000" \
recached-server
```

| Variable | Description |
|---|---|
| `RECACHED_PASSWORD` | Require all clients to authenticate with `AUTH <password>`. Connections are dropped after 5 wrong attempts. |
| `RECACHED_ALLOW_IPS` | Comma-separated IP allowlist. Invalid entries are logged and skipped. |
| `RECACHED_MAX_KEYS` | Hard cap on total stored keys. `SET` returns an error once the limit is hit. |
| `RUST_LOG` | Log level (`info`, `debug`, `warn`). Defaults to `info`. |

---

## 🏗️ Architecture

Recached is a Cargo workspace with hard boundaries between the state machine and the network layer:

| Crate | Description |
|---|---|
| `core-engine` | Zero-dependency, no-network state machine. Custom length-prefixed RESP parser with depth-limited array recursion. Thread-safe `Arc<RwLock<HashMap>>` KV store. |
| `server-native` | Multi-core TCP (port 6379) and WebSocket (port 6380) server built on `tokio`. Persistent read buffers, connection semaphore, auth rate-limiting, structured `tracing` logging. |
| `wasm-edge` | `wasm-bindgen` browser bindings. Zero-latency local reads with RESP-over-WebSocket background sync. Proper closure lifecycle management. |

---

## 🗺️ Roadmap

### 🔲 Redis String & Key Parity
Full coverage of Redis string commands and key utilities — the foundation for a true drop-in replacement.

**String commands**
- `APPEND`, `STRLEN`
- `INCR`, `INCRBY`, `INCRBYFLOAT`, `DECR`, `DECRBY`
- `MGET`, `MSET`, `MSETNX`
- `GETSET`, `GETDEL`, `GETEX`
- `SETNX`, `SETEX`, `PSETEX`
- `SET` options: `EX`, `PX`, `NX`, `XX`, `KEEPTTL`, `GET`

**TTL & expiry**
- `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`
- `TTL`, `PTTL`, `PERSIST`
- Background lazy expiry (scan-on-read + periodic sweep)

**Key utilities**
- `EXISTS` (multi-key), `TYPE`, `RENAME`, `RENAMENX`, `COPY`
- `SCAN`, `KEYS` (pattern matching)
- `RANDOMKEY`, `DBSIZE`
- `FLUSHDB`, `FLUSHALL`
- `UNLINK` (non-blocking async delete)

---

### 🔲 Redis Data Structures
Full implementation of all Redis collection types.

**Hash** — `HSET`, `HGET`, `HMGET`, `HGETALL`, `HKEYS`, `HVALS`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY`, `HINCRBYFLOAT`, `HSCAN`

**List** — `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LMOVE`, `BLPOP`, `BRPOP`

**Set** — `SADD`, `SMEMBERS`, `SREM`, `SCARD`, `SISMEMBER`, `SMISMEMBER`, `SPOP`, `SRANDMEMBER`, `SINTER`, `SUNION`, `SDIFF`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SSCAN`

**Sorted Set** — `ZADD`, `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGE`, `ZREM`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZINTERSTORE`, `ZUNIONSTORE`, `ZPOPMIN`, `ZPOPMAX`, `ZSCAN`

---

### 🔲 Redis Advanced Features
Achieving full behavioral parity with Redis.

**Pub/Sub** — `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBSUB`

**Transactions** — `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

**Persistence**
- RDB snapshotting: `BGSAVE`, `SAVE`, `LASTSAVE`
- AOF (Append Only File) with configurable `fsync` policy

**Server commands** — `INFO`, `DEBUG`, `CONFIG GET/SET/REWRITE`, `COMMAND`, `COMMAND COUNT`, `COMMAND INFO`, `SLOWLOG`, `MONITOR`

**Scripting** — `EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`

**Replication** — `REPLICAOF`, `SLAVEOF`, read-replica propagation

---

### 🔲 Beyond Redis
Features that are architecturally impossible in Redis but native to Recached.

**Performance**
- [ ] **Sharded `DashMap` core** — replace `RwLock<HashMap>` with a lock-striped concurrent map; eliminates write contention on multi-core hardware entirely
- [ ] **RESP3 protocol** — richer types (maps, sets, doubles, attributes) with zero extra parsing overhead
- [ ] **Zero-copy command dispatch** — avoid heap allocation on the hot path for `GET`/`SET`

**Security & Ops**
- [ ] **Native TLS/mTLS** — encrypt TCP and WebSocket connections without a sidecar proxy
- [ ] **Built-in Prometheus metrics** — `/metrics` endpoint exposing hit rate, latency percentiles, memory usage, and connection counts; no plugin required
- [ ] **Pluggable eviction policies** — LRU, LFU, TTL-priority, and ARC available via `RECACHED_EVICTION=lfu`

**New Primitives** (no Redis equivalent)
- [ ] **Native JSON type** — `JSET`, `JGET`, `JMERGE`, `JPATCH`, `JDEL` with JSONPath queries; no module required
- [ ] **Rate-limiting commands** — `RLSET key limit window`, `RLCHECK key` backed by a sliding-window counter; replace hand-rolled Lua scripts in Redis
- [ ] **Observable keys** — `WATCH key` over WebSocket pushes a RESP message to the subscriber on every mutation; reactive data binding without polling
- [ ] **WASM server-side scripting** — run `.wasm` modules as stored procedures; replaces Redis Lua scripting with a sandboxed, type-safe, multi-language alternative
- [ ] **Multi-region CRDTs** — active-active replication using conflict-free replicated data types; eventual consistency across regions without a coordinator

**Edge & Browser**
- [ ] **Cloudflare Workers / Deno Deploy targets** — WASI build profile with `wasm32-wasip1` for true serverless edge caching
- [ ] **Offline-first sync** — IndexedDB-backed persistence in the WASM layer; cache survives browser refresh and syncs delta on reconnect
- [ ] **Native TypeScript SDK** — typed client generated from the command schema; zero-overhead bindings via the WASM module (no JSON serialization)

---

## 🤝 Contributing

Recached is open source. The highest-impact contribution areas right now are:

1. **Commands** — string operations and TTL expiry are the most-requested missing features
2. **Benchmarks** — `redis-benchmark` comparisons against Redis 7 on multi-core hardware
3. **Client libraries** — Python, Go, and Java clients that speak RESP to port 6379
4. **WASM examples** — React, Vue, and SvelteKit demos using `recached-edge`

Open a PR or file an issue to get started.
