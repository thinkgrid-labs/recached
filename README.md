<div align="center">
  <img src="recached.jpg" alt="Recached" width="800" />
  <h1>Recached ⚡</h1>
  <p><b>A Rust cache server that runs on your backend <em>and</em> inside the browser.</b></p>

  <a href="#"><img src="https://img.shields.io/badge/Language-Rust-orange.svg" alt="Rust"></a>
  <a href="#"><img src="https://img.shields.io/badge/Architecture-Multi--Core-blue.svg" alt="Multi-Core"></a>
  <a href="#"><img src="https://img.shields.io/badge/Ecosystem-WebAssembly-yellow.svg" alt="Wasm"></a>
  <a href="#"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT"></a>
</div>

---

**Recached** is an in-memory cache written in Rust with one idea that existing caches don't have: it compiles to WebAssembly so the same cache engine runs natively on your server *and* directly inside the browser or edge runtime, with the two sides kept in sync over WebSockets.

On the backend it speaks RESP, so any Redis client works against it today. In the browser, you import it as a `.wasm` module and get zero-latency local reads with automatic background sync to the server — no extra round-trips, no polling, no external state management library.

> [!NOTE]
> **Status: Active Development**
> Recached covers the most common Redis use cases — strings, expiry, counters, batch ops, all collection types (Hash/List/Set/Sorted Set), transactions, and pub/sub. It is not yet a full Redis replacement (no persistence, no replication, no Lua scripting). Best for local-first web apps, session caches, rate limiters, and edge caching experiments.

---

## Why Recached exists

Every caching solution today forces a choice: put the cache on the server (latency on every read) or duplicate state in the client (stale data, cache invalidation hell). Recached removes that choice.

The `core-engine` crate is a pure Rust state machine with no network dependencies. It compiles to native code for the server and to `.wasm` for the browser. Both run the same logic. The WebSocket sync layer keeps them consistent — a `SET` on the server pushes to all connected browser instances automatically.

```
┌─────────────────┐        RESP (port 6379)        ┌──────────────────┐
│   Your backend  │ ──────────────────────────────► │  Recached Server │
└─────────────────┘                                 │  (server-native) │
                                                    └────────┬─────────┘
                                                             │ WebSocket
                                                             │ sync (6380)
                                                    ┌────────▼─────────┐
                                                    │  Browser / Edge  │
                                                    │  (wasm-edge)     │
                                                    │  local reads: 0ms│
                                                    └──────────────────┘
```

---

## Getting started

### Run the server

```bash
# Docker
docker run -p 6379:6379 -p 6380:6380 ghcr.io/thinkgrid-labs/recached:latest

# Homebrew (macOS)
brew tap thinkgrid-labs/recached && brew install recached && recached-server

# Cargo
cargo install recached && recached-server
```

### Use from your backend (any Redis client, port 6379)

```javascript
import Redis from 'ioredis';

const cache = new Redis('redis://127.0.0.1:6379');
await cache.set('user:1', 'Alice');
console.log(await cache.get('user:1')); // "Alice"

// Collections work too
await cache.hset('session:42', 'user', 'Alice', 'role', 'admin');
await cache.lpush('queue:jobs', 'task-1', 'task-2');
await cache.sadd('tags:post:1', 'rust', 'wasm', 'cache');
await cache.zadd('leaderboard', 100, 'alice', 200, 'bob');

// Pub/Sub
const sub = new Redis('redis://127.0.0.1:6379');
sub.subscribe('events');
sub.on('message', (channel, message) => console.log(channel, message));
await cache.publish('events', 'hello');
```

### Use from the browser (WebAssembly, port 6380)

```javascript
import init, { RecachedCache } from 'recached-edge';

await init();
const cache = new RecachedCache();

// Connects to the server and syncs state changes in the background
cache.connect('ws://127.0.0.1:6380');

cache.set('theme', 'dark');        // writes locally + pushes to server
console.log(cache.get('theme'));   // reads from local WASM memory — 0ms

// Subscribe to server-side pub/sub channels
cache.subscribe('notifications');
// Publish from the browser to all subscribers
cache.publish('events', 'user-clicked');
```

Any mutation on the server side (`SET`, `DEL`, `HSET`, `LPUSH`, etc.) is automatically pushed to all connected browser instances. Any write from the browser is pushed to the server and fanned out to other clients.

---

## Configuration

```bash
RECACHED_PASSWORD="secret"          \  # require AUTH; disconnects after 5 wrong attempts
RECACHED_ALLOW_IPS="127.0.0.1"     \  # comma-separated allowlist (invalid entries are logged + skipped)
RECACHED_MAX_KEYS="1000000"         \  # hard key cap; SET errors when reached
RUST_LOG="info"                     \  # log level: error / warn / info / debug
recached-server
```

---

## Architecture

Three crates with hard dependency boundaries:

| Crate | Role |
|---|---|
| `core-engine` | Pure state machine — no networking, no I/O. RESP parser (depth-limited), typed command dispatch, `Arc<RwLock<HashMap>>` store with `EntryValue` enum (Str/Hash/List/Set/ZSet), TTL engine, optional key cap. Compiles to both native and `wasm32`. |
| `server-native` | Tokio TCP server (port 6379) + WebSocket server (port 6380). Persistent read buffers handle fragmented RESP. Per-connection pub/sub delivery via `mpsc` channels. Connection semaphore, auth rate-limiting, sender-ID broadcast filter, structured `tracing` logging. |
| `wasm-edge` | `wasm-bindgen` JS bindings. Local zero-latency reads, RESP-over-WebSocket sync with the server. Closure lifecycle managed correctly — reconnecting doesn't leak memory. |

---

## What works today

**Protocol & server**
- RESP protocol — full parser/serializer, handles fragmentation, depth-limited (no stack-overflow DoS)
- TCP (port 6379) compatible with any Redis client
- WebSocket sync (port 6380) between server and browser WASM instances
- Sender-ID filter: browser clients don't double-apply their own mutations
- `RECACHED_PASSWORD` + brute-force lockout after 5 failures
- `RECACHED_ALLOW_IPS` with validated IP parsing
- `RECACHED_MAX_KEYS` memory cap
- Connection semaphore (max 1024 concurrent)
- Background active eviction (1s sweep) + lazy eviction on every read
- Structured `tracing` logs

**Commands**

*Core*
- `PING`, `AUTH`

*Strings*
- `SET` (with `EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`KEEPTTL`/`GET`), `GET`, `GETSET`
- `MGET`, `MSET`, `SETNX`, `SETEX`, `PSETEX`
- `APPEND`, `STRLEN`
- `INCR`, `DECR`, `INCRBY`, `DECRBY`

*Expiry*
- `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`
- `TTL`, `PTTL`, `PERSIST`

*Keys*
- `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `RENAME`
- `KEYS`, `SCAN`, `DBSIZE`, `FLUSHDB`

*Hash*
- `HSET`, `HGET`, `HGETALL`, `HDEL`, `HMGET`
- `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`, `HSETNX`
- `HINCRBY`, `HINCRBYFLOAT`

*List*
- `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`
- `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`
- `LSET`, `LREM`, `LTRIM`

*Set*
- `SADD`, `SMEMBERS`, `SREM`, `SCARD`, `SISMEMBER`, `SMISMEMBER`
- `SINTER`, `SINTERSTORE`, `SUNION`, `SUNIONSTORE`, `SDIFF`, `SDIFFSTORE`
- `SPOP`, `SRANDMEMBER`, `SMOVE`

*Sorted Set*
- `ZADD` (with `NX`/`XX`/`CH`/`INCR`), `ZREM`, `ZINCRBY`
- `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`
- `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`, `ZCARD`, `ZCOUNT`

*Transactions*
- `MULTI`, `EXEC`, `DISCARD` — queued execution, broadcast on commit

*Pub/Sub*
- `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`
- Pattern matching with glob syntax (`*`, `?`, `[...]`)
- Works over both TCP (port 6379) and WebSocket (port 6380)

---

## Roadmap

### Redis command parity

The goal is enough behavioral compatibility to cover the top real-world use cases, not a full Redis clone. Full parity (250+ commands, Lua scripting, RDB/AOF, replication) doesn't fit the browser-sync model and won't be pursued.

**Phase 5 — Performance & Ops** ← next
- [ ] **Sharded `DashMap` core** — swap `RwLock<HashMap>` for a lock-striped concurrent map; removes write bottleneck on high-core-count machines
- [ ] **RESP3 support** — richer native types (maps, doubles, blob errors) without client workarounds
- [ ] **Native TLS** — encrypt TCP and WebSocket connections without a sidecar
- [ ] **Built-in Prometheus metrics** — hit rate, latency percentiles, memory, connection counts at `/metrics`; no module needed
- [ ] **Pluggable eviction** — LRU, LFU, TTL-priority, ARC via `RECACHED_EVICTION=lfu`

**Beyond Redis — new primitives**
- [ ] **Native JSON type** — `JSET`, `JGET`, `JMERGE` with JSONPath; no RedisJSON module
- [ ] **Rate-limiting commands** — `RLSET key limit window` / `RLCHECK key`; replaces hand-rolled Lua scripts
- [ ] **Observable keys** — `WATCH key` over WebSocket delivers a push on every mutation; reactive bindings without polling
- [ ] **WASM server-side scripting** — run `.wasm` stored procedures instead of Lua; sandboxed, multi-language

**Edge & browser**
- [ ] **WASI target** — `wasm32-wasip1` build for Cloudflare Workers and Deno Deploy
- [ ] **Offline-first WASM** — IndexedDB persistence layer; cache survives refresh and syncs delta on reconnect
- [ ] **Typed TypeScript SDK** — generated from the command schema, zero-overhead WASM bindings

Intentionally out of scope: RDB/AOF persistence (a browser-synced in-memory cache doesn't need disk durability), `REPLICAOF` (the native→browser WebSocket is already the sync story), Lua scripting (`EVAL` doesn't run in WASM), server introspection (`INFO`, `SLOWLOG`, `COMMAND`).

---

## Contributing

The most useful contributions right now:

1. **Benchmarks** — `redis-benchmark` against Redis 7 on multi-core hardware (results welcome either way)
2. **Client examples** — React, Vue, or SvelteKit demos using `recached-edge`
3. **Phase 5 performance** — `DashMap` swap or Prometheus metrics endpoint
4. **Bug reports** — edge cases in the RESP parser, TTL eviction, pub/sub delivery, or WebSocket sync

Open a PR or file an issue.
