# recached-edge

The browser and edge runtime client for [Recached](https://github.com/thinkgrid-labs/recached) — a Rust-powered in-memory cache that runs natively on the server and inside the browser via WebAssembly.

Zero-latency local reads. Automatic background sync to the Recached server over WebSockets.

## Install

```bash
npm install recached-edge
```

## Quick start

```javascript
import init, { RecachedCache } from 'recached-edge';

// Initialize the WebAssembly module once at app startup
await init();

const cache = new RecachedCache();

// Connect to a running Recached server (optional — local reads work without it)
cache.connect('ws://localhost:6380');

// Write — stored instantly in local WASM memory, pushed to server in background
cache.set('user:theme', 'dark');

// Read — served from local memory, zero network round-trip
const theme = cache.get('user:theme'); // "dark"

// Delete
const deleted = cache.del('user:theme'); // 1
```

## How sync works

```
Browser (recached-edge)          Recached Server
       │                                │
       │  SET user:theme dark  ────────►│  stores in server
       │                       ◄────────│  broadcasts to other clients
       │                                │
other  │◄── SET user:theme dark ────────│  other browser tabs update automatically
tabs   │                                │
```

Any `set` or `del` in the browser is pushed to the server and fanned out to all other connected clients. Any mutation on the server is pushed down to all connected browsers. Reads always come from local WASM memory — no network hop.

## API

### `new RecachedCache()`

Creates a new local cache instance. Safe to call before `connect()` — local reads and writes work immediately.

---

### `cache.connect(url: string): void`

Connects to a Recached server over WebSocket and begins syncing state. Calling `connect()` again on an existing instance cleanly replaces the previous connection without leaking memory.

```javascript
cache.connect('ws://localhost:6380');

// With a custom domain
cache.connect('wss://cache.example.com');
```

---

### `cache.auth(password: string): void`

Sends an `AUTH` command to the server. Call this immediately after `connect()` if the server has `RECACHED_PASSWORD` set. The result is delivered asynchronously via the WebSocket.

```javascript
cache.connect('ws://localhost:6380');
cache.auth('my-secret-password');
```

---

### `cache.set(key: string, value: string): string`

Stores a key-value pair in local memory and syncs it to the server. Returns `"OK"` on success or an error string if the server's key limit is reached.

```javascript
cache.set('session:abc', JSON.stringify({ userId: 42 }));
```

---

### `cache.get(key: string): string | undefined`

Returns the value for a key from local memory, or `undefined` if the key does not exist. Always reads locally — no network latency.

```javascript
const raw = cache.get('session:abc');
const session = raw ? JSON.parse(raw) : null;
```

---

### `cache.del(key: string): number`

Deletes a key from local memory and syncs the deletion to the server. Returns `1` if the key existed, `0` if it did not.

```javascript
cache.del('session:abc'); // 1
```

---

## Framework examples

### React

```javascript
import { useEffect, useState } from 'react';
import init, { RecachedCache } from 'recached-edge';

let cache;

export function useRecached() {
  const [ready, setReady] = useState(false);

  useEffect(() => {
    init().then(() => {
      cache = new RecachedCache();
      cache.connect('ws://localhost:6380');
      setReady(true);
    });
  }, []);

  return { cache, ready };
}
```

### Without a server (browser-only local cache)

`recached-edge` works standalone as a fast in-process key-value store with no server required. Just skip `connect()`.

```javascript
await init();
const cache = new RecachedCache();

cache.set('theme', 'dark');
cache.get('theme'); // "dark" — fully local, no network
```

---

## Running the server

```bash
# Docker
docker run -p 6380:6380 ghcr.io/thinkgrid-labs/recached:latest

# With authentication
docker run -p 6380:6380 -e RECACHED_PASSWORD=secret ghcr.io/thinkgrid-labs/recached:latest
```

See the [Recached README](https://github.com/thinkgrid-labs/recached) for full server configuration.

## Browser compatibility

Requires WebAssembly support (all modern browsers) and the WebSocket API for server sync. Works in Cloudflare Workers and Deno Deploy with the WASI build target (coming soon).

## License

MIT
