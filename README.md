<div align="center">
  <img src="https://recached.dev/recached.jpg" alt="Recached" width="800" />
  <h1>Recached has moved</h1>
  <p><b>This repository is no longer maintained here.</b></p>

  <a href="https://github.com/recached-dev/recached"><img src="https://img.shields.io/badge/New_home-recached--dev%2Frecached-brightgreen.svg?logo=github" alt="New home"></a>
  <a href="https://recached.dev"><img src="https://img.shields.io/badge/Docs-recached.dev-blue.svg" alt="Docs"></a>
  <a href="LICENSE.md"><img src="https://img.shields.io/badge/License-Apache_2.0-green.svg" alt="Apache 2.0"></a>
</div>

---

> [!IMPORTANT]
> **Recached now lives at [github.com/recached-dev/recached](https://github.com/recached-dev/recached).**
>
> This repo was not transferred — it was re-created under the project's own organization — so
> **GitHub does not redirect from here.** Stars, watches, issues, and existing `git remote` URLs do
> not follow automatically. Please update your links and remotes using the table below.

Recached is a Rust cache server that runs on your backend **and** inside the browser: the same
engine speaks RESP on port 6379 (any Redis client works, zero code changes) and compiles to
WebAssembly so reads come from local memory instead of a network round-trip.

**→ Documentation, use cases, API reference, and guides: [recached.dev](https://recached.dev)**

---

## Where things moved

| What | Old | New |
|---|---|---|
| Repository | `thinkgrid-labs/recached` | [`recached-dev/recached`](https://github.com/recached-dev/recached) |
| Issues & discussions | this repo | [recached-dev/recached/issues](https://github.com/recached-dev/recached/issues) |
| Docker image | `ghcr.io/thinkgrid-labs/recached` | `ghcr.io/recached-dev/recached` |
| Homebrew tap | `brew tap thinkgrid-labs/recached` | `brew tap recached-dev/recached` |
| Releases | this repo | [recached-dev/recached/releases](https://github.com/recached-dev/recached/releases) |

Package names are **unchanged** — nothing to do if you install from a registry:

```bash
cargo install recached      # crates.io name unchanged
npm install recached-edge   # npm name unchanged
```

## Update an existing clone

```bash
git remote set-url origin https://github.com/recached-dev/recached.git
git remote -v   # verify
```

## Update a deployment

```bash
# Docker
docker pull ghcr.io/recached-dev/recached:latest

# Homebrew
brew untap thinkgrid-labs/recached
brew tap recached-dev/recached
brew install recached
```

---

## Why the move

Recached started inside `thinkgrid-labs` alongside other projects. It has its own docs site,
release cadence, and users, so it now has its own organization — matching the
[recached.dev](https://recached.dev) domain that fronts the project.

Everything else is the same: same maintainer, same Apache-2.0 license, same roadmap.

---

## Archived state

The code here is frozen at the last version published from this organization. It is left in place
so old links resolve to this notice rather than a 404. **Do not open issues or pull requests
against this repository** — they will not be seen. All development continues at
[recached-dev/recached](https://github.com/recached-dev/recached).

---

<div align="center">
  <sub>Apache-2.0 · <a href="https://recached.dev">recached.dev</a> · <a href="https://github.com/recached-dev/recached">recached-dev/recached</a></sub>
</div>
