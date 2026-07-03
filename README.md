# apt-blitz

HTTP forward proxy for APT package managers with multithreaded downloads and disk caching.

Inspired by `apt-cacher-ng` and `aria2` — combines request coalescing, range-based parallel downloads, and LRU-evicting SQLite cache to speed up repetitive package downloads in CI or local networks.

## Features

- **Multithreaded downloads** — Splits a single file into ranged chunks and downloads them in parallel (up to 32 connections). Adapts segment size dynamically based on per-worker throughput (64 K–4 M).
- **Request coalescing** — When multiple clients request the same URL simultaneously, only one upstream download is made; followers read from the same in-flight buffer.
- **SQLite disk cache** — WAL mode, LRU eviction by `last_access`. Stores response headers alongside cached files.
- **Bandwidth throttling** — Per-request minimum speed guarantee; disables automatically when all segments are ready.
- **Plain proxy fallback** — Falls back to single-stream proxy for small files (<256 K), servers without `Accept-Ranges: bytes`, or when multithreaded download fails.
- **Zero-config** — Reasonable defaults; configure via CLI flags or environment variables.

## Quick start

```bash
# Build
cargo build --release

# Run (default port 8080)
./target/release/apt-blitz
```

Point APT at the proxy:

```console
$ echo 'Acquire::http::Proxy "http://localhost:8080";' > /etc/apt/apt.conf.d/99proxy
$ apt update
```

## Configuration

All options can be set via CLI flags or environment variables.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--port` | `PROXY_PORT` | `8080` | Listen port |
| `--bind` | `PROXY_BIND` | `0.0.0.0` | Bind address |
| `--connections` | `PROXY_CONNECTIONS` | `4` | Parallel connections per download |
| `--min-speed` | `PROXY_MIN_SPEED` | `51200` | Minimum throttle speed (bytes/s) |
| `--cache-dir` | `PROXY_CACHE_DIR` | `/var/cache/apt-blitz` | Cache directory |
| `--max-cache-size` | `PROXY_MAX_CACHE_SIZE` | `1073741824` (1 GiB) | Maximum cache size |

```bash
# All environment variables
PROXY_PORT=3128 PROXY_CACHE_DIR=/tmp/cache ./target/release/apt-blitz
```

## Architecture

```
┌─────────┐  GET /pool/a.deb   ┌──────────────────────┐
│  client  │ ──────────────────▶│  axum handler        │
│  (apt)   │◀──────────────────│  (proxy.rs)          │
└─────────┘  streamed body     └───────┬──────────────┘
                                       │
                          ┌────────────┼────────────┐
                          ▼            ▼            ▼
                     ┌──────────┐ ┌──────────┐ ┌──────────┐
                     │  cache   │ │coalescer │ │  temp    │
                     │ (SQLite) │ │(dedup)   │ │  file    │
                     └──────────┘ └────┬─────┘ └──────────┘
                                       │
                          ┌────────────┼────────────┐
                          ▼            ▼            ▼
                     ┌──────────┐ ┌──────────┐ ┌──────────┐
                     │download  │ │download  │ │download  │
                     │worker 0  │ │worker 1  │ │worker N  │
                     └──────────┘ └──────────┘ └──────────┘
                          │            │            │
                          └────────────┼────────────┘
                                       ▼
                               ┌──────────────┐
                               │   upstream    │
                               │   (mirror)    │
                               └──────────────┘
```

### Request flow

1. **Cache lookup** — SQLite check by URL hash; serve file directly on hit.
2. **Coalescer** — If another client is already fetching the same URL, join as follower reading the shared `SegmentsBuffer`. Otherwise become leader.
3. **HEAD probe** — Leader sends HEAD to upstream to check `Content-Length` and `Accept-Ranges`.
4. **Decision** — Files ≥256 K with `Accept-Ranges: bytes` get multithreaded download; everything else falls through to plain proxy.
5. **Multithreaded** — Leader creates a pre-allocated temp file, spawns N workers that claim byte ranges atomically, download via ranged GETs, write via `pwrite(2)`, and mark segments ready.
6. **Streaming** — Leader and any follower(s) stream the temp file to their clients; throttle applies until all segments complete.
7. **Caching** — On success, the temp file is renamed into the cache directory and indexed in SQLite. On failure, the temp file is deleted.

### Key modules

| Module | File | Role |
|--------|------|------|
| `proxy` | `src/proxy.rs` | HTTP handler, request routing, stream construction |
| `buffer` | `src/buffer.rs` | `SegmentsBuffer` — thread-safe shared buffer with CAS range claiming and broadcast readiness |
| `coalescer` | `src/coalescer.rs` | In-flight request deduplication via oneshot channels |
| `downloader` | `src/downloader.rs` | N parallel range workers with adaptive segment sizing |
| `cache` | `src/cache.rs` | SQLite-backed disk cache, WAL mode, LRU eviction |
| `config` | `src/config.rs` | Clap-derived configuration |
| `lib` | `src/lib.rs` | `build_app` / `run_proxy` helpers for reuse and testing |

## Development

```bash
# Build and run
cargo build
cargo run -- --port 8080

# Run tests (135 unit + 16 integration)
cargo test

# Release binary
cargo build --release
```

Environment variable for slow networks:

```bash
export CARGO_HTTP_LOW_SPEED_LIMIT=5
```

## Packaging

### DEB (Debian / Ubuntu)

```bash
dpkg-buildpackage -us -uc
```

The resulting `.deb` package is placed in the parent directory.

### RPM (Fedora / RHEL)

```bash
cargo build --release
rpmbuild -ba rpm/apt-blitz.spec
```

## Limitations

- HTTP forward proxy only — no HTTPS CONNECT tunneling.
- Single catch-all route (`/{*url}`) — expects fully-qualified upstream URLs in the path.
- No authentication or access control.
- Minimum segment size is 64 K.

## License

MIT
