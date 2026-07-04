# apt-blitz

HTTP forward proxy for APT package managers with multithreaded downloads, FTP support, and disk caching.

Inspired by `apt-cacher-ng` and `aria2` — combines request coalescing, range-based parallel downloads, CONNECT tunneling, FTP proxying, and LRU-evicting SQLite cache to speed up repetitive package downloads in CI or local networks.

## Features

- **Multithreaded downloads** — Splits a single file into ranged chunks and downloads them in parallel (up to 32 connections). Adapts segment size dynamically based on per-worker throughput (64 K–4 M).
- **Request coalescing** — When multiple clients request the same URL simultaneously, only one upstream download is made; followers read from the same in-flight buffer.
- **FTP support** — Proxies FTP URLs (`ftp://`), single-threaded and multithreaded (`PASV` + `REST`). Anonymous or password-authenticated.
- **CONNECT tunnel** — Handles `CONNECT` for HTTPS, SOCKS5, and arbitrary TCP tunnels. Supports upstream HTTP/SOCKS5 proxy chaining and `NO_PROXY` bypass.
- **SQLite disk cache** — WAL mode, LRU eviction by `last_access`. Stores response headers alongside cached files.
- **Plain proxy fallback** — Falls back to single-stream proxy for files without `Accept-Ranges: bytes`, or when multithreaded download fails.
- **URL mapping** — Map fake hosts to real upstream URLs to cache HTTPS content through the proxy.
- **Upstream proxy chain** — Route through another HTTP, HTTPS, or SOCKS5 proxy with optional authentication.
- **YAML configuration** — Config file auto-discovery (`apt-blitz.yaml`, `~/.config/apt-blitz/config.yaml`, `/etc/apt-blitz/`) with CLI/env override hierarchy.
- **Graceful shutdown** — Ctrl+C waits for active connections to finish.
- **Zero-config** — Reasonable defaults; works out of the box.

## Quick start

```bash
# Build
cargo build --release

# Run (default port 8080)
./target/release/apt-blitz
```

Or with Docker:

```bash
docker build -t apt-blitz .
docker run --rm -p 8080:8080 apt-blitz
```

Point APT at the proxy:

```console
$ echo 'Acquire::http::Proxy "http://localhost:8080";' > /etc/apt/apt.conf.d/99proxy
$ apt update
```

## Configuration

All options can be set via CLI flags or environment variables. A YAML config file can provide defaults (CLI/env take precedence).

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--port` | `PROXY_PORT` | `8080` | Listen port |
| `--bind` | `PROXY_BIND` | `0.0.0.0` | Bind address |
| `--connections` | `PROXY_CONNECTIONS` | `4` | Parallel connections per download |
| `--cache-dir` | `PROXY_CACHE_DIR` | `/var/cache/apt-blitz` | Cache directory |
| `--max-cache-size` | `PROXY_MAX_CACHE_SIZE` | `1073741824` (1 GiB) | Maximum cache size |
| `--url-map` | `PROXY_URL_MAP` | — | Fake-host to real-base mapping (`fake-apt=https://real.example.com`), repeatable or comma-separated |
| `--upstream-proxy` | `PROXY_UPSTREAM_PROXY` | — | Upstream proxy URL (`http://proxy:3128`, `socks5://host:1080`) |
| `--no-proxy` | `PROXY_NO_PROXY` | — | Bypass upstream proxy for these hosts (supports `*`, suffix `.local`, CIDR) |
| `--config-file` | `PROXY_CONFIG_FILE` | — | Explicit YAML config path |

```bash
# All environment variables
PROXY_PORT=3128 PROXY_CACHE_DIR=/tmp/cache PROXY_UPSTREAM_PROXY=socks5://10.0.0.1:1080 \
  ./target/release/apt-blitz
```

### YAML config example

```yaml
# apt-blitz.yaml
port: 8080
bind: "0.0.0.0"
connections: 8
cache_dir: "/var/cache/apt-blitz"
max_cache_size: 4294967296
url_map:
  - "deb=https://deb.debian.org"
  - "sec=https://security.debian.org"
upstream_proxy: "http://10.0.0.1:3128"
no_proxy:
  - ".local"
  - "10.0.0.0/8"
```

Auto-discovery locations (in order):
1. `./apt-blitz.yaml` / `./apt-blitz.yml`
2. `~/.config/apt-blitz/config.yaml` / `config.yml`
3. `/etc/apt-blitz/config.yaml` / `config.yml`

## Architecture

```
                         ┌──────────────┐
                         │   TCP accept  │
                         │  (lib.rs)     │
                         └──────┬───────┘
                                │
                     ┌──────────┴──────────┐
                     ▼                     ▼
             ┌──────────────┐    ┌─────────────────┐
             │  CONNECT     │    │  HTTP request    │
             │  tunnel      │    │  (proxy.rs)      │
             │  (proxy.rs)  │    └────────┬────────┘
             └──────────────┘             │
                  │            ┌──────────┼──────────┐
                  ▼            ▼          ▼          ▼
           ┌───────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐
           │ upstream  │ │  cache   │ │coalesc │ │  FTP     │
           │ (direct / │ │ (SQLite) │ │(dedup) │ │ (ftp.rs) │
           │  proxy)   │ └──────────┘ └───┬────┘ └──────────┘
           └───────────┘                  │
                               ┌──────────┼──────────┐
                               ▼          ▼          ▼
                          ┌─────────┐ ┌─────────┐ ┌─────────┐
                          │download │ │download │ │download │
                          │worker 0 │ │worker 1 │ │worker N │
                          └─────────┘ └─────────┘ └─────────┘
                               │          │          │
                               └──────────┼──────────┘
                                          ▼
                                  ┌──────────────┐
                                  │   upstream    │
                                  │   (mirror)    │
                                  └──────────────┘
```

### Request flow

1. **TCP accept + peek** — First 7 bytes are peeked; if `CONNECT`, the request is handled by `handle_connect_tunnel` (direct, SOCKS5, or HTTP proxy upstream). Otherwise, the connection is upgraded to HTTP/1.1 and forwarded to the axum router.
2. **URL resolution** — If the URL matches a `fake-host` prefix from `--url-map`, it is rewritten to the real upstream base URL (allows caching HTTPS content via the proxy).
3. **Cache lookup** — SQLite check by SHA-256 URL hash; if present and the file exists on disk, it is served directly.
4. **Coalescer** — If another client is already fetching the same URL, join as follower reading the shared `SegmentsBuffer`. Otherwise become leader.
5. **HEAD probe** — Leader sends HEAD to upstream to check `Content-Length` and `Accept-Ranges`.
6. **Decision** — Files with `Accept-Ranges: bytes` and a known `Content-Length` get multithreaded download; everything else falls through to plain proxy.
7. **Multithreaded** — Leader creates a pre-allocated temp file, spawns N workers that claim byte ranges atomically (CAS), download via ranged GETs, write via `pwrite(2)`, and mark segments ready. Segment size adapts per-worker based on throughput.
8. **Fallback** — If the multithreaded download fails (e.g. server doesn't support ranges as advertised), the leader falls back to a plain `GET` into the same buffer.
9. **Streaming** — Leader and follower(s) stream the temp file to their clients via `pread(2)`. Throttle (24 KiB chunks) applies until all segments complete; afterwards the full remaining data is sent without pacing.
10. **Caching** — On success, the temp file is renamed into the cache directory (sharded by hash prefix) and indexed in SQLite with stored response headers. On failure, the temp file is deleted.

### CONNECT tunnel variants

| Upstream proxy | Mode | Auth |
|----------------|------|------|
| None | Direct TCP to target | — |
| `socks5://host:port` | SOCKS5 | Optional user:pass |
| `http://host:port` / `https://host:port` | HTTP CONNECT relay | Optional Basic auth |

`NO_PROXY` rules (`*`, `.suffix`, exact host, CIDR) bypass the upstream proxy for matching destinations.

### Key modules

| Module | File | Role |
|--------|------|------|
| `proxy` | `src/proxy.rs` | HTTP handler, CONNECT tunnel, FTP proxy, request routing, stream construction, cache serving, URL resolution |
| `buffer` | `src/buffer.rs` | `SegmentsBuffer` — thread-safe shared buffer with CAS range claiming, per-segment Mutex, broadcast channel for readiness, `pwrite`/`pread` I/O |
| `coalescer` | `src/coalescer.rs` | In-flight request deduplication via oneshot channels; `Pending` → `Downloading` state machine |
| `downloader` | `src/downloader.rs` | N parallel HTTP range workers with adaptive segment sizing (64 K–4 M), cancellation token |
| `ftp` | `src/ftp.rs` | FTP protocol (`PASV`, `SIZE`, `REST`, `RETR`), single + multithreaded download, URL parsing |
| `cache` | `src/cache.rs` | SQLite-backed disk cache, WAL mode, LRU eviction, header serialization |
| `config` | `src/config.rs` | Clap-derived config with YAML/ENV/CLI hierarchy, `UrlMap`, `UpstreamProxy`, auto-discovery |
| `lib` | `src/lib.rs` | `build_app` / `run_proxy` helpers, TCP accept loop with CONNECT detection, graceful shutdown |

## Development

```bash
# Build and run
cargo build
cargo run -- --port 8080

# Run tests
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
# Create source archive (version extracted from Cargo.toml)
VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml)
git archive --format=tar.gz -o "rpm/apt-blitz-${VERSION}.tar.gz" \
  --prefix="apt-blitz-${VERSION}/" HEAD

# Build RPM (--nodeps required if Rust is installed via rustup)
rpmbuild -ba rpm/apt-blitz.spec \
  --nodeps \
  --define "_sourcedir $(pwd)/rpm" \
  --define "_specdir $(pwd)/rpm" \
  --define "_builddir $(pwd)/rpm/build" \
  --define "_buildrootdir $(pwd)/rpm/buildroot" \
  --define "_rpmdir $(pwd)/rpm" \
  --define "_srcrpmdir $(pwd)/rpm"

# Result: rpm/RPMS/x86_64/apt-blitz-${VERSION}-1.x86_64.rpm
```

### Docker

```bash
docker build -t apt-blitz .
docker run --rm -p 8080:8080 apt-blitz
```

## Limitations

- HTTP forward proxy only; no transparent or reverse proxy mode.
- FTPS (`ftps://`) is parsed but not yet supported — use plain `ftp://` instead.
- Single catch-all route (`/{*url}`) — expects fully-qualified upstream URLs in the path.
- No authentication or access control.
- Minimum segment size is 64 K.

## License

MIT
