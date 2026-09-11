# apt-blitz

HTTP forward proxy for APT package managers with multithreaded downloads, FTP support, and disk caching.

Inspired by `apt-cacher-ng` and `aria2` — combines request coalescing, range-based parallel downloads, CONNECT tunneling, FTP proxying, and LRU-evicting SQLite cache to speed up repetitive package downloads in CI or local networks.

## Features

- **Multithreaded downloads** — Splits a single file into ranged chunks and downloads them in parallel, starting with one worker and scaling up adaptively. Bounded by `--connections` (default 4) and the global worker cap `--max-workers`. Adapts segment size dynamically based on per-worker throughput (64 K–4 M).
- **Request coalescing** — When multiple clients request the same URL simultaneously, only one upstream download is made; followers read from the same in-flight buffer.
- **FTP support** — Proxies FTP URLs (`ftp://`), single-threaded and multithreaded (`PASV` + `REST`). Anonymous or password-authenticated.
- **CONNECT tunnel** — Handles `CONNECT` for HTTPS, SOCKS5, and arbitrary TCP tunnels. Supports upstream HTTP/SOCKS5 proxy chaining and `NO_PROXY` bypass.
- **SQLite disk cache** — WAL mode, LRU eviction by `last_access`. Stores response headers alongside cached files.
- **Plain proxy fallback** — Falls back to single-stream proxy for files without `Accept-Ranges: bytes`, or when multithreaded download fails.
- **Bandwidth limiting** — Per-IP (`--per-ip-bandwidth`) and global upstream (`--upstream-bandwidth`) bandwidth caps in bytes/sec. Uses token bucket algorithm with adaptive refill; excess data is buffered until tokens become available.
- **Concurrency limits** — Per-IP (`--max-connections-per-ip`) and global (`--max-total-connections`) connection caps, plus worker thread limit (`--max-workers`). Returns `429 Too Many Requests` when exceeded.
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
| `--bind` | `PROXY_BIND` | `127.0.0.1` | Bind address |
| `--connections` | `PROXY_CONNECTIONS` | `4` | Parallel connections per download |
| `--cache-dir` | `PROXY_CACHE_DIR` | `/var/cache/apt-blitz` | Cache directory |
| `--max-cache-size` | `PROXY_MAX_CACHE_SIZE` | `1073741824` (1 GiB) | Maximum cache size (supports `K`/`M`/`G`/`T`/`P` suffixes) |
| `--max-cache-age` | `PROXY_MAX_CACHE_AGE` | `86400` | Seconds a cached file is served without revalidation; 0 = revalidate every request |
| `--url-map` | `PROXY_URL_MAP` | — | Fake-host to real-base mapping (`fake-apt=https://real.example.com`), repeatable or comma-separated |
| `--upstream-proxy` | `PROXY_UPSTREAM_PROXY` | — | Upstream proxy URL (`http://proxy:3128`, `socks5://host:1080`) |
| `--no-proxy` | `PROXY_NO_PROXY` | — | Bypass upstream proxy for these hosts (supports `*`, suffix `.local`, CIDR) |
| `--config-file` | `PROXY_CONFIG_FILE` | — | Explicit YAML config path |

**Bandwidth limiting** (bytes/sec, 0 = unlimited, supports `K`/`M`/`G`/`T`/`P` suffixes):

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--upstream-bandwidth` | `PROXY_UPSTREAM_BANDWIDTH` | `0` | Global upstream bandwidth limit — throttles all download workers combined |
| `--per-ip-bandwidth` | `PROXY_PER_IP_BANDWIDTH` | `0` | Per-IP bandwidth limit — throttles each client's response stream independently |

**Concurrency limits** (0 = unlimited):

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--max-connections-per-ip` | `PROXY_MAX_CONNECTIONS_PER_IP` | `0` | Max concurrent downloads per client IP (0 = unlimited) |
| `--max-total-connections` | `PROXY_MAX_TOTAL_CONNECTIONS` | `0` | Max total concurrent connections across all IPs |
| `--max-workers` | `PROXY_MAX_WORKERS` | `0` | Max worker threads across all concurrent downloads |

**Coalescing** (request deduplication tuning):

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--coalesce-follower-timeout-secs` | `PROXY_COALESCE_FOLLOWER_TIMEOUT_SECS` | `50` | Timeout (seconds) for a follower waiting for the leader's in-flight download buffer |
| `--coalesce-max-retries` | `PROXY_COALESCE_MAX_RETRIES` | `3` | Max retries when the leader drops the download before the follower can attach to the buffer |
| `--coalesce-etag-max-retries` | `PROXY_COALESCE_ETAG_MAX_RETRIES` | `8` | Max retries when an upstream file changes mid-download (`If-Match` 412). Such failures are transient mirror re-syncs, so the download retries with the new generation instead of being treated as an upstream outage |

```bash
# All environment variables
PROXY_PORT=3128 PROXY_CACHE_DIR=/tmp/cache PROXY_UPSTREAM_PROXY=socks5://10.0.0.1:1080 \
  PROXY_COALESCE_FOLLOWER_TIMEOUT_SECS=50 PROXY_COALESCE_MAX_RETRIES=3 \
  ./target/release/apt-blitz
```

### YAML config example

```yaml
# apt-blitz.yaml
port: 8080
bind: "127.0.0.1"
connections: 8
cache_dir: "/var/cache/apt-blitz"
max_cache_size: 4G
max_cache_age: 86400
url_map:
  - "deb=https://deb.debian.org"
  - "sec=https://security.debian.org"
upstream_proxy: "http://10.0.0.1:3128"
no_proxy:
  - ".local"
  - "10.0.0.0/8"
max_connections_per_ip: 0
max_total_connections: 0
max_workers: 0
upstream_bandwidth: 50M
per_ip_bandwidth: 10M
coalesce_follower_timeout_secs: 50
coalesce_max_retries: 3
coalesce_etag_max_retries: 8
```

Auto-discovery locations (in order):
1. `./apt-blitz.yaml` / `./apt-blitz.yml`
2. `~/.config/apt-blitz/config.yaml` / `config.yml`
3. `/etc/apt-blitz/config.yaml` / `config.yml`

### Cache freshness

Cached files are served without contacting upstream while they are
fresh (upstream `Cache-Control: max-age` / `Expires` take precedence,
falling back to `--max-cache-age`). When a request is served from a
fresh cache hit, the log line `cache hit (fresh)` reports the
`ttl_secs` field — the number of seconds until the cached entry
expires. Once the freshness window expires,
the proxy revalidates the file with a conditional `HEAD`
(`If-None-Match` / `If-Modified-Since`):

- `304 Not Modified` → the cached copy is still valid and is served;
- `200 OK` with a different `ETag` → the file changed and is downloaded
  again, replacing the cached copy;
- upstream unreachable during revalidation → the stale cached copy is
  served with a warning.

Files without validators (no `ETag` / `Last-Modified`, including FTP)
are re-downloaded once their freshness window expires. Set
`--max-cache-age 0` to revalidate on every request.

## blitzctl — cache control utility

`apt-blitz` ships a companion utility, `blitzctl`, for inspecting and
managing the on-disk cache. It works directly against the SQLite database,
so it can be used while the proxy is running.

```bash
blitzctl cache --help
```

The cache directory is resolved like the service does (YAML `cache_dir` →
`PROXY_CACHE_DIR` → `/var/cache/apt-blitz`) and can be overridden with the
`--cache-dir` global flag.

All commands accept a `HOST` that may be a configured `--url-map` alias or
the real upstream host. Filtering matches either form, and output is shown
in the matching perspective (alias vs real).

| Command | Description |
|---------|-------------|
| `cache hosts` | List cached resource hosts with totals (alias, real host, size, file count). |
| `cache tree <HOST> [<PATH>]` | Show the per-host resource filesystem tree. Files are hidden by default; add `--files`/`-f` to list them. |
| `cache find <HOST> <QUERY>` | Search files/folders within a host by name. `*` and `?` are wildcards; a `/` in the query matches against the full path. Optional filters: `--min-size`/`--max-size` (bytes or `k/m/g/t`, e.g. `1k`, `2M`), `--cached-min-age`/`--cached-max-age` (age of the cache entry, e.g. `30m`, `6h`, `2d`, `1w`), and `--access-min-age`/`--access-max-age` (age of the last access). `min_age` keeps entries older than the bound, `max_age` keeps entries within it. |
| `cache ls [<HOST>] [<PATH>]` | List cached entries like the system `ls`, optionally limited to `HOST` and a `PATH` prefix or glob. |
| `cache info <HOST> <PATH>` | Show details of a single cached file (URL, perspective host/path, size, MD5/SHA1/SHA256/SHA512 checksums, timestamps, freshness, content type). Checksums are computed on the fly from the stored file. Timestamps are shown in the local timezone in GNU `ls -l` style. |
| `cache cat <HOST> <PATH>` | Print the raw contents of a single cached file to stdout (streamed). |
| `cache cp [--force] <HOST> <PATH> <DEST>` | Copy a single cached file to the local filesystem. Existing directory `DEST` places the file inside it under its original name; otherwise `DEST` is the literal file path. Refuses to overwrite unless `--force`/`-f` is given. |
| `cache rm [<TARGET>] [--yes]` | Remove all cached entries, or only those matching `TARGET` (`host` or `host/path`, prefix or exact file). Full removal asks for confirmation unless `--yes` is given. |

`cache ls` mirrors `ls(1)` flags: `-l` (long format — cached date, last
access, seconds until expiry, size), `--human` (human-readable sizes),
`-R` (recursive), `-1` (one entry per line), sorting by `-N` name (default),
`-t` last access, `-c` cache time, `-S` size, and `-r` to reverse.
Directories are always listed first. Timestamps follow GNU `ls -l` and are
shown in the local timezone: `%b %e %H:%M` for entries within the last
six months, `%b  %e  %Y` otherwise.

`cache ls`, `cache info`, `cache cat`, and `cache cp` take `HOST` and
`PATH` as separate positional arguments, like the other commands — URLs and
single-token `host/path` selectors are rejected.

`cache cat` and `cache cp` require an unambiguous match: the selector must
resolve to exactly one cached entry. No glob patterns are supported — a
selector that matches zero or more than one entry is an error.

Examples (add `--cache-dir PATH` to target a non-default cache):

```bash
blitzctl cache hosts
blitzctl cache tree deb.debian.org --files
blitzctl cache find deb.debian.org '*.deb'
blitzctl cache find deb.debian.org '*.deb' --min-size 10M --cached-min-age 30d
blitzctl cache ls -S security.debian.org pool
blitzctl cache info deb.debian.org pool/main/a/apt_1.0_all.deb
blitzctl cache cat deb.debian.org pool/main/a/apt_1.0_all.deb > apt.deb
blitzctl cache cp deb.debian.org pool/main/a/apt_1.0_all.deb .
blitzctl cache rm deb.debian.org --yes
```

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
2. **Concurrency limit** — Per-IP semaphore (`--max-connections-per-ip`) and global semaphore (`--max-total-connections`) are acquired. If limits are exceeded, the client receives `429 Too Many Requests`.
3. **URL resolution** — If the URL matches a `fake-host` prefix from `--url-map`, it is rewritten to the real upstream base URL (allows caching HTTPS content via the proxy).
4. **Cache lookup** — SQLite check by SHA-256 URL hash; if present and the file exists on disk, it is served directly.
5. **Coalescer** — If another client is already fetching the same URL, join as follower reading the shared `SegmentsBuffer`. Otherwise become leader.
6. **HEAD probe** — Leader sends HEAD to upstream to check `Content-Length` and `Accept-Ranges`.
7. **Decision** — Files with `Accept-Ranges: bytes` and a known `Content-Length` get multithreaded download; everything else falls through to plain proxy.
 8. **Multithreaded** — Leader creates a pre-allocated temp file, spawns N workers that claim byte ranges atomically (CAS), download via ranged GETs, write via `pwrite(2)`, and mark segments ready. Each worker checks the global upstream bucket (`--upstream-bandwidth`) before writing; if tokens are unavailable, it waits until refill. Segment size adapts per-worker based on throughput. If upstream advertises a strong `ETag`, each ranged GET carries `If-Match` — if the file changes mid-download (412), the download aborts to avoid mixed-generation data. A `200 OK` answer to a ranged request on a multi-segment download is rejected instead of silently truncating head-of-file bytes.
 9. **Fallback** — If the multithreaded download fails (e.g. server doesn't support ranges as advertised), the leader falls back to a plain `GET` into the same buffer. Upstream bandwidth is enforced on each chunk.
 10. **Streaming to client** — Leader and follower(s) stream the temp file to their clients via `pread(2)`. If `--per-ip-bandwidth` is set, a per-IP token bucket throttles the response body stream — chunks that exceed the bucket capacity are split and delivered as tokens become available. Throttle (24 KiB chunks) applies until all segments complete; afterwards the full remaining data is sent without pacing.
 11. **Caching** — On success, the temp file is renamed into the cache directory (sharded by hash prefix) and indexed in SQLite with stored response headers. `.xz` streams are verified with `xz --test` before being stored — a corrupt or mixed-generation index never reaches the cache. On failure, the temp file is deleted.

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
| `downloader` | `src/downloader.rs` | N parallel HTTP range workers with adaptive segment sizing (64 K–4 M), cancellation token |
| `rate_limit` | `src/rate_limit.rs` | `TokenBucket` (atomic refill + CAS), `IpRateLimiter` (per-IP concurrency + bandwidth), `WorkerLimiter` (global worker cap), `IpPermit` (held permits with bucket) |
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
- CONNECT tunnel does not filter loopback, cloud metadata, or private network IPs.
- FTPS (`ftps://`) is parsed but not yet supported — use plain `ftp://` instead.
- Single catch-all route (`/{*url}`) — expects fully-qualified upstream URLs in the path.
- No authentication or access control.
- Minimum segment size is 64 K.

## License

MIT
