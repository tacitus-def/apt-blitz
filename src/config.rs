//! CLI, YAML config, and environment variable parsing.

use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;

/// Parse a human-readable byte value like `2K`, `10M`, `1G`, `500KB`.
///
/// Binary suffixes (×1024): `K`, `M`, `G`, `T`, `P`
/// Decimal suffixes (×1000): `KB`, `MB`, `GB`, `TB`, `PB`
/// Plain numbers without suffix are treated as bytes.
/// Whitespace between number and suffix is allowed (`2 M`).
/// Case-insensitive.
pub fn parse_bytes(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("empty string");
    }

    // Split into numeric prefix and optional suffix
    let split_pos = s.find(|c: char| !c.is_ascii_digit());
    let (num_part, suffix) = if let Some(pos) = split_pos {
        (&s[..pos], s[pos..].trim())
    } else {
        (s, "")
    };

    let base: u64 = num_part
        .parse()
        .with_context(|| format!("invalid number '{num_part}'"))?;

    if suffix.is_empty() {
        return Ok(base);
    }

    let multiplier: u64 = match suffix.to_ascii_lowercase().as_str() {
        "kb" => 1_000,
        "mb" => 1_000 * 1_000,
        "gb" => 1_000 * 1_000 * 1_000,
        "tb" => 1_000u64 * 1_000 * 1_000 * 1_000,
        "pb" => 1_000u64 * 1_000 * 1_000 * 1_000 * 1_000,
        "k" => 1024,
        "m" => 1024 * 1024,
        "g" => 1024 * 1024 * 1024,
        "t" => 1024u64 * 1024 * 1024 * 1024,
        "p" => 1024u64 * 1024 * 1024 * 1024 * 1024,
        _ => anyhow::bail!("unknown suffix '{suffix}', expected K/KB/M/MB/G/GB/T/TB/P/PB"),
    };

    base.checked_mul(multiplier)
        .with_context(|| format!("value too large: {base} × {multiplier}"))
}

/// Clap value_parser wrapper — converts `anyhow::Result` to `Result<_, String>`.
fn parse_bytes_value(s: &str) -> Result<u64, String> {
    parse_bytes(s).map_err(|e| e.to_string())
}

/// Serde helper: deserialize `Option<u64>` from either a YAML number or a
/// string with human-readable byte suffix (`"1G"`, `"10MB"`, etc.).
mod serde_bytes_option {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Num(u64),
            Str(String),
        }

        match Option::<Raw>::deserialize(deserializer)? {
            Some(Raw::Num(n)) => Ok(Some(n)),
            Some(Raw::Str(s)) => {
                super::parse_bytes(&s).map(Some).map_err(serde::de::Error::custom)
            }
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn serialize<S>(val: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(n) => serializer.serialize_some(n),
            None => serializer.serialize_none(),
        }
    }
}

/// Type of upstream proxy
#[derive(Clone, Debug, PartialEq)]
pub enum ProxyType {
    Http,
    Https,
    Socks5,
}

/// Upstream proxy configuration
#[derive(Clone, Debug, PartialEq)]
pub struct UpstreamProxy {
    pub proxy_type: ProxyType,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl UpstreamProxy {
    /// Parse proxy URL like `http://user:pass@host:port` or `socks5://host:port`
    pub fn parse(input: &str) -> anyhow::Result<Self> {
        // Strip scheme
        let (proxy_type, rest) = if let Some(s) = input.strip_prefix("socks5://") {
            (ProxyType::Socks5, s)
        } else if let Some(s) = input.strip_prefix("http://") {
            (ProxyType::Http, s)
        } else if let Some(s) = input.strip_prefix("https://") {
            (ProxyType::Https, s)
        } else {
            anyhow::bail!("upstream proxy must start with http://, https://, or socks5://");
        };

        // Split userinfo and hostport
        let (userinfo, hostport) = if let Some(idx) = rest.rfind('@') {
            let ui = &rest[..idx];
            let hp = &rest[idx + 1..];
            (Some(ui), hp)
        } else {
            (None, rest)
        };

        let (username, password) = if let Some(ui) = userinfo {
            if let Some((u, p)) = ui.split_once(':') {
                (Some(u.to_string()), Some(p.to_string()))
            } else {
                (Some(ui.to_string()), None)
            }
        } else {
            (None, None)
        };

        // Parse host:port
        let (host, port) = if let Some((h, p)) = hostport.rsplit_once(':') {
            let port: u16 = p.parse().context("invalid proxy port")?;
            (h.to_string(), port)
        } else {
            (hostport.to_string(), 1080) // default SOCKS port
        };

        if host.is_empty() {
            anyhow::bail!("upstream proxy host must not be empty");
        }

        Ok(UpstreamProxy { proxy_type, host, port, username, password })
    }
}

/// Single fake-host → real upstream mapping (supports http, https, ftp, ftps)
#[derive(Clone, Debug, PartialEq)]
pub struct UrlMap {
    pub fake_host: String,
    pub real_base: String,
}

impl UrlMap {
    pub fn parse(input: &str) -> anyhow::Result<Self> {
        let (fake, real) = input.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("invalid url-map format '{input}': expected fake-host=real-base-url")
        })?;
        let fake_host = fake.trim().to_string();
        let real_base = real.trim().to_string();
        if fake_host.is_empty() {
            anyhow::bail!("fake host must not be empty");
        }
        if !real_base.starts_with("http://")
            && !real_base.starts_with("https://")
            && !real_base.starts_with("ftp://")
            && !real_base.starts_with("ftps://")
        {
            anyhow::bail!("real base URL must start with http://, https://, ftp://, or ftps://, got '{real_base}'");
        }
        let trimmed = real_base.trim_end_matches('/');
        Ok(UrlMap { fake_host, real_base: trimmed.to_string() })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub port: u16,
    pub bind: String,
    pub connections: usize,
    pub cache_dir: PathBuf,
    pub max_cache_size: u64,
    pub url_maps: Vec<UrlMap>,
    pub upstream_proxy: Option<UpstreamProxy>,
    pub no_proxy: Vec<String>,
    pub max_connections_per_ip: usize,
    pub max_total_connections: usize,
    pub max_workers: usize,
    pub upstream_bandwidth: u64,
    pub per_ip_bandwidth: u64,
    pub coalesce_follower_timeout_secs: u64,
    pub coalesce_max_retries: u32,
}

/// Raw YAML representation — all fields optional (file provides defaults)
#[derive(serde::Deserialize)]
struct YamlConfig {
    port: Option<u16>,
    bind: Option<String>,
    connections: Option<usize>,
    cache_dir: Option<PathBuf>,
    #[serde(default, with = "serde_bytes_option")]
    max_cache_size: Option<u64>,
    url_map: Option<Vec<String>>,
    upstream_proxy: Option<String>,
    no_proxy: Option<Vec<String>>,
    max_connections_per_ip: Option<usize>,
    max_total_connections: Option<usize>,
    max_workers: Option<usize>,
    #[serde(default, with = "serde_bytes_option")]
    upstream_bandwidth: Option<u64>,
    #[serde(default, with = "serde_bytes_option")]
    per_ip_bandwidth: Option<u64>,
    coalesce_follower_timeout_secs: Option<u64>,
    coalesce_max_retries: Option<u32>,
}

// ---------------------------------------------------------------------------
// CLI definition (clap derive — single source of truth for defaults/env)
// ---------------------------------------------------------------------------
#[derive(Parser, Clone, Debug)]
#[command(name = "apt-blitz", version, about = "Multithreaded proxy for APT-like package managers")]
struct Cli {
    #[arg(long, default_value = "8080", env = "PROXY_PORT")]
    port: u16,

    #[arg(long, default_value = "127.0.0.1", env = "PROXY_BIND")]
    bind: String,

    #[arg(long, default_value_t = 4, env = "PROXY_CONNECTIONS")]
    connections: usize,

    #[arg(long, default_value = "/var/cache/apt-blitz", env = "PROXY_CACHE_DIR")]
    cache_dir: PathBuf,

    #[arg(long, default_value = "1073741824", env = "PROXY_MAX_CACHE_SIZE",
          value_parser = parse_bytes_value)]
    max_cache_size: u64,

    /// Explicit config file path. When unset, auto‑discovery is used.
    #[arg(long, env = "PROXY_CONFIG_FILE", hide = true)]
    config_file: Option<String>,

    /// Fake‑host to real‑base mapping, e.g. `fake-apt=https://real.example.com`
    #[arg(long, env = "PROXY_URL_MAP", value_delimiter = ',')]
    url_map: Vec<String>,

    /// Upstream proxy URL, e.g. `http://10.0.0.1:3128` or `socks5://proxy:1080`
    #[arg(long, env = "PROXY_UPSTREAM_PROXY")]
    upstream_proxy: Option<String>,

    /// Comma‑separated hosts to bypass upstream proxy (NO_PROXY syntax)
    #[arg(long, env = "PROXY_NO_PROXY", value_delimiter = ',')]
    no_proxy: Vec<String>,

    /// Max in-flight downloads per client IP (0 = unlimited)
    #[arg(long, default_value_t = 0, env = "PROXY_MAX_CONNECTIONS_PER_IP")]
    max_connections_per_ip: usize,

    /// Max total concurrent connections across all IPs (0 = unlimited)
    #[arg(long, default_value_t = 0, env = "PROXY_MAX_TOTAL_CONNECTIONS")]
    max_total_connections: usize,

    /// Max total worker threads across all concurrent downloads (0 = unlimited)
    #[arg(long, default_value_t = 0, env = "PROXY_MAX_WORKERS")]
    max_workers: usize,

    /// Global upstream bandwidth limit (bytes/sec, supports K/M/G/T suffixes, 0 = unlimited)
    #[arg(long, default_value = "0", env = "PROXY_UPSTREAM_BANDWIDTH",
          value_parser = parse_bytes_value)]
    upstream_bandwidth: u64,

    /// Per-IP bandwidth limit (bytes/sec, supports K/M/G/T suffixes, 0 = unlimited)
    #[arg(long, default_value = "0", env = "PROXY_PER_IP_BANDWIDTH",
          value_parser = parse_bytes_value)]
    per_ip_bandwidth: u64,

    /// Timeout (seconds) for a follower waiting for an in-flight download buffer
    #[arg(long, default_value_t = 50, env = "PROXY_COALESCE_FOLLOWER_TIMEOUT_SECS")]
    coalesce_follower_timeout_secs: u64,

    /// Max retries when leader drops the download before attaching a buffer
    #[arg(long, default_value_t = 3, env = "PROXY_COALESCE_MAX_RETRIES")]
    coalesce_max_retries: u32,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------
impl Config {
    /// Load config using the full hierarchy: YAML → ENV → CLI.
    pub fn load() -> anyhow::Result<Self> {
        // 1. Extract --config-file before full clap parse
        let explicit = std::env::args()
            .skip(1)
            .filter_map(|a| a.strip_prefix("--config-file=").map(|s| s.to_string()))
            .next();

        let config_path = explicit
            .map(PathBuf::from)
            .or_else(Self::discover);

        // 2. Load YAML → set missing env vars
        if let Some(path) = &config_path {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read config {path:?}"))?;
            let yaml: YamlConfig = serde_yaml::from_str(&content)
                .with_context(|| format!("failed to parse config {path:?}"))?;
            yaml.apply_env();
        }

        // 3. Clap parse — picks up shell env + newly‑set YAML env
        let cli = Cli::parse();

        // 4. Build final config
        let url_maps: Vec<UrlMap> = cli
            .url_map
            .iter()
            .map(|s| UrlMap::parse(s))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let upstream_proxy = cli
            .upstream_proxy
            .as_ref()
            .map(|s| UpstreamProxy::parse(s))
            .transpose()?;

        Ok(Config {
            port: cli.port,
            bind: cli.bind,
            connections: cli.connections,
            cache_dir: cli.cache_dir,
            max_cache_size: cli.max_cache_size,
            url_maps,
            upstream_proxy,
            no_proxy: cli.no_proxy,
            max_connections_per_ip: cli.max_connections_per_ip,
            max_total_connections: cli.max_total_connections,
            max_workers: cli.max_workers,
            upstream_bandwidth: cli.upstream_bandwidth,
            per_ip_bandwidth: cli.per_ip_bandwidth,
            coalesce_follower_timeout_secs: cli.coalesce_follower_timeout_secs,
            coalesce_max_retries: cli.coalesce_max_retries,
        })
    }

    /// Auto‑discover config file at standard locations.
    fn discover() -> Option<PathBuf> {
        let candidates = [
            PathBuf::from("apt-blitz.yaml"),
            PathBuf::from("apt-blitz.yml"),
        ];

        // CWD
        for p in &candidates {
            if p.exists() {
                return Some(p.clone());
            }
        }

        // ~/.config/apt-blitz/
        if let Some(home) = dirs::config_dir() {
            let dir = home.join("apt-blitz");
            for name in ["config.yaml", "config.yml"] {
                let p = dir.join(name);
                if p.exists() {
                    return Some(p);
                }
            }
        }

        // /etc/apt-blitz/
        for name in ["config.yaml", "config.yml"] {
            let p = PathBuf::from("/etc/apt-blitz").join(name);
            if p.exists() {
                return Some(p);
            }
        }

        None
    }
}

// ---------------------------------------------------------------------------
// YAML → ENV bridge
// ---------------------------------------------------------------------------
impl YamlConfig {
    fn apply_env(&self) {
        macro_rules! set {
            ($key:literal, $val:expr) => {
                if let Some(v) = $val {
                    let var = format!("PROXY_{}", $key);
                    if std::env::var(&var).is_err() {
                        std::env::set_var(&var, v.to_string());
                    }
                }
            };
        }

        set!("PORT", self.port);
        set!("BIND", self.bind.as_ref());
        set!("CONNECTIONS", self.connections);
        set!("CACHE_DIR", self.cache_dir.as_ref().map(|p| p.to_string_lossy().to_string()));
        set!("MAX_CACHE_SIZE", self.max_cache_size);

        if let Some(maps) = &self.url_map {
            if !maps.is_empty() {
                let var = "PROXY_URL_MAP".to_string();
                if std::env::var(&var).is_err() {
                    std::env::set_var(&var, maps.join(","));
                }
            }
        }

        set!("UPSTREAM_PROXY", self.upstream_proxy.as_ref());
        if let Some(np) = &self.no_proxy {
            if !np.is_empty() {
                let var = "PROXY_NO_PROXY".to_string();
                if std::env::var(&var).is_err() {
                    std::env::set_var(&var, np.join(","));
                }
            }
        }
        set!("MAX_CONNECTIONS_PER_IP", self.max_connections_per_ip);
        set!("MAX_TOTAL_CONNECTIONS", self.max_total_connections);
        set!("MAX_WORKERS", self.max_workers);
        set!("UPSTREAM_BANDWIDTH", self.upstream_bandwidth);
        set!("PER_IP_BANDWIDTH", self.per_ip_bandwidth);
        set!("COALESCE_FOLLOWER_TIMEOUT_SECS", self.coalesce_follower_timeout_secs);
        set!("COALESCE_MAX_RETRIES", self.coalesce_max_retries);
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Config {{ port: {}, bind: {}, connections: {}, cache_dir: {}, max_cache_size: {}, url_maps: {}, upstream_proxy: {}, no_proxy: {}, max_connections_per_ip: {}, max_total_connections: {}, max_workers: {}, upstream_bandwidth: {}, per_ip_bandwidth: {}, coalesce_follower_timeout_secs: {}, coalesce_max_retries: {} }}",
            self.port,
            self.bind,
            self.connections,
            self.cache_dir.display(),
            self.max_cache_size,
            self.url_maps.len(),
            self.upstream_proxy.as_ref().map(|u| format!("{:?}://{}:{}", u.proxy_type, u.host, u.port)).unwrap_or_default(),
            self.no_proxy.join(","),
            self.max_connections_per_ip,
            self.max_total_connections,
            self.max_workers,
            self.upstream_bandwidth,
            self.per_ip_bandwidth,
            self.coalesce_follower_timeout_secs,
            self.coalesce_max_retries,
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    // --- UrlMap ---

    #[test]
    fn test_url_map_valid() {
        let m = UrlMap::parse("fake-debian=https://deb.debian.org").unwrap();
        assert_eq!(m.fake_host, "fake-debian");
        assert_eq!(m.real_base, "https://deb.debian.org");
    }

    #[test]
    fn test_url_map_trailing_slash_removed() {
        let m = UrlMap::parse("x=http://example.com/").unwrap();
        assert_eq!(m.real_base, "http://example.com");
    }

    #[test]
    fn test_url_map_no_delimiter() {
        assert!(UrlMap::parse("bogus").is_err());
    }

    #[test]
    fn test_url_map_empty_fake() {
        assert!(UrlMap::parse("=https://x.com").is_err());
    }

    #[test]
    fn test_url_map_bad_scheme() {
        assert!(UrlMap::parse("x=file:///tmp/foo").is_err());
    }

    #[test]
    fn test_url_map_https_ok() {
        let m = UrlMap::parse("s=https://secure.example.com").unwrap();
        assert_eq!(m.real_base, "https://secure.example.com");
    }

    #[test]
    fn test_url_map_http_ok() {
        let m = UrlMap::parse("h=http://http.example.com").unwrap();
        assert_eq!(m.real_base, "http://http.example.com");
    }

    #[test]
    fn test_url_map_whitespace_trimmed() {
        let m = UrlMap::parse("  host = https://x.com  ").unwrap();
        assert_eq!(m.fake_host, "host");
        assert_eq!(m.real_base, "https://x.com");
    }

    // --- Cli parse (light) ---

    #[test]
    fn test_cli_defaults() {
        let cli = Cli::try_parse_from(&["apt-blitz"]).unwrap();
        assert_eq!(cli.port, 8080);
        assert!(cli.config_file.is_none());
        assert!(cli.url_map.is_empty());
        assert_eq!(cli.coalesce_follower_timeout_secs, 50);
        assert_eq!(cli.coalesce_max_retries, 3);
    }

    #[test]
    fn test_cli_url_map_single() {
        let cli = Cli::try_parse_from(&[
            "apt-blitz",
            "--url-map",
            "a=http://a.com",
        ])
        .unwrap();
        assert_eq!(cli.url_map, vec!["a=http://a.com"]);
    }

    #[test]
    fn test_cli_url_map_multi() {
        let cli = Cli::try_parse_from(&[
            "apt-blitz",
            "--url-map",
            "a=http://a.com",
            "--url-map",
            "b=https://b.org",
        ])
        .unwrap();
        assert_eq!(cli.url_map, vec!["a=http://a.com", "b=https://b.org"]);
    }

    #[test]
    fn test_cli_config_file() {
        let cli =
            Cli::try_parse_from(&["apt-blitz", "--config-file", "/tmp/proxy.yaml"]).unwrap();
        assert_eq!(cli.config_file, Some("/tmp/proxy.yaml".into()));
    }

    #[test]
    fn test_cli_port_override() {
        let cli = Cli::try_parse_from(&["apt-blitz", "--port", "9999"]).unwrap();
        assert_eq!(cli.port, 9999);
    }

    #[test]
    fn test_cli_no_defaults_override() {
        let cli = Cli::try_parse_from(&["apt-blitz", "--port=0", "--connections=0"]).unwrap();
        assert_eq!(cli.port, 0);
        assert_eq!(cli.connections, 0);
    }

    // --- Config::load (tested with temp file) ---

    #[test]
    fn test_yaml_apply_env() {
        check_env_cleared();
        let dir = std::env::temp_dir().join("apt-blitz-config-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test-config.yaml");
        let yaml_src = r#"
port: 3128
bind: "127.0.0.1"
connections: 2
url_map:
  - fake-debian=https://deb.debian.org
upstream_proxy: "http://10.0.0.1:3128"
no_proxy:
  - .local
  - 10.0.0.0/8
"#;
        std::fs::write(&path, yaml_src).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let yaml: YamlConfig = serde_yaml::from_str(&content).unwrap();
        yaml.apply_env();

        assert_eq!(std::env::var("PROXY_PORT").unwrap(), "3128");
        assert_eq!(std::env::var("PROXY_BIND").unwrap(), "127.0.0.1");
        assert_eq!(std::env::var("PROXY_CONNECTIONS").unwrap(), "2");
        assert!(std::env::var("PROXY_URL_MAP").unwrap().contains("fake-debian"));
        assert_eq!(std::env::var("PROXY_UPSTREAM_PROXY").unwrap(), "http://10.0.0.1:3128");
        assert_eq!(std::env::var("PROXY_NO_PROXY").unwrap(), ".local,10.0.0.0/8");

        std::fs::remove_dir_all(&dir).ok();
        check_env_cleared();
    }

    fn check_env_cleared() {
        std::env::remove_var("PROXY_PORT");
        std::env::remove_var("PROXY_BIND");
        std::env::remove_var("PROXY_CONNECTIONS");
        std::env::remove_var("PROXY_URL_MAP");
        std::env::remove_var("PROXY_UPSTREAM_PROXY");
        std::env::remove_var("PROXY_NO_PROXY");
        std::env::remove_var("PROXY_MAX_CONNECTIONS_PER_IP");
        std::env::remove_var("PROXY_MAX_TOTAL_CONNECTIONS");
        std::env::remove_var("PROXY_MAX_WORKERS");
        std::env::remove_var("PROXY_UPSTREAM_BANDWIDTH");
        std::env::remove_var("PROXY_PER_IP_BANDWIDTH");
    }

    #[test]
    fn test_url_map_eq_produces_correct_config() {
        let parsed = UrlMap::parse("a=http://example.com/path").unwrap();
        assert_eq!(parsed.fake_host, "a");
        assert_eq!(parsed.real_base, "http://example.com/path");
    }

    #[test]
    fn test_lots_of_url_maps() {
        let maps: Vec<UrlMap> = (0..100)
            .map(|i| UrlMap::parse(&format!("h{i}=http://s{i}.example.com")).unwrap())
            .collect();
        assert_eq!(maps.len(), 100);
        assert_eq!(maps[0].fake_host, "h0");
        assert_eq!(maps[99].fake_host, "h99");
    }

    #[test]
    fn test_discover_no_file() {
        // Verifies that Config::discover() does not panic when no config file exists.
        // Result is intentionally unused — the test passes if it doesn't crash.
        let _ = Config::discover();
    }

    #[test]
    fn test_config_debug() {
        let cfg = Config {
            port: 8080,
            bind: "127.0.0.1".into(),
            connections: 4,
            cache_dir: PathBuf::from("/tmp/cache"),
            max_cache_size: 1024,
            url_maps: vec![UrlMap::parse("a=http://a.com").unwrap()],
            upstream_proxy: None,
            no_proxy: vec![],
            max_connections_per_ip: 4,
            max_total_connections: 0,
            max_workers: 0,
            upstream_bandwidth: 0,
            per_ip_bandwidth: 0,
            coalesce_follower_timeout_secs: 50,
            coalesce_max_retries: 3,
        };
        let s = format!("{cfg}");
        assert!(s.contains("8080"));
        assert!(s.contains("url_maps: 1"));
    }

    // --- UpstreamProxy ---

    #[test]
    fn test_upstream_proxy_http_parse() {
        let p = UpstreamProxy::parse("http://proxy.example.com:3128").unwrap();
        assert_eq!(p.proxy_type, ProxyType::Http);
        assert_eq!(p.host, "proxy.example.com");
        assert_eq!(p.port, 3128);
        assert!(p.username.is_none());
    }

    #[test]
    fn test_upstream_proxy_socks5_with_auth() {
        let p = UpstreamProxy::parse("socks5://user:pass@10.0.0.1:1080").unwrap();
        assert_eq!(p.proxy_type, ProxyType::Socks5);
        assert_eq!(p.host, "10.0.0.1");
        assert_eq!(p.port, 1080);
        assert_eq!(p.username.as_deref(), Some("user"));
        assert_eq!(p.password.as_deref(), Some("pass"));
    }

    #[test]
    fn test_upstream_proxy_socks5_default_port() {
        let p = UpstreamProxy::parse("socks5://myproxy").unwrap();
        assert_eq!(p.port, 1080);
    }

    #[test]
    fn test_upstream_proxy_https() {
        let p = UpstreamProxy::parse("https://10.0.0.1:443").unwrap();
        assert_eq!(p.proxy_type, ProxyType::Https);
        assert_eq!(p.port, 443);
    }

    #[test]
    fn test_upstream_proxy_invalid_scheme() {
        assert!(UpstreamProxy::parse("ftp://proxy").is_err());
    }

    #[test]
    fn test_upstream_proxy_no_scheme() {
        assert!(UpstreamProxy::parse("10.0.0.1:3128").is_err());
    }

    #[test]
    fn test_config_display_contain_fields() {
        let cfg = Config {
            port: 8080,
            bind: "127.0.0.1".into(),
            connections: 4,
            cache_dir: PathBuf::from("/var/cache/apt-blitz"),
            max_cache_size: 1_073_741_824,
            url_maps: vec![],
            upstream_proxy: None,
            no_proxy: vec![],
            max_connections_per_ip: 4,
            max_total_connections: 0,
            max_workers: 0,
            upstream_bandwidth: 0,
            per_ip_bandwidth: 0,
            coalesce_follower_timeout_secs: 50,
            coalesce_max_retries: 3,
        };
        let output = cfg.to_string();
        assert!(output.contains("port: 8080"));
        assert!(output.contains("bind: 127.0.0.1"));
        assert!(output.contains("url_maps: 0"));
    }

    #[test]
    fn test_url_map_removes_all_trailing_slashes() {
        let m = UrlMap::parse("x=http://example.com//").unwrap();
        assert_eq!(m.real_base, "http://example.com");
    }

    #[test]
    fn test_apply_env_does_not_override_existing_env() {
        check_env_cleared();
        std::env::set_var("PROXY_PORT", "9999");
        std::env::set_var("PROXY_BIND", "10.0.0.1");

        let yaml = YamlConfig {
            port: Some(8080),
            bind: Some("127.0.0.1".into()),
            connections: None,
            cache_dir: None,
            max_cache_size: None,
            url_map: None,
            upstream_proxy: None,
            no_proxy: None,
            max_connections_per_ip: None,
            max_total_connections: None,
            max_workers: None,
            upstream_bandwidth: None,
            per_ip_bandwidth: None,
            coalesce_follower_timeout_secs: None,
            coalesce_max_retries: None,
        };
        yaml.apply_env();

        // YAML must NOT override existing env
        assert_eq!(std::env::var("PROXY_PORT").unwrap(), "9999");
        assert_eq!(std::env::var("PROXY_BIND").unwrap(), "10.0.0.1");

        std::env::remove_var("PROXY_PORT");
        std::env::remove_var("PROXY_BIND");
    }

    #[test]
    fn test_parse_bytes_decimal() {
        assert_eq!(parse_bytes("100").unwrap(), 100);
        assert_eq!(parse_bytes("1K").unwrap(), 1024);
        assert_eq!(parse_bytes("10K").unwrap(), 10_240);
        assert_eq!(parse_bytes("1M").unwrap(), 1_048_576);
        assert_eq!(parse_bytes("100M").unwrap(), 104_857_600);
        assert_eq!(parse_bytes("1G").unwrap(), 1_073_741_824);
        assert_eq!(parse_bytes("2G").unwrap(), 2_147_483_648);
        assert_eq!(parse_bytes("1T").unwrap(), 1_099_511_627_776);
        assert_eq!(parse_bytes("1P").unwrap(), 1_125_899_906_842_624);
    }

    #[test]
    fn test_parse_bytes_decimal_suffix() {
        assert_eq!(parse_bytes("1KB").unwrap(), 1000);
        assert_eq!(parse_bytes("1MB").unwrap(), 1_000_000);
        assert_eq!(parse_bytes("1GB").unwrap(), 1_000_000_000);
    }

    #[test]
    fn test_parse_bytes_case_insensitive() {
        assert_eq!(parse_bytes("1k").unwrap(), 1024);
        assert_eq!(parse_bytes("1K").unwrap(), 1024);
        assert_eq!(parse_bytes("1kb").unwrap(), 1000);
        assert_eq!(parse_bytes("1Mb").unwrap(), 1_000_000);
    }

    #[test]
    fn test_parse_bytes_with_whitespace() {
        assert_eq!(parse_bytes(" 1G ").unwrap(), 1_073_741_824);
        assert_eq!(parse_bytes("2 M").unwrap(), 2_097_152);
    }

    #[test]
    fn test_parse_bytes_overflow() {
        assert!(parse_bytes("999999999999999999T").is_err());
    }

    #[test]
    fn test_parse_bytes_invalid() {
        assert!(parse_bytes("").is_err());
        assert!(parse_bytes("abc").is_err());
        assert!(parse_bytes("1X").is_err());
    }

    #[test]
    fn test_parse_bytes_value_clap() {
        assert_eq!(parse_bytes_value("1G").unwrap(), 1_073_741_824);
        assert!(parse_bytes_value("abc").is_err());
    }

    #[test]
    fn test_serde_bytes_option_number() {
        #[derive(serde::Deserialize, Debug)]
        struct TestConfig {
            #[serde(with = "serde_bytes_option")]
            val: Option<u64>,
        }

        let c: TestConfig = serde_json::from_str(r#"{"val": 1024}"#).unwrap();
        assert_eq!(c.val, Some(1024));
    }

    #[test]
    fn test_serde_bytes_option_string() {
        #[derive(serde::Deserialize, Debug)]
        struct TestConfig {
            #[serde(with = "serde_bytes_option")]
            val: Option<u64>,
        }

        let c: TestConfig = serde_json::from_str(r#"{"val": "1G"}"#).unwrap();
        assert_eq!(c.val, Some(1_073_741_824));
    }

    #[test]
    fn test_serde_bytes_option_null() {
        #[derive(serde::Deserialize, Debug)]
        struct TestConfig {
            #[serde(with = "serde_bytes_option", default)]
            val: Option<u64>,
        }

        let c: TestConfig = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(c.val, None);
    }

    #[test]
    fn test_serde_bytes_option_invalid_string() {
        #[derive(serde::Deserialize, Debug)]
        struct TestConfig {
            #[serde(with = "serde_bytes_option")]
            val: Option<u64>,
        }

        let c = serde_json::from_str::<TestConfig>(r#"{"val": "abc"}"#);
        assert!(c.is_err());
    }

    // === Security fuzz tests: CLI edge cases ===

    #[test]
    fn test_cli_port_out_of_range() {
        let r = Cli::try_parse_from(&["apt-blitz", "--port", "65536"]);
        assert!(r.is_err());
    }

    #[test]
    fn test_cli_port_negative() {
        let r = Cli::try_parse_from(&["apt-blitz", "--port", "-1"]);
        assert!(r.is_err());
    }

    #[test]
    fn test_cli_port_text() {
        let r = Cli::try_parse_from(&["apt-blitz", "--port", "abc"]);
        assert!(r.is_err());
    }

    #[test]
    fn test_cli_connections_zero() {
        let cli = Cli::try_parse_from(&["apt-blitz", "--connections", "0"]).unwrap();
        assert_eq!(cli.connections, 0);
    }

    #[test]
    fn test_cli_connections_large() {
        let cli = Cli::try_parse_from(&["apt-blitz", "--connections", "999999"]).unwrap();
        assert_eq!(cli.connections, 999999);
    }

    #[test]
    fn test_cli_connections_negative() {
        let r = Cli::try_parse_from(&["apt-blitz", "--connections", "-1"]);
        assert!(r.is_err());
    }

    #[test]
    fn test_cli_max_cache_size_zero() {
        let cli = Cli::try_parse_from(&[
            "apt-blitz",
            "--max-cache-size",
            "0",
        ])
        .unwrap();
        assert_eq!(cli.max_cache_size, 0);
    }

    #[test]
    fn test_cli_max_cache_size_overflow() {
        let r = Cli::try_parse_from(&[
            "apt-blitz",
            "--max-cache-size",
            "999999999999999999T",
        ]);
        assert!(r.is_err());
    }

    #[test]
    fn test_cli_max_cache_size_text() {
        let r = Cli::try_parse_from(&[
            "apt-blitz",
            "--max-cache-size",
            "abc",
        ]);
        assert!(r.is_err());
    }

    #[test]
    fn test_cli_upstream_bandwidth_zero() {
        let cli =
            Cli::try_parse_from(&["apt-blitz", "--upstream-bandwidth", "0"]).unwrap();
        assert_eq!(cli.upstream_bandwidth, 0);
    }

    #[test]
    fn test_cli_upstream_bandwidth_text() {
        let r =
            Cli::try_parse_from(&["apt-blitz", "--upstream-bandwidth", "abc"]);
        assert!(r.is_err());
    }

    #[test]
    fn test_cli_per_ip_bandwidth_text() {
        let r =
            Cli::try_parse_from(&["apt-blitz", "--per-ip-bandwidth", "not-a-number"]);
        assert!(r.is_err());
    }

    #[test]
    fn test_cli_max_connections_per_ip_large() {
        let cli = Cli::try_parse_from(&[
            "apt-blitz",
            "--max-connections-per-ip",
            "999999",
        ])
        .unwrap();
        assert_eq!(cli.max_connections_per_ip, 999999);
    }

    #[test]
    fn test_cli_max_total_connections_large() {
        let cli = Cli::try_parse_from(&[
            "apt-blitz",
            "--max-total-connections",
            "999999",
        ])
        .unwrap();
        assert_eq!(cli.max_total_connections, 999999);
    }

    #[test]
    fn test_cli_max_workers_large() {
        let cli =
            Cli::try_parse_from(&["apt-blitz", "--max-workers", "999999"]).unwrap();
        assert_eq!(cli.max_workers, 999999);
    }

    // === parse_bytes: more edge cases ===

    #[test]
    fn test_parse_bytes_negative() {
        // Rejected because '-' is not a digit → split_pos=Some(0) → num_part=""
        // → "".parse::<u64>() fails. This is correct but the error message
        // says "invalid number ''" rather than "negative values not allowed".
        assert!(parse_bytes("-5").is_err());
    }

    #[test]
    fn test_parse_bytes_just_suffix() {
        assert!(parse_bytes("K").is_err());
    }

    #[test]
    fn test_parse_bytes_hex_like() {
        assert!(parse_bytes("0xFF").is_err());
    }

    #[test]
    fn test_parse_bytes_float() {
        // Rejected because '.' is not a digit → suffix=".5G" → unknown suffix.
        // This is correct but the rejection reason is "unknown suffix" not "float".
        assert!(parse_bytes("1.5G").is_err());
    }

    #[test]
    fn test_parse_bytes_trailing_garbage() {
        // Trailing characters after known suffix must be rejected
        assert!(parse_bytes("1GBB").is_err());
        assert!(parse_bytes("1Gextra").is_err());
        assert!(parse_bytes("10MB!").is_err());
    }

    #[test]
    fn test_parse_bytes_spaces_only() {
        assert!(parse_bytes("   ").is_err());
    }

    #[test]
    fn test_parse_bytes_plus_sign() {
        assert!(parse_bytes("+1G").is_err());
    }

    // === UpstreamProxy: more edge cases ===

    #[test]
    fn test_upstream_proxy_empty() {
        assert!(UpstreamProxy::parse("").is_err());
    }

    #[test]
    fn test_upstream_proxy_just_scheme_empty_host() {
        let result = UpstreamProxy::parse("http://");
        assert!(result.is_err(), "empty host must be rejected");
    }

    #[test]
    fn test_upstream_proxy_huge_port() {
        assert!(UpstreamProxy::parse("http://proxy:99999").is_err());
    }

    #[test]
    fn test_upstream_proxy_port_text() {
        assert!(UpstreamProxy::parse("http://proxy:notaport").is_err());
    }

    #[test]
    fn test_upstream_proxy_http_no_port_defaults_to_1080() {
        // Known issue: HTTP proxy without port defaults to 1080 (SOCKS default)
        // instead of 80 (HTTP default). The comment in code says "default SOCKS port"
        // but the fallback applies to all proxy types.
        let p = UpstreamProxy::parse("http://proxy.example.com").unwrap();
        assert_eq!(
            p.port, 1080,
            "BUG: HTTP proxy without port defaults to {} instead of 80",
            p.port
        );
    }

    // === UrlMap: more edge cases ===

    #[test]
    fn test_url_map_ftp_allowed() {
        let m = UrlMap::parse("x=ftp://files.example.com").unwrap();
        assert_eq!(m.real_base, "ftp://files.example.com");
    }

    #[test]
    fn test_url_map_ftps_allowed() {
        let m = UrlMap::parse("x=ftps://files.example.com").unwrap();
        assert_eq!(m.real_base, "ftps://files.example.com");
    }

    #[test]
    fn test_url_map_multiple_equals() {
        let m = UrlMap::parse("host=http://example.com/path?q=1").unwrap();
        assert_eq!(m.fake_host, "host");
        assert_eq!(m.real_base, "http://example.com/path?q=1");
    }

    #[test]
    fn test_url_map_empty_after_trim() {
        assert!(UrlMap::parse("  = https://x.com").is_err());
    }

    #[test]
    fn test_url_map_only_equals() {
        assert!(UrlMap::parse("=").is_err());
    }
}
