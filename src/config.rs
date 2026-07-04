use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;

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
}

/// Raw YAML representation — all fields optional (file provides defaults)
#[derive(serde::Deserialize)]
struct YamlConfig {
    port: Option<u16>,
    bind: Option<String>,
    connections: Option<usize>,
    cache_dir: Option<PathBuf>,
    max_cache_size: Option<u64>,
    url_map: Option<Vec<String>>,
    upstream_proxy: Option<String>,
    no_proxy: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// CLI definition (clap derive — single source of truth for defaults/env)
// ---------------------------------------------------------------------------
#[derive(Parser, Clone, Debug)]
#[command(name = "apt-blitz", version, about = "Multithreaded proxy for APT-like package managers")]
struct Cli {
    #[arg(long, default_value = "8080", env = "PROXY_PORT")]
    port: u16,

    #[arg(long, default_value = "0.0.0.0", env = "PROXY_BIND")]
    bind: String,

    #[arg(long, default_value_t = 4, env = "PROXY_CONNECTIONS")]
    connections: usize,

    #[arg(long, default_value = "/var/cache/apt-blitz", env = "PROXY_CACHE_DIR")]
    cache_dir: PathBuf,

    #[arg(long, default_value_t = 1_073_741_824, env = "PROXY_MAX_CACHE_SIZE")]
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
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Config {{ port: {}, bind: {}, connections: {}, cache_dir: {}, max_cache_size: {}, url_maps: {}, upstream_proxy: {}, no_proxy: {} }}",
            self.port,
            self.bind,
            self.connections,
            self.cache_dir.display(),
            self.max_cache_size,
            self.url_maps.len(),
            self.upstream_proxy.as_ref().map(|u| format!("{:?}://{}:{}", u.proxy_type, u.host, u.port)).unwrap_or_default(),
            self.no_proxy.join(","),
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
        // Should return None when no config exists
        let result = Config::discover();
        // No assertion on value — just ensure it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_config_debug() {
        let cfg = Config {
            port: 8080,
            bind: "0.0.0.0".into(),
            connections: 4,
            cache_dir: PathBuf::from("/tmp/cache"),
            max_cache_size: 1024,
            url_maps: vec![UrlMap::parse("a=http://a.com").unwrap()],
            upstream_proxy: None,
            no_proxy: vec![],
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
            bind: "0.0.0.0".into(),
            connections: 4,
            cache_dir: PathBuf::from("/var/cache/apt-blitz"),
            max_cache_size: 1_073_741_824,
            url_maps: vec![],
            upstream_proxy: None,
            no_proxy: vec![],
        };
        let output = cfg.to_string();
        assert!(output.contains("port: 8080"));
        assert!(output.contains("bind: 0.0.0.0"));
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
        };
        yaml.apply_env();

        // YAML must NOT override existing env
        assert_eq!(std::env::var("PROXY_PORT").unwrap(), "9999");
        assert_eq!(std::env::var("PROXY_BIND").unwrap(), "10.0.0.1");

        std::env::remove_var("PROXY_PORT");
        std::env::remove_var("PROXY_BIND");
    }
}
