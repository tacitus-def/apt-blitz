//! Cache management subcommands for `blitzctl`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use apt_blitz::cache::{time_until_expiry_map, Cache, CacheEntryDetail, CachedFile};
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum CacheCmd {
    /// List cached resource hosts with totals
    Hosts,
    /// Show the resource filesystem tree for a host
    Tree {
        /// Resource host, e.g. deb.debian.org
        host: String,
        /// Optional path prefix filter within the host
        #[arg(default_value = "")]
        path: String,
    },
    /// Show details of a single cached file by its exact URL
    Info {
        /// Exact cached URL
        url: String,
    },
    /// Clear the cache (all, or by host/path selector)
    Clear {
        /// Optional selector: `host` or `host/path` (prefix or exact file)
        target: Option<String>,
        /// Skip the confirmation prompt (full clear only)
        #[arg(long, short)]
        yes: bool,
    },
}

pub async fn run(dir: PathBuf, sub: CacheCmd) -> anyhow::Result<()> {
    match sub {
        CacheCmd::Hosts => cmd_hosts(&dir).await,
        CacheCmd::Tree { host, path } => cmd_tree(&dir, &host, &path).await,
        CacheCmd::Info { url } => cmd_info(&dir, &url).await,
        CacheCmd::Clear { target, yes } => cmd_clear(&dir, target, yes).await,
    }
}

// ---------------------------------------------------------------------------
// URL helpers
// ---------------------------------------------------------------------------

/// Split a cached URL into its `(host, path)` components.
fn host_and_path(url: &str) -> (String, String) {
    match url::Url::parse(url) {
        Ok(u) => {
            let host = u.host_str().unwrap_or("(unknown)").to_string();
            (host, u.path().to_string())
        }
        Err(_) => ("(unknown)".to_string(), url.to_string()),
    }
}

/// Strip a `scheme://` prefix so selectors may be given as full URLs.
fn normalize_target(s: &str) -> String {
    if let Some(idx) = s.find("://") {
        s[idx + 3..].to_string()
    } else {
        s.to_string()
    }
}

struct Target {
    host: String,
    path_prefix: Option<String>,
}

/// Parse a selector into a host and optional path prefix.
///
/// `deb.debian.org` → host only; `deb.debian.org/pool/main` → host + `/pool/main`.
fn parse_target(s: &str) -> Target {
    match s.find('/') {
        Some(idx) => Target {
            host: s[..idx].to_string(),
            path_prefix: Some(s[idx..].to_string()),
        },
        None => Target {
            host: s.to_string(),
            path_prefix: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

/// Format a byte count into a human-readable string (binary units).
fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{} B", bytes);
    }
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{:.1} {}", size, UNITS[unit])
}

/// Format a unix timestamp as an HTTP date, or `n/a` for non-positive values.
fn fmt_ts(secs: i64) -> String {
    if secs <= 0 {
        return "n/a".to_string();
    }
    let st = std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs as u64);
    httpdate::fmt_http_date(st)
}

// ---------------------------------------------------------------------------
// Tree model
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TreeNode {
    name: String,
    is_dir: bool,
    size: u64,
    file_count: usize,
    children: BTreeMap<String, TreeNode>,
}

impl TreeNode {
    fn new(name: String, is_dir: bool) -> Self {
        TreeNode {
            name,
            is_dir,
            size: 0,
            file_count: 0,
            children: BTreeMap::new(),
        }
    }

    fn insert(&mut self, path: &str, size: u64) {
        let comps: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        self.insert_comps(&comps, size);
    }

    fn insert_comps(&mut self, comps: &[&str], size: u64) {
        if comps.is_empty() {
            self.size += size;
            self.file_count += 1;
            return;
        }
        let (first, rest) = comps.split_first().unwrap();
        if rest.is_empty() {
            let entry = self
                .children
                .entry(first.to_string())
                .or_insert_with(|| TreeNode::new(first.to_string(), false));
            entry.is_dir = false;
            entry.size += size;
            entry.file_count += 1;
            self.size += size;
            self.file_count += 1;
        } else {
            let entry = self
                .children
                .entry(first.to_string())
                .or_insert_with(|| TreeNode::new(first.to_string(), true));
            entry.is_dir = true;
            entry.insert_comps(rest, size);
            self.size += size;
            self.file_count += 1;
        }
    }
}

fn print_tree(node: &TreeNode, prefix: &str) {
    let mut children: Vec<&TreeNode> = node.children.values().collect();
    children.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.cmp(&b.name),
    });
    let count = children.len();
    for (i, child) in children.iter().enumerate() {
        let last = i == count - 1;
        let branch = if last { "└── " } else { "├── " };
        if child.is_dir {
            println!(
                "{}{}{}/  ({} , {} files)",
                prefix,
                branch,
                child.name,
                human_size(child.size),
                child.file_count
            );
            let child_prefix = format!("{}{}    ", prefix, if last { " " } else { "│" });
            print_tree(child, &child_prefix);
        } else {
            println!(
                "{}{}{}  ({})",
                prefix,
                branch,
                child.name,
                human_size(child.size)
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Subcommands
// ---------------------------------------------------------------------------

async fn cmd_hosts(dir: &PathBuf) -> anyhow::Result<()> {
    if !dir.join("cache.db").exists() {
        println!("cache is empty or not initialized at {}", dir.display());
        return Ok(());
    }
    let cache = Cache::new(dir.clone(), u64::MAX)?;
    let entries = cache.list_all().await?;
    if entries.is_empty() {
        println!("(no cached entries)");
        return Ok(());
    }

    let mut map: BTreeMap<String, (u64, usize)> = BTreeMap::new();
    for e in &entries {
        let (host, _) = host_and_path(&e.url);
        let entry = map.entry(host).or_insert((0, 0));
        entry.0 += e.size;
        entry.1 += 1;
    }
    let total: u64 = map.values().map(|(s, _)| *s).sum();

    println!(
        "Cached hosts: {} hosts, {} files, {} total",
        map.len(),
        entries.len(),
        human_size(total)
    );
    for (host, (size, count)) in &map {
        println!("  {:<40} {:>10}  {} files", host, human_size(*size), count);
    }
    Ok(())
}

async fn cmd_tree(dir: &PathBuf, host: &str, path_filter: &str) -> anyhow::Result<()> {
    if !dir.join("cache.db").exists() {
        println!("cache is empty or not initialized at {}", dir.display());
        return Ok(());
    }
    let cache = Cache::new(dir.clone(), u64::MAX)?;
    let entries = cache.list_all().await?;

    let prefix = if path_filter.is_empty() {
        None
    } else {
        Some(path_filter.to_string())
    };

    let filtered: Vec<(String, String, u64)> = entries
        .iter()
        .map(|e| {
            let (h, p) = host_and_path(&e.url);
            (h, p, e.size)
        })
        .filter(|(h, p, _)| {
            h == host && prefix.as_ref().map_or(true, |pre| p.starts_with(pre))
        })
        .collect();

    if filtered.is_empty() {
        match prefix {
            Some(p) => println!("no entries for host '{}' under '{}'", host, p),
            None => println!("no entries for host '{}'", host),
        }
        return Ok(());
    }

    let mut root = TreeNode::new(host.to_string(), true);
    for (_, path, size) in &filtered {
        root.insert(path, *size);
    }

    println!(
        "{}  ({} files, {})",
        host,
        root.file_count,
        human_size(root.size)
    );
    print_tree(&root, "");
    Ok(())
}

async fn cmd_info(dir: &PathBuf, url: &str) -> anyhow::Result<()> {
    if !dir.join("cache.db").exists() {
        println!("cache is empty or not initialized at {}", dir.display());
        return Ok(());
    }
    let cache = Cache::new(dir.clone(), u64::MAX)?;
    let detail: Option<CacheEntryDetail> = cache.entry_by_url(url).await?;
    match detail {
        None => {
            eprintln!("no cached entry for URL: {}", url);
            std::process::exit(1);
        }
        Some(d) => {
            let (host, path) = host_and_path(&d.url);
            let fresh = time_until_expiry_map(d.cached_at, &d.headers, 86400);
            let content_type = d.headers.get("content-type").map(String::as_str).unwrap_or("-");
            let fresh_str = match fresh {
                Some(s) if s > 0 => format!("{}s remaining", s),
                _ => "expired / not cacheable".to_string(),
            };
            println!("URL:         {}", d.url);
            println!("Host:        {}", host);
            println!("Path:        {}", path);
            println!("Size:        {} ({} bytes)", human_size(d.size), d.size);
            println!("Cached at:   {}", fmt_ts(d.cached_at));
            println!("Last access: {}", fmt_ts(d.last_access));
            println!("Freshness:   {}", fresh_str);
            println!("Content-Type:{}", content_type);
            println!("Stored file: {}/{}", dir.display(), d.file_path);
            Ok(())
        }
    }
}

async fn cmd_clear(dir: &PathBuf, target: Option<String>, yes: bool) -> anyhow::Result<()> {
    if !dir.join("cache.db").exists() {
        println!(
            "cache is empty or not initialized at {}; nothing to clear",
            dir.display()
        );
        return Ok(());
    }
    let cache = Cache::new(dir.clone(), u64::MAX)?;

    match target {
        None => {
            if !yes {
                print!(
                    "This will DELETE ALL cached files in {}. Type 'yes' to confirm: ",
                    dir.display()
                );
                use std::io::Write;
                std::io::stdout().flush().ok();
                let mut input = String::new();
                std::io::stdin().read_line(&mut input).ok();
                if input.trim() != "yes" {
                    println!("aborted");
                    return Ok(());
                }
            }
            let removed = cache.clear_all().await?;
            println!("cleared all cached entries ({} files removed)", removed);
        }
        Some(t) => {
            let norm = normalize_target(&t);
            let tgt = parse_target(&norm);
            let entries = cache.list_all().await?;
            let matches = |e: &CachedFile| -> bool {
                let (h, p) = host_and_path(&e.url);
                h == tgt.host
                    && tgt
                        .path_prefix
                        .as_ref()
                        .map_or(true, |pre| p.starts_with(pre))
            };
            let filtered: Vec<&CachedFile> = entries.iter().filter(|e| matches(e)).collect();
            if filtered.is_empty() {
                println!("no entries match target '{}'", t);
                return Ok(());
            }
            let total_size: u64 = filtered.iter().map(|e| e.size).sum();
            let urls: Vec<String> = filtered.iter().map(|e| e.url.clone()).collect();
            let removed = cache.delete_by_urls(&urls).await?;
            println!(
                "removed {} entries ({}) for target '{}'",
                removed,
                human_size(total_size),
                t
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_target_host_only() {
        let t = parse_target("deb.debian.org");
        assert_eq!(t.host, "deb.debian.org");
        assert!(t.path_prefix.is_none());
    }

    #[test]
    fn test_parse_target_host_path() {
        let t = parse_target("deb.debian.org/pool/main");
        assert_eq!(t.host, "deb.debian.org");
        assert_eq!(t.path_prefix, Some("/pool/main".to_string()));
    }

    #[test]
    fn test_normalize_target_scheme() {
        assert_eq!(
            normalize_target("http://deb.debian.org/pool/x"),
            "deb.debian.org/pool/x"
        );
    }

    #[test]
    fn test_host_and_path() {
        let (h, p) = host_and_path("http://deb.debian.org/pool/main/a.deb");
        assert_eq!(h, "deb.debian.org");
        assert_eq!(p, "/pool/main/a.deb");
    }

    #[test]
    fn test_tree_build_aggregates() {
        let mut root = TreeNode::new("host".to_string(), true);
        root.insert("/pool/main/a/apt.deb", 100);
        root.insert("/pool/main/b/b.deb", 200);
        root.insert("/pool/other.deb", 50);
        assert_eq!(root.size, 350);
        assert_eq!(root.file_count, 3);
        let pool = &root.children["pool"];
        assert_eq!(pool.size, 350);
        assert_eq!(pool.file_count, 3);
        assert!(pool.children.contains_key("main"));
        assert!(pool.children.contains_key("other.deb"));
    }

    #[test]
    fn test_human_size() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1024), "1.0 KiB");
        assert_eq!(human_size(1536), "1.5 KiB");
        assert_eq!(human_size(1048576), "1.0 MiB");
    }
}
