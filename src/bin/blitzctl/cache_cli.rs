//! Cache management subcommands for `blitzctl`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use apt_blitz::cache::{time_until_expiry_map, Cache, CacheEntryDetail, CachedFile};
use apt_blitz::config::UrlMap;
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
        /// Also list individual cached files (directories only by default)
        #[arg(long, short)]
        files: bool,
    },
    /// Show details of a single cached file by its exact URL
    Info {
        /// Exact cached URL
        url: String,
    },
    /// Search files and folders within a host by partial/full match
    Find {
        /// Resource host, e.g. deb.debian.org
        host: String,
        /// Search query. Supports `*` and `?` wildcards. With a `/` it matches
        /// against the full path; otherwise against the name (last component).
        query: String,
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

pub async fn run(
    dir: PathBuf,
    url_maps: Vec<UrlMap>,
    sub: CacheCmd,
) -> anyhow::Result<()> {
    let maps = url_maps.as_slice();
    match sub {
        CacheCmd::Hosts => cmd_hosts(&dir, maps).await,
        CacheCmd::Tree {
            host,
            path,
            files,
        } => cmd_tree(&dir, maps, &host, &path, files).await,
        CacheCmd::Info { url } => cmd_info(&dir, maps, &url).await,
        CacheCmd::Find { host, query } => cmd_find(&dir, maps, &host, &query).await,
        CacheCmd::Clear { target, yes } => cmd_clear(&dir, maps, target, yes).await,
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

/// Which host form the user provided as input.
///
/// In `Alias` perspective only the configured alias (`fake-host` + alias
/// path) is shown; in `Real` perspective only the real upstream host/path is
/// shown. Filtering still accepts either form so a query reaches the right
/// entries regardless of perspective.
#[derive(Clone, Copy, PartialEq)]
enum Perspective {
    Alias,
    Real,
}

/// Choose the display perspective from the host the user typed: if it is a
/// configured alias, show the alias; otherwise show the real host.
fn perspective_for(host: &str, maps: &[UrlMap]) -> Perspective {
    if maps.iter().any(|m| m.fake_host == host) {
        Perspective::Alias
    } else {
        Perspective::Real
    }
}

/// A cached URL resolved against the configured `url_maps`.
///
/// `real_*` is the actual upstream host/path stored in the cache (post
/// `resolve_url`); `disp_*` is the alias (`fake-host` + leftover path) shown
/// to the user. When no mapping applies, `disp_*` equals `real_*`.
struct Resolved {
    real_host: String,
    real_path: String,
    disp_host: String,
    disp_path: String,
}

impl Resolved {
    fn new(url: &str, maps: &[UrlMap]) -> Self {
        let (real_host, real_path) = host_and_path(url);
        let (disp_host, disp_path) = reverse_resolve(url, maps);
        Resolved {
            real_host,
            real_path,
            disp_host,
            disp_path,
        }
    }

    /// Whether this entry belongs to the given host selector, matching either
    /// the alias or the real host (so users may filter by either form).
    fn host_matches(&self, host: &str) -> bool {
        self.disp_host == host || self.real_host == host
    }

    /// Host shown under the given perspective.
    fn host(&self, p: Perspective) -> &str {
        match p {
            Perspective::Alias => &self.disp_host,
            Perspective::Real => &self.real_host,
        }
    }

    /// Path shown under the given perspective.
    fn path(&self, p: Perspective) -> &str {
        match p {
            Perspective::Alias => &self.disp_path,
            Perspective::Real => &self.real_path,
        }
    }
}

/// Reverse of `resolve_url`: map a real upstream URL back to its configured
/// alias (`fake-host` + leftover path) when it matches a `url_map`.
///
/// First match wins (consistent with `resolve_url`). Falls back to the real
/// host/path when no mapping applies.
fn reverse_resolve(url: &str, maps: &[UrlMap]) -> (String, String) {
    for map in maps {
        let base = map.real_base.trim_end_matches('/');
        if let Some(rest) = url.strip_prefix(base) {
            if rest.is_empty() || rest.starts_with('/') {
                return (map.fake_host.clone(), rest.to_string());
            }
        }
    }
    host_and_path(url)
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

fn print_tree(node: &TreeNode, prefix: &str, show_files: bool) {
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
            print_tree(child, &child_prefix, show_files);
        } else if show_files {
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

async fn cmd_hosts(dir: &PathBuf, maps: &[UrlMap]) -> anyhow::Result<()> {
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

    let mut map: BTreeMap<String, (u64, usize, BTreeSet<String>)> = BTreeMap::new();
    for e in &entries {
        let r = Resolved::new(&e.url, maps);
        let entry = map
            .entry(r.disp_host.clone())
            .or_insert((0, 0, BTreeSet::new()));
        entry.0 += e.size;
        entry.1 += 1;
        entry.2.insert(r.real_host.clone());
    }
    let total: u64 = map.values().map(|(s, _, _)| *s).sum();

    println!(
        "Cached hosts: {} hosts, {} files, {} total",
        map.len(),
        entries.len(),
        human_size(total)
    );
    println!("  {:<32} {:<32} {:>10}  files", "ALIAS", "REAL HOST", "");
    for (alias, (size, count, reals)) in &map {
        let real = if reals.len() == 1 && reals.contains(alias) {
            "-".to_string()
        } else {
            reals.iter().cloned().collect::<Vec<_>>().join(", ")
        };
        println!(
            "  {:<32} {:<32} {:>10}  {}",
            alias,
            real,
            human_size(*size),
            count
        );
    }
    Ok(())
}

async fn cmd_tree(
    dir: &PathBuf,
    maps: &[UrlMap],
    host: &str,
    path_filter: &str,
    show_files: bool,
) -> anyhow::Result<()> {
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

    let p = perspective_for(host, maps);
    let filtered: Vec<(Resolved, u64)> = entries
        .iter()
        .map(|e| (Resolved::new(&e.url, maps), e.size))
        .filter(|(r, _)| {
            r.host_matches(host) && prefix.as_ref().is_none_or(|pre| r.path(p).starts_with(pre))
        })
        .collect();

    if filtered.is_empty() {
        match prefix {
            Some(p) => println!("no entries for host '{}' under '{}'", host, p),
            None => println!("no entries for host '{}'", host),
        };
        return Ok(());
    }

    let display_host = filtered[0].0.host(p).to_string();
    let mut root = TreeNode::new(display_host.clone(), true);
    for (r, size) in &filtered {
        root.insert(r.path(p), *size);
    }

    println!(
        "{}  ({} files, {})",
        display_host,
        root.file_count,
        human_size(root.size)
    );
    print_tree(&root, "", show_files);
    Ok(())
}

// ---------------------------------------------------------------------------
// Find
// ---------------------------------------------------------------------------

/// Convert a glob-like pattern (`*` = any run, `?` = any char) into a
/// case-insensitive regex. The regex is intentionally unanchored so the
/// pattern matches both fully and partially.
fn glob_to_regex(pattern: &str) -> anyhow::Result<regex::Regex> {
    let mut re = String::from("(?i)");
    for c in pattern.chars() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            _ => re.push_str(&regex::escape(&c.to_string())),
        }
    }
    Ok(regex::Regex::new(&re)?)
}

/// Last path component of `path` (the file or folder name).
fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Pure matching core for `find`.
///
/// `entries` are pre-resolved against `url_maps`. `p` selects which host/path
/// form to display and match against (alias or real). Returns `(folders,
/// files)` where each item is a single printable path (`host + path`,
/// folders with a trailing `/`). The match is against the file/folder name
/// unless `query` contains a `/`, in which case it is against the full path.
fn find_matches(
    host: &str,
    query: &str,
    entries: &[Resolved],
    p: Perspective,
) -> anyhow::Result<(BTreeSet<String>, BTreeSet<String>)> {
    let re = glob_to_regex(query)?;
    let path_mode = query.contains('/');

    let mut folders: BTreeSet<String> = BTreeSet::new();
    let mut files: BTreeSet<String> = BTreeSet::new();

    for r in entries {
        if !r.host_matches(host) {
            continue;
        }
        let candidate = if path_mode {
            r.path(p).to_string()
        } else {
            basename(r.path(p)).to_string()
        };
        if re.is_match(&candidate) {
            files.insert(format!("{}{}", r.host(p), r.path(p)));
        }

        let comps: Vec<&str> = r
            .path(p)
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        let mut acc = String::new();
        for c in &comps[..comps.len().saturating_sub(1)] {
            acc.push('/');
            acc.push_str(c);
            let folder_candidate = if path_mode { acc.clone() } else { c.to_string() };
            if re.is_match(&folder_candidate) || re.is_match(&c) {
                folders.insert(format!("{}{}/", r.host(p), acc));
            }
        }
    }

    Ok((folders, files))
}

async fn cmd_find(
    dir: &PathBuf,
    maps: &[UrlMap],
    host: &str,
    query: &str,
) -> anyhow::Result<()> {
    if !dir.join("cache.db").exists() {
        println!("cache is empty or not initialized at {}", dir.display());
        return Ok(());
    }
    let cache = Cache::new(dir.clone(), u64::MAX)?;
    let entries = cache.list_all().await?;

    let p = perspective_for(host, maps);
    let resolved: Vec<Resolved> = entries
        .iter()
        .map(|e| Resolved::new(&e.url, maps))
        .collect();

    let (folders, files) = find_matches(host, query, &resolved, p)?;

    if folders.is_empty() && files.is_empty() {
        println!("no matches for host '{}' query '{}'", host, query);
        return Ok(());
    }

    println!(
        "Matches in {} (query '{}'): {} folders, {} files",
        host,
        query,
        folders.len(),
        files.len()
    );
    for f in &folders {
        println!("  {}", f);
    }
    for f in &files {
        println!("  {}", f);
    }
    Ok(())
}

async fn cmd_info(dir: &PathBuf, maps: &[UrlMap], url: &str) -> anyhow::Result<()> {
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
            let r = Resolved::new(&d.url, maps);
            let fresh = time_until_expiry_map(d.cached_at, &d.headers, 86400);
            let content_type = d.headers.get("content-type").map(String::as_str).unwrap_or("-");
            let fresh_str = match fresh {
                Some(s) if s > 0 => format!("{}s remaining", s),
                _ => "expired / not cacheable".to_string(),
            };
            // `info` is queried by the real cached URL, so only the real
            // host/path is shown (no alias).
            println!("URL:         {}", d.url);
            println!("Host:        {}", r.real_host);
            println!("Path:        {}", r.real_path);
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

async fn cmd_clear(
    dir: &PathBuf,
    maps: &[UrlMap],
    target: Option<String>,
    yes: bool,
) -> anyhow::Result<()> {
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
            let p = perspective_for(&tgt.host, maps);
            let entries = cache.list_all().await?;
            let matches = |e: &CachedFile| -> bool {
                let r = Resolved::new(&e.url, maps);
                r.host_matches(&tgt.host)
                    && tgt
                        .path_prefix
                        .as_ref()
                        .is_none_or(|pre| r.path(p).starts_with(pre))
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

    #[test]
    fn test_print_tree_hides_files_by_default() {
        // Files remain in the tree model regardless of printing; the default
        // behaviour of `print_tree` (show_files == false) must omit file
        // lines while keeping directory aggregation intact.
        let mut root = TreeNode::new("host".to_string(), true);
        root.insert("/pool/main/a.deb", 100);
        root.insert("/pool/main/b.deb", 200);
        assert_eq!(root.file_count, 2);
        let pool = &root.children["pool"];
        assert_eq!(pool.file_count, 2);
        let main = &pool.children["main"];
        assert!(main.children.contains_key("a.deb"));
        assert!(main.children.contains_key("b.deb"));
    }

    #[test]
    fn test_glob_to_regex_wildcards() {
        let re = glob_to_regex("*.deb").unwrap();
        assert!(re.is_match("a.deb"));
        assert!(re.is_match("pool/main/a.deb"));
        assert!(!re.is_match("a.rpm"));

        let re = glob_to_regex("pool/??in").unwrap();
        assert!(re.is_match("pool/main"));
        assert!(!re.is_match("pool/again"));

        // Case-insensitive.
        let re = glob_to_regex("MAIN").unwrap();
        assert!(re.is_match("Main"));
    }

    #[test]
    fn test_find_name_mode() {
        let mk = |host: &str, path: &str| Resolved::new(&format!("http://{}{}", host, path), &[]);
        let entries = vec![
            mk("deb.debian.org", "/pool/main/a.deb"),
            mk("deb.debian.org", "/pool/main/b.deb"),
            mk("deb.debian.org", "/pool/unstable/x.deb"),
            mk("other.host", "/pool/main/a.deb"),
        ];
        // Name-only: `main` matches the folder, not the files.
        let (folders, files) =
            find_matches("deb.debian.org", "main", &entries, Perspective::Real).unwrap();
        assert_eq!(
            folders,
            BTreeSet::from(["deb.debian.org/pool/main/".to_string()])
        );
        assert!(files.is_empty());

        // Wildcard name: `*.deb` matches all cached files, no folder.
        let (folders, files) =
            find_matches("deb.debian.org", "*.deb", &entries, Perspective::Real).unwrap();
        assert!(folders.is_empty());
        assert_eq!(files.len(), 3);
        assert!(files.contains("deb.debian.org/pool/unstable/x.deb"));

        // Host filter is honoured: a host with no entries yields nothing.
        let (folders, files) =
            find_matches("example.com", "main", &entries, Perspective::Real).unwrap();
        assert!(folders.is_empty());
        assert!(files.is_empty());
    }

    #[test]
    fn test_find_path_mode() {
        let mk = |host: &str, path: &str| Resolved::new(&format!("http://{}{}", host, path), &[]);
        let entries = vec![
            mk("deb.debian.org", "/pool/main/a.deb"),
            mk("deb.debian.org", "/pool/main/b.deb"),
            mk("deb.debian.org", "/dists/unstable/InRelease"),
        ];
        // Path pattern: matches both the folder path and file paths.
        let (folders, files) =
            find_matches("deb.debian.org", "pool/main", &entries, Perspective::Real).unwrap();
        assert_eq!(
            folders,
            BTreeSet::from(["deb.debian.org/pool/main/".to_string()])
        );
        assert_eq!(files.len(), 2);
        assert!(files.contains("deb.debian.org/pool/main/a.deb"));

        // Glob path pattern with a slash enables path matching.
        let (folders, files) =
            find_matches("deb.debian.org", "pool/*", &entries, Perspective::Real).unwrap();
        assert_eq!(
            folders,
            BTreeSet::from(["deb.debian.org/pool/main/".to_string()])
        );
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_find_perspective() {
        let maps = vec![UrlMap::parse("f=http://real.com/base").unwrap()];
        let mk = |url: &str| Resolved::new(url, &maps);
        let entries = vec![
            mk("http://real.com/base/pool/a.deb"),
            mk("http://real.com/base/pool/b.deb"),
        ];

        // Query by alias → alias-only output (no real host leaked).
        let (_, files) = find_matches("f", "*.deb", &entries, Perspective::Alias).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|f| f.starts_with("f/")));
        assert!(!files.iter().any(|f| f.contains("real.com")));

        // Query by real host → real-only output (no alias leaked).
        let (_, files) = find_matches("real.com", "*.deb", &entries, Perspective::Real).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|f| f.starts_with("real.com/")));
        assert!(!files.iter().any(|f| f.starts_with("f/")));
    }

    #[test]
    fn test_reverse_resolve() {
        let maps = vec![
            UrlMap::parse("f=http://real.com/base").unwrap(),
            UrlMap::parse("ftp-f=ftp://real.ftp/pub").unwrap(),
        ];

        // Match: real base + path → fake host + leftover path.
        assert_eq!(
            reverse_resolve("http://real.com/base/foo/bar.deb", &maps),
            ("f".to_string(), "/foo/bar.deb".to_string())
        );

        // Root (no path after base) → fake host, empty path.
        assert_eq!(
            reverse_resolve("http://real.com/base", &maps),
            ("f".to_string(), "".to_string())
        );

        // Trailing slash on base stripped: still matches.
        assert_eq!(
            reverse_resolve("http://real.com/base/", &maps),
            ("f".to_string(), "/".to_string())
        );

        // Prefix that is only a string prefix, not a path boundary, must NOT match.
        assert_eq!(
            reverse_resolve("http://real.com/baseball", &maps),
            (
                "real.com".to_string(),
                "/baseball".to_string()
            )
        );

        // FTP mapping.
        assert_eq!(
            reverse_resolve("ftp://real.ftp/pub/file.iso", &maps),
            ("ftp-f".to_string(), "/file.iso".to_string())
        );

        // No mapping → falls back to real host/path.
        assert_eq!(
            reverse_resolve("http://other.com/x", &maps),
            ("other.com".to_string(), "/x".to_string())
        );

        // First match wins.
        let maps2 = vec![
            UrlMap::parse("a=http://first.com").unwrap(),
            UrlMap::parse("b=http://first.com/x").unwrap(),
        ];
        assert_eq!(
            reverse_resolve("http://first.com/x", &maps2),
            ("a".to_string(), "/x".to_string())
        );
    }

    #[test]
    fn test_resolved_host_and_path_match() {
        let maps = vec![UrlMap::parse("f=http://real.com/base").unwrap()];

        // Entry under the mapped base: matches either alias or real host.
        let r = Resolved::new("http://real.com/base/pool/a.deb", &maps);
        assert!(r.host_matches("f"));
        assert!(r.host_matches("real.com"));
        assert!(!r.host_matches("other"));
        assert!(r.path(Perspective::Alias).starts_with("/pool"));
        assert!(r.path(Perspective::Real).starts_with("/base/pool"));

        // No mapping: alias equals real, matches by real host only.
        let r2 = Resolved::new("http://other.com/pool/a.deb", &[]);
        assert!(r2.host_matches("other.com"));
        assert!(!r2.host_matches("f"));
    }
}
