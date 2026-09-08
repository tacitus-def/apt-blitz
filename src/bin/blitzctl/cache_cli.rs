//! Cache management subcommands for `blitzctl`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

use apt_blitz::cache::{time_until_expiry_map, Cache, CacheEntryDetail, CachedFile};
use anyhow::Context;
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};
use apt_blitz::config::{Config, UrlMap};
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
    /// Show details of a single cached file by host + exact path
    Info {
        /// Resource host (alias or real), e.g. deb.debian.org
        host: String,
        /// Exact file path within the host, e.g. pool/main/a.deb
        #[arg(default_value = "")]
        path: String,
    },
    /// Print the raw contents of a cached file to stdout
    Cat {
        /// Resource host (alias or real), e.g. deb.debian.org
        host: String,
        /// Exact file path within the host, e.g. pool/main/a.deb
        path: String,
    },
    /// Copy a cached file to the local filesystem
    Cp {
        /// Overwrite the destination file if it already exists
        #[arg(long, short)]
        force: bool,
        /// Resource host (alias or real), e.g. deb.debian.org
        host: String,
        /// Exact file path within the host, e.g. pool/main/a.deb
        path: String,
        /// Destination file, or an existing directory to place the file into
        dest: String,
    },
    /// Search files and folders within a host by partial/full match
    Find {
        /// Resource host, e.g. deb.debian.org
        host: String,
        /// Search query. Supports `*` and `?` wildcards. With a `/` it matches
        /// against the full path; otherwise against the name (last component).
        query: String,
    },
    /// Remove cached entries (all, or by host/path selector)
    Rm {
        /// Optional selector: `host` or `host/path` (prefix or exact file)
        target: Option<String>,
        /// Skip the confirmation prompt (full removal only)
        #[arg(long, short)]
        yes: bool,
    },
    /// List cached entries (like `ls` over the cache URL namespace)
    Ls {
        /// Resource host (alias or real), e.g. deb.debian.org. Empty lists
        /// all cached hosts.
        host: Option<String>,
        /// Path prefix or glob within the host, e.g. `pool/main` or
        /// `pool/*.deb`.
        #[arg(default_value = "")]
        path: String,
        /// Long format: cached_at, last access, seconds until expiry, size.
        #[arg(long, short)]
        long: bool,
        /// Human-readable sizes (e.g. 1.0 MiB). `-h` is reserved by clap for
        /// help, so this uses the long flag only.
        #[arg(long)]
        human: bool,
        /// Recurse into subdirectories.
        #[arg(long, short = 'R')]
        recursive: bool,
        /// One entry per line.
        #[arg(long, short = '1')]
        oneline: bool,
        /// Sort by name (default).
        #[arg(long, short = 'N')]
        name: bool,
        /// Sort by last access time (newest first).
        #[arg(long, short = 't')]
        time: bool,
        /// Sort by cache time (cached_at, newest first).
        #[arg(long, short = 'c')]
        cached: bool,
        /// Sort by size (largest first).
        #[arg(long, short = 'S')]
        size: bool,
        /// Reverse the sort order.
        #[arg(long, short = 'r')]
        reverse: bool,
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
        CacheCmd::Info { host, path } => cmd_info(&dir, maps, &host, &path).await,
        CacheCmd::Cat { host, path } => cmd_cat(&dir, maps, &host, &path).await,
        CacheCmd::Cp {
            force,
            host,
            path,
            dest,
        } => cmd_cp(&dir, maps, force, &host, &path, &dest).await,
        CacheCmd::Find { host, query } => cmd_find(&dir, maps, &host, &query).await,
        CacheCmd::Rm { target, yes } => cmd_rm(&dir, maps, target, yes).await,
        CacheCmd::Ls {
            host,
            path,
            long,
            human,
            recursive,
            oneline,
            name: _,
            time,
            cached,
            size,
            reverse,
        } => {
            let opts = LsOpts {
                long,
                human,
                recursive,
                oneline,
                sort: if size {
                    SortKey::Size
                } else if time {
                    SortKey::Time
                } else if cached {
                    SortKey::Cached
                } else {
                    SortKey::Name
                },
                reverse,
            };
            cmd_ls(&dir, maps, host, &path, &opts).await
        }
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

/// Strict positional interface: a `HOST` argument must be a plain hostname.
fn validate_host(host: &str) -> anyhow::Result<()> {
    if host.contains('/') {
        anyhow::bail!(
            "host '{}' must be a plain hostname; use `blitzctl cache <cmd> HOST [PATH]`",
            host
        );
    }
    Ok(())
}

/// Join a host and an optional path into a single `host[/path]` selector,
/// tolerating leading/trailing slashes in `path`.
fn join_host_path(host: &str, path: &str) -> String {
    let p = path.trim_matches('/');
    if p.is_empty() {
        host.to_string()
    } else {
        format!("{}/{}", host, p)
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

struct FileHashes {
    md5: String,
    sha1: String,
    sha256: String,
    sha512: String,
}

async fn cmd_info(
    dir: &PathBuf,
    maps: &[UrlMap],
    host: &str,
    path: &str,
) -> anyhow::Result<()> {
    if !dir.join("cache.db").exists() {
        println!("cache is empty or not initialized at {}", dir.display());
        return Ok(());
    }
    validate_host(host)?;
    if path.is_empty() {
        anyhow::bail!("path is required; use `blitzctl cache info HOST PATH`");
    }
    let cache = Cache::new(dir.clone(), u64::MAX)?;
    let details = cache.list_all_detailed().await?;
    let p = perspective_for(host, maps);
    let matches = info_matches(host, path, maps, p, &details);
    if matches.is_empty() {
        eprintln!("no cached entry for host '{}' path '{}'", host, path);
        std::process::exit(1);
    }

    let max_age = Config::max_cache_age_only();
    for d in &matches {
        if d.url != matches[0].url {
            println!();
        }
        let r = Resolved::new(&d.url, maps);
        let fresh = time_until_expiry_map(d.cached_at, &d.headers, max_age);
        let content_type = d.headers.get("content-type").map(String::as_str).unwrap_or("-");
        let fresh_str = match fresh {
            Some(s) if s > 0 => format!("{}s remaining", s),
            _ => "expired / not cacheable".to_string(),
        };
        println!("URL:         {}", d.url);
        println!("Host:        {}", r.host(p));
        println!("Path:        {}", r.path(p));
        println!("Size:        {} ({} bytes)", human_size(d.size), d.size);
        let hashes = compute_hashes(&dir.join(&d.file_path));
        match &hashes {
            Ok(h) => {
                println!("MD5:         {}", h.md5);
                println!("SHA1:        {}", h.sha1);
                println!("SHA256:      {}", h.sha256);
                println!("SHA512:      {}", h.sha512);
            }
            Err(_) => {
                println!("MD5:         - (stored file missing or unreadable)");
                println!("SHA1:        - (stored file missing or unreadable)");
                println!("SHA256:      - (stored file missing or unreadable)");
                println!("SHA512:      - (stored file missing or unreadable)");
            }
        }
        println!("Cached at:   {}", fmt_ts(d.cached_at));
        println!("Last access: {}", fmt_ts(d.last_access));
        println!("Freshness:   {}", fresh_str);
        println!("Content-Type:{}", content_type);
        println!("Stored file: {}/{}", dir.display(), d.file_path);
    }
    Ok(())
}

fn compute_hashes(path: &Path) -> anyhow::Result<FileHashes> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut md5 = Md5::new();
    let mut sha1 = Sha1::new();
    let mut sha256 = Sha256::new();
    let mut sha512 = Sha512::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        md5.update(&buf[..n]);
        sha1.update(&buf[..n]);
        sha256.update(&buf[..n]);
        sha512.update(&buf[..n]);
    }
    Ok(FileHashes {
        md5: format!("{:x}", md5.finalize()),
        sha1: format!("{:x}", sha1.finalize()),
        sha256: format!("{:x}", sha256.finalize()),
        sha512: format!("{:x}", sha512.finalize()),
    })
}

async fn cmd_cat(
    dir: &Path,
    maps: &[UrlMap],
    host: &str,
    path: &str,
) -> anyhow::Result<()> {
    let d = resolve_cached_entry(dir, maps, host, path).await?;
    let full = dir.join(&d.file_path);
    let stdout = std::io::stdout();
    let copied = pipe_file(&full, &mut stdout.lock())?;
    if copied != d.size {
        eprintln!(
            "warning: wrote {} bytes, cache entry reports {} bytes",
            copied, d.size
        );
    }
    Ok(())
}

async fn cmd_cp(
    dir: &Path,
    maps: &[UrlMap],
    force: bool,
    host: &str,
    path: &str,
    dest: &str,
) -> anyhow::Result<()> {
    let d = resolve_cached_entry(dir, maps, host, path).await?;
    let src = dir.join(&d.file_path);
    let dest_path = resolve_dest(dest, path)?;
    let copied = copy_source(&src, &dest_path, force)?;
    if copied != d.size {
        eprintln!(
            "warning: copied {} bytes, cache entry reports {} bytes",
            copied, d.size
        );
    }
    println!(
        "copied {} -> {} ({} bytes)",
        src.display(),
        dest_path.display(),
        copied
    );
    Ok(())
}

/// Load the cache and resolve a `host` + `path` selector to exactly one entry.
async fn resolve_cached_entry(
    dir: &Path,
    maps: &[UrlMap],
    host: &str,
    path: &str,
) -> anyhow::Result<CacheEntryDetail> {
    if !dir.join("cache.db").exists() {
        anyhow::bail!("cache is empty or not initialized at {}", dir.display());
    }
    validate_host(host)?;
    if path.is_empty() {
        anyhow::bail!("path is required");
    }
    let cache = Cache::new(dir.to_path_buf(), u64::MAX)?;
    let details = cache.list_all_detailed().await?;
    let p = perspective_for(host, maps);
    Ok(resolve_unique(host, path, maps, p, &details)?.clone())
}

/// Resolve a `host` + `path` selector to exactly one cached entry.
///
/// Unlike `info`, `cat`/`cp` require an unambiguous full match: zero or
/// multiple matches are both fatal. No glob patterns are supported.
fn resolve_unique<'a>(
    host: &str,
    path: &str,
    maps: &[UrlMap],
    p: Perspective,
    details: &'a [CacheEntryDetail],
) -> anyhow::Result<&'a CacheEntryDetail> {
    let matches = info_matches(host, path, maps, p, details);
    match matches.as_slice() {
        [only] => Ok(only),
        [] => anyhow::bail!("no cached entry for host '{}' path '{}'", host, path),
        many => {
            let urls: Vec<&str> = many.iter().map(|d| d.url.as_str()).collect();
            anyhow::bail!(
                "ambiguous selector: exactly one match required but '{}' '{}' \
                 matches {} entries (found {})",
                host,
                path,
                many.len(),
                urls.join(", ")
            )
        }
    }
}

/// Resolve the destination path for `cp`.
///
/// If `dest` is an existing directory, the file is placed inside it under its
/// original name. Otherwise `dest` is treated as a literal file path and its
/// parent directory must already exist.
fn resolve_dest(dest: &str, path: &str) -> anyhow::Result<PathBuf> {
    let dest_path = PathBuf::from(dest);
    if dest_path.is_dir() {
        let name = Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|n| !n.is_empty())
            .ok_or_else(|| anyhow::anyhow!("cannot derive file name from path '{}'", path))?;
        return Ok(dest_path.join(name));
    }
    if let Some(parent) = dest_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.is_dir() {
            anyhow::bail!(
                "destination directory does not exist: {}",
                parent.display()
            );
        }
    }
    Ok(dest_path)
}

/// Stream `src` into `out`, returning the number of bytes copied.
fn pipe_file(src: &Path, out: &mut dyn std::io::Write) -> anyhow::Result<u64> {
    let mut file = std::fs::File::open(src)
        .with_context(|| format!("stored file missing: {}", src.display()))?;
    Ok(std::io::copy(&mut file, out)?)
}

/// Copy `src` to `dest`, refusing to overwrite an existing file unless
/// `force` is set. Returns the number of bytes copied.
fn copy_source(src: &Path, dest: &Path, force: bool) -> anyhow::Result<u64> {
    if dest.exists() && !force {
        anyhow::bail!(
            "destination exists: {} (use --force to overwrite)",
            dest.display()
        );
    }
    let mut input = std::fs::File::open(src)
        .with_context(|| format!("cannot open source file: {}", src.display()))?;
    let mut output = std::fs::File::create(dest)
        .with_context(|| format!("cannot create destination file: {}", dest.display()))?;
    let copied = std::io::copy(&mut input, &mut output)?;
    output.flush()?;
    Ok(copied)
}

/// Collect all cached details whose perspective form matches `host` + `path`.
///
/// `host` may be a configured alias or the real upstream host. The path
/// comparison is exact and tolerant of a leading slash (as entered by the
/// user, without it).
fn info_matches<'a>(
    host: &str,
    path: &str,
    maps: &[UrlMap],
    p: Perspective,
    details: &'a [CacheEntryDetail],
) -> Vec<&'a CacheEntryDetail> {
    let want = path.trim_matches('/');
    details
        .iter()
        .filter(|d| {
            let r = Resolved::new(&d.url, maps);
            r.host_matches(host) && r.path(p).trim_start_matches('/') == want
        })
        .collect()
}

async fn cmd_rm(
    dir: &PathBuf,
    maps: &[UrlMap],
    target: Option<String>,
    yes: bool,
) -> anyhow::Result<()> {
    if !dir.join("cache.db").exists() {
        println!(
            "cache is empty or not initialized at {}; nothing to remove",
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
            println!("removed all cached entries ({} files removed)", removed);
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
// ls
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum SortKey {
    Name,
    Time,
    Cached,
    Size,
}

struct LsOpts {
    long: bool,
    human: bool,
    recursive: bool,
    oneline: bool,
    sort: SortKey,
    reverse: bool,
}

/// A single entry in an `ls` listing (file or aggregated directory).
struct LsItem {
    name: String,
    is_dir: bool,
    size: u64,
    cached_at: i64,
    last_access: i64,
    expiry: Option<u64>,
    file_count: usize,
}

/// Longest prefix of `target` ending at a `/` that contains no glob metachar,
/// i.e. the directory under which a glob pattern is applied. Returns an empty
/// string when the target has no glob metacharacters.
fn glob_base(target: &str) -> String {
    let idx = match target.find(['*', '?']) {
        Some(i) => i,
        None => return String::new(),
    };
    let before = &target[..idx];
    match before.rfind('/') {
        Some(i) => target[..=i].to_string(),
        None => String::new(),
    }
}

/// Build the immediate children (files + aggregated dirs) of `base` from the
/// already-matched `(root_path, detail)` pairs. Returns the children plus an
/// optional exact file when `base` itself matches a single cached entry.
fn collect_children(
    matched: &[(String, CacheEntryDetail)],
    base: &str,
    max_age: u64,
) -> (Vec<LsItem>, Option<CacheEntryDetail>) {
    let mut children: BTreeMap<String, LsItem> = BTreeMap::new();
    let mut exact: Option<CacheEntryDetail> = None;

        for (root, d) in matched {
        if !root.starts_with(base) {
            continue;
        }
        let rel = root[base.len()..].trim_start_matches('/');
        if rel.is_empty() {
            exact = Some((*d).clone());
            continue;
        }
        let name = match rel.split('/').next() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let is_dir = rel.contains('/');
        let entry = children.entry(name.clone()).or_insert(LsItem {
            name,
            is_dir,
            size: 0,
            cached_at: 0,
            last_access: 0,
            expiry: None,
            file_count: 0,
        });
        entry.is_dir = entry.is_dir || is_dir;
        if is_dir {
            entry.size += d.size;
            entry.file_count += 1;
            entry.last_access = entry.last_access.max(d.last_access);
            entry.cached_at = entry.cached_at.max(d.cached_at);
        } else {
            entry.size = d.size;
            entry.file_count = 1;
            entry.last_access = d.last_access;
            entry.cached_at = d.cached_at;
            entry.expiry = time_until_expiry_map(d.cached_at, &d.headers, max_age);
        }
    }

    (children.into_values().collect(), exact)
}

/// Sort items: directories always first, then by the chosen key.
fn sort_items(items: &mut [LsItem], key: SortKey, reverse: bool) {
    items.sort_by(|a, b| {
        match (a.is_dir, b.is_dir) {
            (true, false) => return std::cmp::Ordering::Less,
            (false, true) => return std::cmp::Ordering::Greater,
            _ => {}
        }
        let ord = match key {
            SortKey::Name => a.name.cmp(&b.name),
            SortKey::Time => a.last_access.cmp(&b.last_access),
            SortKey::Cached => a.cached_at.cmp(&b.cached_at),
            SortKey::Size => a.size.cmp(&b.size),
        };
        // time/cached/size default to descending (newest/largest first)
        match key {
            SortKey::Name => {
                if reverse {
                    ord.reverse()
                } else {
                    ord
                }
            }
            _ => {
                if reverse {
                    ord
                } else {
                    ord.reverse()
                }
            }
        }
    });
}

/// Print a single `ls` line for one item (long or name form).
fn print_item(item: &LsItem, long: bool, human: bool) {
    if long {
        let size = if human {
            human_size(item.size)
        } else {
            item.size.to_string()
        };
        if item.is_dir {
            println!(
                "-                -                -         {:>10}  {}/",
                size, item.name
            );
        } else {
            let cached = fmt_ts(item.cached_at);
            let access = fmt_ts(item.last_access);
            let expiry = match item.expiry {
                Some(s) if s > 0 => format!("{}s", s),
                _ => "expired".to_string(),
            };
            println!(
                "{cached}  {access}  {expiry:<8}  {:>10}  {}",
                size, item.name
            );
        }
    } else {
        let name = if item.is_dir {
            format!("{}/", item.name)
        } else {
            item.name.clone()
        };
        println!("{}", name);
    }
}

/// Print a list of names in `ls` style: one per line when `-1` or not a tty,
/// otherwise in aligned columns wrapped to the terminal (or 80) width.
fn print_names(names: &[String], oneline: bool) {
    if oneline || !std::io::stdout().is_terminal() {
        for n in names {
            println!("{}", n);
        }
        return;
    }
    let width = names.iter().map(|s| s.len()).max().unwrap_or(0) + 2;
    let width = width.max(1);
    let term_w = std::env::var("COLUMNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(80);
    let cols = std::cmp::max(1, term_w / width);
    for (i, n) in names.iter().enumerate() {
        if i > 0 && i % cols == 0 {
            println!();
        }
        print!("{:<width$}", n);
    }
    println!();
}

/// Render one directory level: header (only when recursing), entries, then
/// descend into subdirectories if `-R` was requested.
fn render_level(
    matched: &[(String, CacheEntryDetail)],
    base: &str,
    opts: &LsOpts,
    max_age: u64,
) {
    let (mut items, exact) = collect_children(matched, base, max_age);

    if let Some(d) = exact {
        // `base` is a single cached file — list just it.
        let item = LsItem {
            name: d.url.rsplit('/').next().unwrap_or(&d.url).to_string(),
            is_dir: false,
            size: d.size,
            cached_at: d.cached_at,
            last_access: d.last_access,
            expiry: time_until_expiry_map(d.cached_at, &d.headers, max_age),
            file_count: 1,
        };
        if opts.long {
            print_item(&item, true, opts.human);
        } else {
            print_item(&item, false, opts.human);
        }
        return;
    }

    if items.is_empty() {
        if opts.recursive {
            let display = if base.is_empty() {
                "cache".to_string()
            } else {
                base.to_string()
            };
            println!("{}:", display);
        }
        return;
    }

    sort_items(&mut items, opts.sort, opts.reverse);

    if opts.recursive {
        let display = if base.is_empty() {
            "cache".to_string()
        } else {
            base.to_string()
        };
        println!("{}:", display);
    }

    if opts.long {
        for it in &items {
            print_item(it, true, opts.human);
        }
    } else {
        let names: Vec<String> = items
            .iter()
            .map(|it| {
                if it.is_dir {
                    format!("{}/", it.name)
                } else {
                    it.name.clone()
                }
            })
            .collect();
        print_names(&names, opts.oneline);
    }

    if opts.recursive {
        for it in &items {
            if it.is_dir {
                let new_base = if base.is_empty() {
                    it.name.clone()
                } else {
                    format!("{}/{}", base, it.name)
                };
                render_level(matched, &new_base, opts, max_age);
            }
        }
    }
}

async fn cmd_ls(
    dir: &PathBuf,
    maps: &[UrlMap],
    host_arg: Option<String>,
    path: &str,
    opts: &LsOpts,
) -> anyhow::Result<()> {
    if !dir.join("cache.db").exists() {
        println!("cache is empty or not initialized at {}", dir.display());
        return Ok(());
    }
    let cache = Cache::new(dir.clone(), u64::MAX)?;
    let details = cache.list_all_detailed().await?;
    if details.is_empty() {
        println!("(no cached entries)");
        return Ok(());
    }

    let max_age = Config::max_cache_age_only();
    let target_str = match host_arg {
        None => String::new(),
        Some(h) => {
            validate_host(&h)?;
            join_host_path(&h, path)
        }
    };
    let normalized = normalize_target(&target_str);
    let has_glob = normalized.contains('*') || normalized.contains('?');

    // Resolve each entry to its display root and whether it matches the selector.
    let mut matched: Vec<(String, CacheEntryDetail)> = Vec::new();
    let base;

    if target_str.is_empty() {
        base = String::new();
        for d in details {
            let r = Resolved::new(&d.url, maps);
            let root = format!("{}{}", r.disp_host, r.disp_path);
            matched.push((root, d));
        }
    } else if !has_glob {
        let t = parse_target(&normalized);
        let p = perspective_for(&t.host, maps);
        base = format!("{}{}", t.host, t.path_prefix.clone().unwrap_or_default());
        for d in details {
            let r = Resolved::new(&d.url, maps);
            if !r.host_matches(&t.host) {
                continue;
            }
            let root = format!("{}{}", r.host(p), r.path(p));
            let in_scope = match &t.path_prefix {
                None => true,
                Some(pre) => {
                    root == format!("{}{}", t.host, pre)
                        || root.starts_with(&format!("{}{}/", t.host, pre))
                }
            };
            if in_scope {
                matched.push((root, d));
            }
        }
    } else {
        // glob selector
        let gb = glob_base(&normalized);
        base = gb.clone();
        let host_scope = if gb.contains('/') {
            Some(gb[..gb.find('/').unwrap()].to_string())
        } else if !gb.is_empty() {
            Some(gb.clone())
        } else {
            None
        };
        let p = host_scope
            .as_ref()
            .map(|h| perspective_for(h, maps))
            .unwrap_or(Perspective::Real);
        let re = glob_to_regex(&normalized[gb.len()..])?;
        let pattern = &normalized[gb.len()..];
        for d in details {
            let r = Resolved::new(&d.url, maps);
            let root = match &host_scope {
                Some(h) => {
                    if !r.host_matches(h) {
                        continue;
                    }
                    format!("{}{}", r.host(p), r.path(p))
                }
                None => format!("{}{}", r.disp_host, r.disp_path),
            };
            if !root.starts_with(gb.as_str()) {
                continue;
            }
            let rel = root.strip_prefix(gb.as_str()).unwrap_or(&root);
            let rel = rel.trim_start_matches('/');
            let glob_ok = if pattern.contains('/') {
                re.is_match(rel)
            } else {
                match &host_scope {
                    Some(_) => {
                        let immediate = rel.split('/').next().unwrap_or("");
                        !immediate.is_empty() && re.is_match(immediate)
                    }
                    None => {
                        let comps: Vec<&str> =
                            root.split('/').filter(|s| !s.is_empty()).collect();
                        comps.len() == 2 && re.is_match(comps[1])
                    }
                }
            };
            if glob_ok {
                matched.push((root, d));
            }
        }
    }

    if matched.is_empty() {
        if target_str.is_empty() {
            println!("(no cached entries)");
        } else if has_glob {
            println!("no entries match '{}'", target_str);
        } else {
            println!("no entries for '{}'", target_str);
        }
        return Ok(());
    }

    render_level(&matched, &base, opts, max_age);
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
    fn test_join_host_path() {
        assert_eq!(join_host_path("deb.debian.org", ""), "deb.debian.org");
        assert_eq!(
            join_host_path("deb.debian.org", "pool/main"),
            "deb.debian.org/pool/main"
        );
        assert_eq!(
            join_host_path("deb.debian.org", "/pool/main/"),
            "deb.debian.org/pool/main"
        );
        assert_eq!(join_host_path("f", "pool/*.deb"), "f/pool/*.deb");
        assert_eq!(join_host_path("deb.debian.org", "/"), "deb.debian.org");
    }

    #[test]
    fn test_validate_host_rejects_slash() {
        assert!(validate_host("deb.debian.org").is_ok());
        assert!(validate_host("f").is_ok());
        assert!(validate_host("deb.debian.org/pool").is_err());
        assert!(validate_host("http://deb.debian.org/pool").is_err());
    }

    #[test]
    fn test_glob_base() {
        assert_eq!(glob_base("deb.debian.org/*.deb"), "deb.debian.org/");
        assert_eq!(glob_base("deb.debian.org/pool/*/a.deb"), "deb.debian.org/pool/");
        assert_eq!(glob_base("*.deb"), "");
        assert_eq!(glob_base("deb.debian.org/pool/main"), "");
        assert_eq!(glob_base("a/b?c"), "a/");
        assert_eq!(glob_base("host/prefix/x*.deb"), "host/prefix/");
    }

    #[test]
    fn test_glob_base_no_trailing_slash_for_root_host() {
        // host-only glob: base is the host + trailing slash
        assert_eq!(glob_base("deb.debian.org/*.deb"), "deb.debian.org/");
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
    fn test_info_matches_real_and_alias() {
        let maps = vec![UrlMap::parse("f=http://real.com/base").unwrap()];
        let mk = |url: &str| CacheEntryDetail {
            url: url.to_string(),
            file_path: url.to_string(),
            size: 0,
            last_access: 0,
            cached_at: 0,
            headers: std::collections::HashMap::new(),
        };
        let details = vec![
            mk("http://real.com/base/pool/a.deb"),
            mk("http://real.com/base/pool/b.deb"),
            mk("http://other.com/x.deb"),
        ];

        // Real host form: path includes the mapping base prefix.
        let m = info_matches(
            "real.com",
            "base/pool/a.deb",
            &maps,
            Perspective::Real,
            &details,
        );
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].url, "http://real.com/base/pool/a.deb");

        // Alias form: host is the fake host, path is the leftover after base.
        let m = info_matches("f", "pool/a.deb", &maps, Perspective::Alias, &details);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].url, "http://real.com/base/pool/a.deb");

        // Mixed forms must not match: wrong path under the right perspective.
        let m = info_matches("real.com", "pool/a.deb", &maps, Perspective::Real, &details);
        assert!(m.is_empty());
        let m = info_matches("f", "base/pool/a.deb", &maps, Perspective::Alias, &details);
        assert!(m.is_empty());

        // Unknown host yields nothing.
        let m = info_matches("nope", "pool/a.deb", &maps, Perspective::Real, &details);
        assert!(m.is_empty());
    }

    #[test]
    fn test_info_matches_leading_slash_insensitive() {
        let mk = |url: &str| CacheEntryDetail {
            url: url.to_string(),
            file_path: url.to_string(),
            size: 0,
            last_access: 0,
            cached_at: 0,
            headers: std::collections::HashMap::new(),
        };
        let details = vec![mk("http://real.com/pool/a.deb")];

        // The path may be typed with or without a leading slash.
        let m = info_matches("real.com", "pool/a.deb", &[], Perspective::Real, &details);
        assert_eq!(m.len(), 1);
        let m = info_matches("real.com", "/pool/a.deb", &[], Perspective::Real, &details);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn test_find_leading_slash_path_is_strict() {
        // A wrong sub-path (not the exact file) must not match.
        let mk = |url: &str| CacheEntryDetail {
            url: url.to_string(),
            file_path: url.to_string(),
            size: 0,
            last_access: 0,
            cached_at: 0,
            headers: std::collections::HashMap::new(),
        };
        let details = vec![mk("http://real.com/pool/main/a.deb")];
        let m = info_matches("real.com", "pool/main", &[], Perspective::Real, &details);
        assert!(m.is_empty());
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

    #[test]
    fn test_compute_hashes_known_values() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("abc");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"abc").unwrap();
        drop(f);

        let h = compute_hashes(&path).unwrap();
        assert_eq!(h.md5, "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(h.sha1, "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            h.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            h.sha512,
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
             2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        );
    }

    #[test]
    fn test_compute_hashes_empty_and_large() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();

        let empty = dir.path().join("empty");
        std::fs::write(&empty, b"").unwrap();
        let h = compute_hashes(&empty).unwrap();
        assert_eq!(h.md5, "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(h.sha1, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            h.sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            h.sha512,
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce\
             47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        );

        // Streams a payload larger than the internal 64 KiB buffer.
        let big = dir.path().join("big");
        let mut f = std::fs::File::create(&big).unwrap();
        let block = vec![0u8; 4096];
        for _ in 0..40 {
            f.write_all(&block).unwrap();
        }
        drop(f);
        let h2 = compute_hashes(&big).unwrap();
        assert_eq!(h2.md5, "6cc3d8ecd5a9967c9227be8d17b988a6");
        assert_eq!(h2.sha1, "c6323f046be19b282c7ea235a2627b7122d8d618");
        assert_eq!(
            h2.sha256,
            "6cdd259c8ecbe61fbc369f3293c1961541386954a223b17a37899d7fd9ad42da"
        );
        assert_eq!(
            h2.sha512,
            "f3f4aa0929bcdef3fa44b24112a4dcb579e819e77316a7a2894d1de4ca0ec14f3\
             caca7308e1f331ee6916f9483a7a54048a1e035374e4b9ef5228dd4f4d03bd3"
        );
    }

    #[test]
    fn test_compute_hashes_missing_file() {
        let h = compute_hashes(Path::new("/nonexistent/definitely-missing"));
        assert!(h.is_err());
    }

    fn detail(url: &str) -> CacheEntryDetail {
        let hash = format!("{:x}", Sha256::digest(url.as_bytes()));
        CacheEntryDetail {
            url: url.to_string(),
            file_path: format!("{}/{}", &hash[..2], hash),
            size: 0,
            last_access: 0,
            cached_at: 0,
            headers: Default::default(),
        }
    }

    #[test]
    fn test_resolve_unique_single() {
        let details = vec![detail("http://deb.debian.org/pool/main/a.deb")];
        let r = resolve_unique("deb.debian.org", "/pool/main/a.deb", &[], Perspective::Real, &details);
        assert!(r.is_ok());
        assert_eq!(r.unwrap().url, "http://deb.debian.org/pool/main/a.deb");
    }

    #[test]
    fn test_resolve_unique_none() {
        let details = vec![detail("http://deb.debian.org/pool/main/a.deb")];
        let r = resolve_unique("deb.debian.org", "/pool/main/missing.deb", &[], Perspective::Real, &details);
        let msg = match r {
            Err(e) => format!("{:#}", e),
            Ok(_) => panic!("expected an error"),
        };
        assert!(msg.contains("no cached entry"), "got: {msg}");
    }

    #[test]
    fn test_resolve_unique_ambiguous() {
        // Same host+path cached under different upstream URLs (e.g. scheme/port variants).
        let details = vec![
            detail("http://deb.debian.org/pool/main/a.deb"),
            detail("https://deb.debian.org/pool/main/a.deb"),
        ];
        let r = resolve_unique("deb.debian.org", "/pool/main/a.deb", &[], Perspective::Real, &details);
        let msg = match r {
            Err(e) => format!("{:#}", e),
            Ok(_) => panic!("expected an error"),
        };
        assert!(msg.contains("ambiguous"), "got: {msg}");
    }

    #[test]
    fn test_resolve_dest_directory_and_file() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();

        // Existing directory → file inside it under the source name.
        let d = resolve_dest(sub.to_str().unwrap(), "/pool/main/a.deb").unwrap();
        assert_eq!(d, sub.join("a.deb"));

        // Plain file path → used as-is.
        let d2 = resolve_dest("out.bin", "/pool/main/b.bin").unwrap();
        assert_eq!(d2, PathBuf::from("out.bin"));

        // Missing parent directory → error.
        let d3 = resolve_dest("/nonexistent/out/child.bin", "/pool/main/b.bin");
        assert!(d3.is_err());
    }

    #[test]
    fn test_copy_source_overwrite_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.bin");
        let dest = dir.path().join("dest.bin");
        std::fs::write(&src, b"abc").unwrap();

        assert_eq!(copy_source(&src, &dest, false).unwrap(), 3);
        assert_eq!(std::fs::read(&dest).unwrap(), b"abc");

        // Existing destination without --force → error.
        assert!(copy_source(&src, &dest, false).is_err());

        // With --force → overwrites.
        assert_eq!(copy_source(&src, &dest, true).unwrap(), 3);
    }

    #[test]
    fn test_pipe_file_streams_content() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("body.bin");
        std::fs::write(&src, b"stream me").unwrap();

        let mut out = Vec::new();
        assert_eq!(pipe_file(&src, &mut out).unwrap(), 9);
        assert_eq!(out, b"stream me");

        assert!(pipe_file(Path::new("/nonexistent/missing.bin"), &mut out).is_err());
    }

    #[tokio::test]
    async fn test_cmd_cp_integration() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(dir.path().to_path_buf(), u64::MAX).unwrap();
        let content = b"payload-from-cache";
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), content).unwrap();

        let url = "http://deb.debian.org/pool/main/x.deb";
        cache.store(url, tmp.path(), &http::HeaderMap::new()).await.unwrap();

        let dest = dir.path().join("copy.deb");
        cmd_cp(dir.path(), &[], false, "deb.debian.org", "pool/main/x.deb", dest.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), content);

        // Copying again without --force must fail.
        assert!(cmd_cp(dir.path(), &[], false, "deb.debian.org", "pool/main/x.deb", dest.to_str().unwrap())
            .await
            .is_err());

        // --force overwrites.
        cmd_cp(dir.path(), &[], true, "deb.debian.org", "pool/main/x.deb", dest.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), content);
    }

    #[tokio::test]
    async fn test_cmd_cat_output_matches_entry() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(dir.path().to_path_buf(), u64::MAX).unwrap();
        let content = b"cat-me-please";
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), content).unwrap();

        let url = "http://deb.debian.org/pool/main/y.deb";
        cache.store(url, tmp.path(), &http::HeaderMap::new()).await.unwrap();

        let d = resolve_cached_entry(dir.path(), &[], "deb.debian.org", "pool/main/y.deb")
            .await
            .unwrap();
        let mut out = Vec::new();
        let full = dir.path().join(&d.file_path);
        assert_eq!(pipe_file(&full, &mut out).unwrap(), content.len() as u64);
        assert_eq!(out, content);
    }
}
