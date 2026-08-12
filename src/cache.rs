//! SQLite-backed disk cache with LRU eviction.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use http::HeaderMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{info, warn};

use rusqlite::Connection;

const CREATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS cache_entries (
    url_hash TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    size INTEGER NOT NULL,
    last_access INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    headers TEXT NOT NULL DEFAULT '{}',
    cached_at INTEGER NOT NULL DEFAULT 0
)";

const SELECT_ENTRY: &str = "SELECT file_path, last_access, headers, cached_at FROM cache_entries WHERE url_hash = ?1";
const UPDATE_ACCESS: &str = "UPDATE cache_entries SET last_access = ?1 WHERE url_hash = ?2";
const UPDATE_REFRESH: &str = "UPDATE cache_entries SET headers = ?1, cached_at = ?2 WHERE url_hash = ?3";
const INSERT_ENTRY: &str = "INSERT OR REPLACE INTO cache_entries (url_hash, url, size, last_access, file_path, headers, cached_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";
const DELETE_ENTRY: &str = "DELETE FROM cache_entries WHERE url_hash = ?1";
const SELECT_TOTAL_SIZE: &str = "SELECT COALESCE(SUM(size), 0) FROM cache_entries";

const EVICT_OLDEST: &str = "SELECT url_hash, url, size, file_path FROM cache_entries ORDER BY last_access ASC LIMIT 1";

#[derive(Serialize, Deserialize)]
struct StoredHeaders {
    #[serde(flatten)]
    inner: HashMap<String, String>,
}

fn headers_to_map(headers: &HeaderMap) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for key in ["content-type", "content-disposition", "accept-ranges", "last-modified", "etag", "cache-control", "expires"] {
        if let Some(val) = headers.get(key) {
            if let Ok(s) = val.to_str() {
                map.insert(key.to_string(), s.to_string());
            }
        }
    }
    map
}

fn map_to_headers(map: HashMap<String, String>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (k, v) in map {
        if let Ok(val) = http::HeaderValue::from_str(&v) {
            headers.insert(http::HeaderName::from_bytes(k.as_bytes()).unwrap(), val);
        }
    }
    headers
}

pub struct Cache {
    dir: PathBuf,
    max_size: u64,
    conn: Arc<Mutex<Connection>>,
}

impl Cache {
    pub fn new(dir: PathBuf, max_size: u64) -> anyhow::Result<Arc<Self>> {
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }

        let db_path = dir.join("cache.db");
        let conn = Connection::open(&db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        conn.execute(CREATE_TABLE, [])?;
        migrate_schema(&conn)?;

        info!(
            path = %db_path.display(),
            "cache database opened"
        );

        Ok(Arc::new(Self {
            dir,
            max_size,
            conn: Arc::new(Mutex::new(conn)),
        }))
    }

    fn hash_url(url: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(url.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub async fn lookup(&self, url: &str) -> Option<(PathBuf, HeaderMap)> {
        self.lookup_entry(url)
            .await
            .map(|(path, headers, _)| (path, headers))
    }

    /// Like [`Self::lookup`], but also returns the `cached_at` timestamp (seconds since epoch).
    pub async fn lookup_entry(&self, url: &str) -> Option<(PathBuf, HeaderMap, i64)> {
        let hash = Self::hash_url(url);
        let dir = self.dir.clone();
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();

            let result: Result<(String, i64, String, i64), _> = conn.query_row(
                SELECT_ENTRY,
                [&hash],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            );

            match result {
                Ok((file_path, _last_access, headers_json, cached_at)) => {
                    let full_path = dir.join(&file_path);
                    if full_path.exists() {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64;
                        let _ = conn.execute(UPDATE_ACCESS, rusqlite::params![now, hash]);

                        let headers = serde_json::from_str::<StoredHeaders>(&headers_json)
                            .map(|s| map_to_headers(s.inner))
                            .unwrap_or_default();

                        return Some((full_path, headers, cached_at));
                    }
                    let _ = conn.execute(DELETE_ENTRY, [&hash]);
                    None
                }
                Err(_) => None,
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Update stored headers and freshness timestamp after a successful revalidation.
    pub async fn refresh(&self, url: &str, headers: &HeaderMap) -> anyhow::Result<()> {
        let hash = Self::hash_url(url);
        let headers_json = serde_json::to_string(&StoredHeaders {
            inner: headers_to_map(headers),
        })?;

        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            conn.execute(UPDATE_REFRESH, rusqlite::params![headers_json, now, hash])?;
            anyhow::Ok(())
        })
        .await
        .ok()
        .unwrap_or_else(|| Err(anyhow::anyhow!("spawn_blocking panicked")))
    }

    pub async fn store(&self, url: &str, temp_path: &Path, headers: &HeaderMap) -> anyhow::Result<PathBuf> {
        let hash = Self::hash_url(url);
        let file_name = format!("{}/{}", &hash[..2], hash);
        let final_path = self.dir.join(&file_name);

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        fs::rename(temp_path, &final_path).await?;

        let size = fs::metadata(&final_path).await?.len() as i64;

        let headers_json = serde_json::to_string(&StoredHeaders {
            inner: headers_to_map(headers),
        })?;

        let conn = self.conn.clone();
        let url_owned = url.to_string();
        let file_name_owned = file_name.clone();
        let dir = self.dir.clone();
        let max_size = self.max_size;

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            conn.execute(
                INSERT_ENTRY,
                rusqlite::params![&hash, &url_owned, size, now, &file_name_owned, &headers_json, now],
            )?;

            evict_inner(&conn, &dir, max_size);

            info!(url = %url_owned, size = size, "cached");

            anyhow::Ok(())
        })
        .await
        .ok()
        .unwrap_or(Err(anyhow::anyhow!("spawn_blocking panicked")))?;

        Ok(final_path)
    }
}

fn migrate_schema(conn: &Connection) -> anyhow::Result<()> {
    let mut has_cached_at = false;
    let mut stmt = conn.prepare("PRAGMA table_info(cache_entries)")?;
    let cols = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for col in cols {
        if col? == "cached_at" {
            has_cached_at = true;
            break;
        }
    }
    drop(stmt);

    if !has_cached_at {
        conn.execute_batch(
            "ALTER TABLE cache_entries ADD COLUMN cached_at INTEGER NOT NULL DEFAULT 0;
             UPDATE cache_entries SET cached_at = last_access WHERE cached_at = 0;",
        )?;
        info!("cache schema migrated: added cached_at column");
    }
    Ok(())
}

/// Decide whether a cached entry is fresh enough to serve without contacting upstream.
///
/// Priority: `Cache-Control: max-age` → `Expires` → configured `max_cache_age` TTL.
/// `Cache-Control: no-cache` / `no-store` force revalidation (`false`).
pub fn is_fresh(cached_at: i64, headers: &HeaderMap, max_cache_age: u64) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    if cached_at <= 0 || now < cached_at {
        return false;
    }
    let age = now - cached_at;

    if let Some(cc) = headers.get("cache-control").and_then(|v| v.to_str().ok()) {
        if cc.split(',').any(|d| {
            let d = d.trim();
            d == "no-cache" || d == "no-store"
        }) {
            return false;
        }
        if let Some(max_age) = parse_max_age(cc) {
            return age < max_age as i64;
        }
    }

    if let Some(expires) = headers.get("expires").and_then(|v| v.to_str().ok()) {
        if let Ok(expires_at) = httpdate::parse_http_date(expires) {
            let expires_secs = expires_at
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            return now < expires_secs;
        }
    }

    max_cache_age > 0 && age < max_cache_age as i64
}

/// Extract `max-age=N` from a `Cache-Control` header value.
fn parse_max_age(cache_control: &str) -> Option<u64> {
    for directive in cache_control.split(',') {
        let directive = directive.trim();
        if let Some(rest) = directive.strip_prefix("max-age=") {
            let rest = rest.trim();
            if let Some(plus) = rest.strip_prefix('+') {
                return plus.parse().ok();
            }
            return rest.parse().ok();
        }
    }
    None
}

/// Compare the `etag` of a revalidation response with the stored one.
pub fn etag_equals(stored: &HeaderMap, response: &HeaderMap) -> bool {
    match (stored.get("etag"), response.get("etag")) {
        (Some(a), Some(b)) => a.as_bytes() == b.as_bytes(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_url_consistency() {
        let h1 = Cache::hash_url("http://example.com/file.deb");
        let h2 = Cache::hash_url("http://example.com/file.deb");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // SHA-256 hex
    }

    #[test]
    fn test_hash_url_different() {
        let h1 = Cache::hash_url("http://example.com/a.deb");
        let h2 = Cache::hash_url("http://example.com/b.deb");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_hash_url_empty_string() {
        let h = Cache::hash_url("");
        assert_eq!(h.len(), 64);
    }

    #[test]
    fn test_headers_to_map_roundtrip() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("etag", "\"abc123\"".parse().unwrap());
        headers.insert("cache-control", "max-age=3600".parse().unwrap());
        headers.insert("x-ignored", "should-not-appear".parse().unwrap());

        let map = headers_to_map(&headers);
        assert_eq!(map.get("content-type").unwrap(), "application/json");
        assert_eq!(map.get("etag").unwrap(), "\"abc123\"");
        assert_eq!(map.get("cache-control").unwrap(), "max-age=3600");
        assert!(!map.contains_key("x-ignored"));

        let restored = map_to_headers(map);
        assert_eq!(
            restored.get("content-type").unwrap().to_str().unwrap(),
            "application/json"
        );
        assert_eq!(restored.get("etag").unwrap().to_str().unwrap(), "\"abc123\"");
        assert!(restored.get("x-ignored").is_none());
    }

    #[test]
    fn test_headers_to_map_empty() {
        let headers = HeaderMap::new();
        let map = headers_to_map(&headers);
        assert!(map.is_empty());
    }

    #[test]
    fn test_headers_to_map_only_forwarded() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        headers.insert("content-disposition", "inline".parse().unwrap());
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("last-modified", "Mon, 01 Jan 2024 00:00:00 GMT".parse().unwrap());
        let map = headers_to_map(&headers);
        assert_eq!(map.len(), 4);
    }

    #[test]
    fn test_map_to_headers_invalid_value() {
        let mut map = HashMap::new();
        // null byte makes HeaderValue invalid
        map.insert("content-type".to_string(), "text\0plain".to_string());
        let headers = map_to_headers(map);
        assert!(headers.get("content-type").is_none());
    }

    #[test]
    fn test_stored_headers_serde_roundtrip() {
        let mut inner = HashMap::new();
        inner.insert("content-type".to_string(), "text/html".to_string());
        let stored = StoredHeaders { inner };
        let json = serde_json::to_string(&stored).unwrap();
        let restored: StoredHeaders = serde_json::from_str(&json).unwrap();
        assert_eq!(
            restored.inner.get("content-type").unwrap(),
            "text/html"
        );
    }

    #[test]
    fn test_stored_headers_empty_json() {
        let restored: StoredHeaders = serde_json::from_str("{}").unwrap();
        assert!(restored.inner.is_empty());
    }

    #[tokio::test]
    async fn test_cache_new_creates_dir() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-new");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        assert!(dir.exists());
        assert!(dir.join("cache.db").exists());
        std::fs::remove_dir_all(&dir).ok();
        // keep cache reference alive until cleanup
        drop(cache);
    }

    #[tokio::test]
    async fn test_cache_lookup_miss() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-miss");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let result = cache.lookup("http://example.com/nonexistent").await;
        assert!(result.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_store_and_lookup() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-store");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/test.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let temp_path = temp_dir.join("test.download");
        std::fs::write(&temp_path, b"hello world").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "binary/octet-stream".parse().unwrap());
        let stored_path = cache.store(url, &temp_path, &headers).await.unwrap();
        assert!(stored_path.exists());
        let (lookup_path, lookup_headers) = cache.lookup(url).await.unwrap();
        assert_eq!(lookup_path, stored_path);
        assert_eq!(
            lookup_headers.get("content-type").unwrap().to_str().unwrap(),
            "binary/octet-stream"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_lookup_stale_file() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-stale");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/stale.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let temp_path = temp_dir.join("stale.download");
        std::fs::write(&temp_path, b"data").unwrap();
        let stored_path = cache.store(url, &temp_path, &HeaderMap::new()).await.unwrap();
        // Remove the file manually to simulate stale entry
        std::fs::remove_file(&stored_path).unwrap();
        let result = cache.lookup(url).await;
        assert!(result.is_none());
        // Verify DB entry was also cleaned up
        let hash = Cache::hash_url(url);
        let db_exists: bool = {
            let conn = cache.conn.lock().unwrap();
            conn.query_row(
                "SELECT COUNT(*) FROM cache_entries WHERE url_hash = ?1",
                [&hash],
                |r| r.get::<_, i64>(0),
            ).unwrap() > 0
        };
        assert!(
            !db_exists,
            "BUG: stale DB entry was not deleted after failed file existence check"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_store_updates_existing() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-update");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/replace.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        // First store
        let p1 = temp_dir.join("v1.download");
        std::fs::write(&p1, b"version1").unwrap();
        let stored1 = cache.store(url, &p1, &HeaderMap::new()).await.unwrap();
        // Second store (same URL, different content)
        let p2 = temp_dir.join("v2.download");
        std::fs::write(&p2, b"version2").unwrap();
        let stored2 = cache.store(url, &p2, &HeaderMap::new()).await.unwrap();
        // Path should be the same (since hash is same)
        assert_eq!(stored1, stored2);
        // Content should be updated
        let content = std::fs::read(&stored2).unwrap();
        assert_eq!(content, b"version2");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_eviction_at_max_size() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-evict");
        let _ = std::fs::remove_dir_all(&dir);
        // max_size = 100 bytes — first file (4 bytes) fits, second triggers eviction of first
        let cache = Cache::new(dir.clone(), 100).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        // Store url A
        let pa = temp_dir.join("a.download");
        std::fs::write(&pa, b"aaaa").unwrap();
        cache.store("http://example.com/a.deb", &pa, &HeaderMap::new()).await.unwrap();
        // Lookup A should work
        assert!(cache.lookup("http://example.com/a.deb").await.is_some());
        // Store url B — A should be evicted (total will exceed 100)
        let pb = temp_dir.join("b.download");
        std::fs::write(&pb, b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        cache.store("http://example.com/b.deb", &pb, &HeaderMap::new()).await.unwrap();
        // A should be gone
        assert!(cache.lookup("http://example.com/a.deb").await.is_none());
        // Verify A's file is also removed from disk
        let a_hash = Cache::hash_url("http://example.com/a.deb");
        let a_path = dir.join(format!("{}/{}", &a_hash[..2], &a_hash));
        assert!(
            !a_path.exists(),
            "BUG: evicted entry's file still exists on disk at {}",
            a_path.display()
        );
        // B should be present
        assert!(cache.lookup("http://example.com/b.deb").await.is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_zero_max_size() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-zero");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 0).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let pa = temp_dir.join("z.download");
        std::fs::write(&pa, b"zero").unwrap();
        cache.store("http://example.com/z.deb", &pa, &HeaderMap::new()).await.unwrap();
        // With max_size=0, eviction must remove every entry immediately.
        let result = cache.lookup("http://example.com/z.deb").await;
        assert!(
            result.is_none(),
            "BUG: lookup returned Some after store with max_size=0 — entry should have been evicted"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_concurrent_store() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-concurrent");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 100_000).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let mut handles = Vec::new();
        for i in 0..10 {
            let c = cache.clone();
            let td = temp_dir.clone();
            handles.push(tokio::spawn(async move {
                let url = format!("http://example.com/{}.deb", i);
                let p = td.join(format!("{}.download", i));
                std::fs::write(&p, format!("content{}", i)).unwrap();
                c.store(&url, &p, &HeaderMap::new()).await.ok()
            }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_some());
        }
        // All should be findable
        for i in 0..10 {
            let url = format!("http://example.com/{}.deb", i);
            assert!(cache.lookup(&url).await.is_some());
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_lookup_corrupted_headers_json() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-corrupted-json");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/corrupt.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let temp_path = temp_dir.join("c.download");
        std::fs::write(&temp_path, b"data").unwrap();
        cache.store(url, &temp_path, &HeaderMap::new()).await.unwrap();
        // Manually corrupt the headers JSON in the database
        let hash = Cache::hash_url(url);
        {
            let conn = cache.conn.lock().unwrap();
            conn.execute("UPDATE cache_entries SET headers = 'not valid json' WHERE url_hash = ?1", [&hash]).unwrap();
        }
        // Lookup should still succeed but with empty headers
        let (path, headers) = cache.lookup(url).await.unwrap();
        assert!(path.exists());
        assert!(headers.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_lookup_corrupted_db() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-corrupted-db");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Create a binary file where cache.db should be
        std::fs::write(dir.join("cache.db"), b"this is not a valid sqlite database").unwrap();
        // Cache::new should fail because it tries to open the DB
        let result = Cache::new(dir.clone(), 10_000);
        assert!(result.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_concurrent_store_same_url() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-concurrent-same");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 100_000).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let url = "http://example.com/same-url.deb";
        let mut handles = Vec::new();
        for i in 0..10 {
            let c = cache.clone();
            let td = temp_dir.clone();
            let u = url.to_string();
            handles.push(tokio::spawn(async move {
                let p = td.join(format!("s{}.download", i));
                std::fs::write(&p, format!("content{}", i)).unwrap();
                c.store(&u, &p, &HeaderMap::new()).await.ok()
            }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_some());
        }
        // One final lookup — should succeed
        let result = cache.lookup(url).await;
        assert!(result.is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_multi_eviction() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-multi-evict");
        let _ = std::fs::remove_dir_all(&dir);
        // max_size small — store 5 files, only 1 should remain
        let cache = Cache::new(dir.clone(), 10).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        for i in 0..5 {
            let url = format!("http://example.com/{}.deb", i);
            let p = temp_dir.join(format!("{}.download", i));
            std::fs::write(&p, b"1234567890").unwrap();
            cache.store(&url, &p, &HeaderMap::new()).await.unwrap();
        }
        // At most 1 entry should survive (each is 10 bytes, max_size=10)
        let mut count = 0;
        for i in 0..5 {
            let url = format!("http://example.com/{}.deb", i);
            if cache.lookup(&url).await.is_some() {
                count += 1;
            }
        }
        assert!(count == 1, "BUG: expected exactly 1 cached entry after eviction, got {} — possible off-by-one in eviction loop", count);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_store_nonexistent_temp() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-nonexistent-temp");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let result = cache.store(
            "http://example.com/nonexistent-temp.deb",
            std::path::Path::new("/nonexistent/path/file.deb"),
            &HeaderMap::new(),
        ).await;
        assert!(result.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_lookup_updates_last_access() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-access");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/access.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let temp_path = temp_dir.join("a.download");
        std::fs::write(&temp_path, b"data").unwrap();
        cache.store(url, &temp_path, &HeaderMap::new()).await.unwrap();
        let hash = Cache::hash_url(url);
        // Get initial last_access
        let initial_access: i64 = {
            let conn = cache.conn.lock().unwrap();
            conn.query_row("SELECT last_access FROM cache_entries WHERE url_hash = ?1", [&hash], |r| r.get(0)).unwrap()
        };
        // Wait >1s to guarantee different timestamp (last_access is in seconds)
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        cache.lookup(url).await.unwrap();
        // Last access must be strictly greater
        let new_access: i64 = {
            let conn = cache.conn.lock().unwrap();
            conn.query_row("SELECT last_access FROM cache_entries WHERE url_hash = ?1", [&hash], |r| r.get(0)).unwrap()
        };
        assert!(
            new_access > initial_access,
            "BUG: last_access was not updated (was {}, now {}). UPDATE access query may be missing or not executed.",
            initial_access, new_access
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_reopen_persists() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-reopen");
        let _ = std::fs::remove_dir_all(&dir);
        let url = "http://example.com/persist.deb";
        // Store on first instance
        {
            let cache = Cache::new(dir.clone(), 10_000).unwrap();
            let temp_dir = dir.join("tmp");
            std::fs::create_dir_all(&temp_dir).unwrap();
            let temp_path = temp_dir.join("p.download");
            std::fs::write(&temp_path, b"persist").unwrap();
            cache.store(url, &temp_path, &HeaderMap::new()).await.unwrap();
        }
        // Open again and lookup
        {
            let cache = Cache::new(dir.clone(), 10_000).unwrap();
            let result = cache.lookup(url).await;
            assert!(result.is_some());
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_cache_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Cache>();
        assert_send::<Arc<Cache>>();
        assert_sync::<Arc<Cache>>();
    }

    #[tokio::test]
    async fn test_cache_store_empty_headers() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-empty-hdrs");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/no-headers.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let temp_path = temp_dir.join("nohdr.download");
        std::fs::write(&temp_path, b"content").unwrap();
        let stored = cache.store(url, &temp_path, &HeaderMap::new()).await.unwrap();
        assert!(stored.exists());
        let (path, headers) = cache.lookup(url).await.unwrap();
        assert_eq!(path, stored);
        assert!(headers.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_store_empty_content_length() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-no-clen");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/no-clen.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let temp_path = temp_dir.join("nocl.download");
        std::fs::write(&temp_path, b"payload").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "image/png".parse().unwrap());
        // deliberately no content-length
        cache.store(url, &temp_path, &headers).await.unwrap();
        let (_, lookup_headers) = cache.lookup(url).await.unwrap();
        assert_eq!(lookup_headers.get("content-type").unwrap().to_str().unwrap(), "image/png");
        assert!(lookup_headers.get("content-length").is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    // === Security fuzz tests ===

    #[tokio::test]
    async fn test_cache_sql_injection_in_url() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-sqli");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 100_000).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();

        // SQL injection attempt in URL
        let malicious_url = "http://example.com/'; DROP TABLE cache_entries; --.deb";
        let p = temp_dir.join("sqli.download");
        std::fs::write(&p, b"data").unwrap();
        cache.store(malicious_url, &p, &HeaderMap::new()).await.unwrap();

        // Should be findable
        let result = cache.lookup(malicious_url).await;
        assert!(result.is_some());

        // Table should still exist — store another entry
        let p2 = temp_dir.join("sqli2.download");
        std::fs::write(&p2, b"data2").unwrap();
        cache.store("http://example.com/normal.deb", &p2, &HeaderMap::new()).await.unwrap();
        assert!(cache.lookup("http://example.com/normal.deb").await.is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_sql_injection_in_url_select() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-sqli2");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 100_000).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let url = "http://example.com/ok.deb";
        let p = temp_dir.join("ok.download");
        std::fs::write(&p, b"data").unwrap();
        cache.store(url, &p, &HeaderMap::new()).await.unwrap();

        // SQL injection in lookup
        let evil = "http://example.com/' OR '1'='1";
        let result = cache.lookup(evil).await;
        assert!(result.is_none(), "SQL injection should not return cached data");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_path_traversal_in_url() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-traversal");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 100_000).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let url = "http://example.com/../../../etc/passwd";
        let p = temp_dir.join("traversal.download");
        std::fs::write(&p, b"data").unwrap();
        cache.store(url, &p, &HeaderMap::new()).await.unwrap();

        // Cache file should be stored under SHA256 hash, not path traversal
        let result = cache.lookup(url).await;
        assert!(result.is_some());
        let (path, _) = result.unwrap();
        // Path should be within cache dir, not /etc/passwd
        assert!(path.starts_with(&dir));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_special_chars_in_url() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-special");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 100_000).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let urls = vec![
            "http://example.com/file with spaces.deb",
            "http://example.com/file%00null.deb",
            "http://example.com/файл.deb",
            "http://example.com/🚀.deb",
        ];
        for url in &urls {
            let p = temp_dir.join("special.download");
            std::fs::write(&p, b"data").unwrap();
            cache.store(url, &p, &HeaderMap::new()).await.unwrap();
            let result = cache.lookup(url).await;
            assert!(result.is_some(), "failed for URL: {}", url);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_concurrent_eviction_stress() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-evict-stress");
        let _ = std::fs::remove_dir_all(&dir);
        let max_size: u64 = 500;
        let cache = Cache::new(dir.clone(), max_size).unwrap();
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut handles = Vec::new();
        for i in 0..50 {
            let c = cache.clone();
            let td = temp_dir.clone();
            handles.push(tokio::spawn(async move {
                let url = format!("http://example.com/stress-{}.deb", i);
                let p = td.join(format!("stress-{}.download", i));
                std::fs::write(&p, vec![0xAA; 100]).unwrap();
                c.store(&url, &p, &HeaderMap::new()).await.ok()
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Verify total cached size does not exceed max_size
        let total: i64 = {
            let conn = cache.conn.lock().unwrap();
            conn.query_row("SELECT COALESCE(SUM(size), 0) FROM cache_entries", [], |r| r.get(0)).unwrap()
        };
        assert!(
            (total as u64) <= max_size,
            "BUG: total cached size {} exceeds max_size {} after eviction stress",
            total, max_size
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_lookup_nonexistent() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-lookup-miss-unique");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        assert!(cache.lookup("http://example.com/never-cached.deb").await.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    // === Cache freshness (revalidation) ===

    fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn hdr_cache_control(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("cache-control", value.parse().unwrap());
        h
    }

    #[test]
    fn test_is_fresh_ttl_fallback() {
        let headers = HeaderMap::new();
        assert!(is_fresh(now_secs() - 10, &headers, 3600));
        assert!(!is_fresh(now_secs() - 7200, &headers, 3600));
    }

    #[test]
    fn test_is_fresh_ttl_zero_never_fresh() {
        let headers = HeaderMap::new();
        assert!(!is_fresh(now_secs() - 1, &headers, 0));
    }

    #[test]
    fn test_is_fresh_max_age_within_and_expired() {
        let h = hdr_cache_control("public, max-age=60");
        assert!(is_fresh(now_secs() - 10, &h, 0));
        assert!(!is_fresh(now_secs() - 120, &h, 0));
    }

    #[test]
    fn test_is_fresh_max_age_precedes_ttl() {
        // max-age=60 must take priority even when TTL is much larger
        let h = hdr_cache_control("max-age=60");
        assert!(!is_fresh(now_secs() - 3600, &h, 86400));
        assert!(is_fresh(now_secs() - 10, &h, 86400));
    }

    #[test]
    fn test_is_fresh_max_age_edge_value() {
        let h = hdr_cache_control("max-age=0");
        assert!(!is_fresh(now_secs() - 1, &h, 86400));
        let h = hdr_cache_control("max-age=+30");
        assert!(is_fresh(now_secs() - 5, &h, 0));
    }

    #[test]
    fn test_is_fresh_no_cache_forces_stale() {
        let h = hdr_cache_control("no-cache");
        assert!(!is_fresh(now_secs() - 1, &h, 86400));
        let h = hdr_cache_control("no-store");
        assert!(!is_fresh(now_secs() - 1, &h, 86400));
        let h = hdr_cache_control("max-age=3600, no-store");
        assert!(!is_fresh(now_secs() - 1, &h, 86400));
    }

    #[test]
    fn test_is_fresh_expires_within_and_expired() {
        let future = httpdate::fmt_http_date(
            std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
        );
        let past = httpdate::fmt_http_date(
            std::time::SystemTime::now() - std::time::Duration::from_secs(3600),
        );
        let mut h_future = HeaderMap::new();
        h_future.insert("expires", future.parse().unwrap());
        assert!(is_fresh(now_secs() - 10, &h_future, 0));

        let mut h_past = HeaderMap::new();
        h_past.insert("expires", past.parse().unwrap());
        assert!(!is_fresh(now_secs() - 10, &h_past, 86400));
    }

    #[test]
    fn test_is_fresh_cached_at_zero_or_future() {
        let headers = HeaderMap::new();
        assert!(!is_fresh(0, &headers, 86400), "cached_at=0 must be stale");
        assert!(!is_fresh(now_secs() + 1000, &headers, 86400), "future cached_at must be stale");
    }

    #[test]
    fn test_is_fresh_invalid_expires_ignored() {
        let mut h = HeaderMap::new();
        h.insert("expires", "not-a-date".parse().unwrap());
        // Invalid date → falls back to TTL
        assert!(is_fresh(now_secs() - 10, &h, 3600));
        assert!(!is_fresh(now_secs() - 7200, &h, 3600));
    }

    #[test]
    fn test_etag_equals() {
        let mut a = HeaderMap::new();
        a.insert("etag", "\"abc\"".parse().unwrap());
        let mut b = HeaderMap::new();
        b.insert("etag", "\"abc\"".parse().unwrap());
        assert!(etag_equals(&a, &b));

        let mut c = HeaderMap::new();
        c.insert("etag", "\"xyz\"".parse().unwrap());
        assert!(!etag_equals(&a, &c));

        // Missing on either side → not equal
        assert!(!etag_equals(&a, &HeaderMap::new()));
        assert!(!etag_equals(&HeaderMap::new(), &b));
    }

    #[test]
    fn test_parse_max_age_robustness() {
        assert_eq!(parse_max_age("max-age=60"), Some(60));
        assert_eq!(parse_max_age("public, max-age=3600, s-maxage=7200"), Some(3600));
        assert_eq!(parse_max_age("max-age=abc"), None);
        assert_eq!(parse_max_age("max-age=+"), None);
        assert_eq!(parse_max_age("max-age = 60"), None); // space before '=' → not matched
        assert_eq!(parse_max_age("no-cache"), None);
    }

    #[tokio::test]
    async fn test_lookup_entry_returns_cached_at() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-cached-at");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/cached-at.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let temp_path = temp_dir.join("t.download");
        std::fs::write(&temp_path, b"data").unwrap();
        let before = now_secs();
        cache.store(url, &temp_path, &HeaderMap::new()).await.unwrap();
        let (_, _, cached_at) = cache.lookup_entry(url).await.unwrap();
        assert!(cached_at >= before, "cached_at must be set at store time");
        assert!(cached_at <= now_secs());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_refresh_updates_headers_and_cached_at() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-refresh");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        let url = "http://example.com/refresh.deb";
        let temp_dir = dir.join("tmp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let temp_path = temp_dir.join("r.download");
        std::fs::write(&temp_path, b"data").unwrap();
        cache.store(url, &temp_path, &HeaderMap::new()).await.unwrap();

        let (_, _, cached_at_before) = cache.lookup_entry(url).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        let mut new_headers = HeaderMap::new();
        new_headers.insert("etag", "\"fresh\"".parse().unwrap());
        cache.refresh(url, &new_headers).await.unwrap();

        let (_, headers, cached_at_after) = cache.lookup_entry(url).await.unwrap();
        assert_eq!(headers.get("etag").unwrap().to_str().unwrap(), "\"fresh\"");
        assert!(
            cached_at_after > cached_at_before,
            "BUG: refresh did not update cached_at (was {}, now {})",
            cached_at_before, cached_at_after
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_cache_refresh_nonexistent_url() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-refresh-miss");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        // Refreshing a non-cached URL must not fail nor create an entry
        let result = cache.refresh("http://example.com/not-cached.deb", &HeaderMap::new()).await;
        assert!(result.is_ok());
        assert!(cache.lookup("http://example.com/not-cached.deb").await.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_migrate_old_schema_adds_cached_at() {
        let dir = std::env::temp_dir().join("apt-blitz-test-cache-migrate");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Create a DB with the OLD schema (no cached_at column)
        let db_path = dir.join("cache.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE cache_entries (
                url_hash TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                size INTEGER NOT NULL,
                last_access INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                headers TEXT NOT NULL DEFAULT '{}'
            );
            INSERT INTO cache_entries (url_hash, url, size, last_access, file_path, headers)
            VALUES ('hash', 'http://example.com/old.deb', 4, 1111, 'ha/hash', '{}');",
        ).unwrap();
        drop(conn);

        // Reopen via Cache::new → migration must add cached_at and backfill from last_access
        let cache = Cache::new(dir.clone(), 10_000).unwrap();
        drop(cache);

        let conn = Connection::open(&db_path).unwrap();
        let cached_at: i64 = conn.query_row(
            "SELECT cached_at FROM cache_entries WHERE url_hash = 'hash'",
            [],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(cached_at, 1111, "BUG: cached_at not backfilled from last_access during migration");

        // Column must now exist in schema
        let has_col: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('cache_entries') WHERE name = 'cached_at'",
            [],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(has_col, 1, "BUG: cached_at column missing after migration");
        std::fs::remove_dir_all(&dir).ok();
    }
}

fn evict_inner(conn: &Connection, dir: &Path, max_size: u64) {
    loop {
        let total: u64 = conn
            .query_row(SELECT_TOTAL_SIZE, [], |row| row.get::<_, i64>(0))
            .unwrap_or(0) as u64;
        if total <= max_size {
            return;
        }

        let result: Result<(String, String, i64, String), _> = conn.query_row(
            EVICT_OLDEST,
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        );

        if let Ok((url_hash, url, size, file_path)) = result {
            let path = dir.join(&file_path);
            if let Err(e) = std::fs::remove_file(&path) {
                warn!(path = %path.display(), error = %e, "failed to remove cached file");
            }
            let _ = conn.execute(DELETE_ENTRY, [&url_hash]);
            info!(url = %url, size = size, "evicted from cache");
        } else {
            return;
        }
    }
}
