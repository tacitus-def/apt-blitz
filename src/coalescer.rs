//! In-flight request deduplication (leader/follower pattern).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::buffer::SegmentsBuffer;

/// Inner state of the coalescer, wrapped in Arc for shared ownership.
#[derive(Debug)]
struct CoalescerInner {
    inflight: Mutex<HashMap<String, Inflight>>,
    /// Tracks whether the last failure for a URL was caused by an upstream
    /// generation change (`If-Match` 412). Survives the `inflight` entry so
    /// followers that are about to retry can pick a dedicated retry budget.
    etag_failed: Mutex<HashMap<String, bool>>,
}

#[derive(Clone, Debug)]
pub struct Coalescer {
    inner: Arc<CoalescerInner>,
}

#[derive(Debug)]
enum Inflight {
    Pending(Vec<oneshot::Sender<Arc<SegmentsBuffer>>>),
    Downloading(Arc<SegmentsBuffer>),
}

/// RAII guard for a leader registration. On drop, cleans up the `Pending` entry
/// from the coalescer if the leader never successfully attached a buffer.
/// Call `commit()` to transfer ownership to the download task and prevent cleanup.
#[derive(Debug)]
pub struct LeaderGuard {
    url: String,
    coalescer: Option<Arc<Coalescer>>,
    committed: bool,
}

impl LeaderGuard {
    /// Create a no-op guard that does nothing on drop.
    fn noop() -> Self {
        Self {
            url: String::new(),
            coalescer: None,
            committed: true,
        }
    }

    /// Consume the guard, marking the leader as successfully started.
    /// The download task now owns the in-flight entry.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(coalescer) = self.coalescer.take() {
                coalescer.complete(&self.url);
            }
        }
    }
}

const MAX_INFLIGHT: usize = 1024;

#[derive(Debug)]
pub enum RegisterResult {
    Leader(LeaderGuard),
    Follower(oneshot::Receiver<Arc<SegmentsBuffer>>),
    FollowerBuffer(Arc<SegmentsBuffer>),
}

impl Default for Coalescer {
    fn default() -> Self {
        Self::new()
    }
}

impl Coalescer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CoalescerInner {
                inflight: Mutex::new(HashMap::new()),
                etag_failed: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn register(&self, url: &str) -> RegisterResult {
        let mut map = self.inner.inflight.lock().unwrap();

        if map.len() >= MAX_INFLIGHT {
            // Return a no-op guard for overflow case - nothing was inserted so nothing to clean up
            return RegisterResult::Leader(LeaderGuard::noop());
        }

        if let Some(inflight) = map.get_mut(url) {
            match inflight {
                Inflight::Downloading(buffer) => {
                    return RegisterResult::FollowerBuffer(buffer.clone());
                }
                Inflight::Pending(senders) => {
                    let (tx, rx) = oneshot::channel();
                    senders.push(tx);
                    return RegisterResult::Follower(rx);
                }
            }
        }
        map.insert(url.to_string(), Inflight::Pending(Vec::new()));
        RegisterResult::Leader(LeaderGuard {
            url: url.to_string(),
            coalescer: Some(self.clone().into()),
            committed: false,
        })
    }

    pub fn attach_buffer(&self, url: &str, buffer: Arc<SegmentsBuffer>) {
        let mut map = self.inner.inflight.lock().unwrap();
        if let Some(Inflight::Pending(senders)) = map.remove(url) {
            for sender in senders {
                let _ = sender.send(buffer.clone());
            }
            map.insert(url.to_string(), Inflight::Downloading(buffer));
        }
    }

    pub fn complete(&self, url: &str) {
        self.inner.inflight.lock().unwrap().remove(url);
        self.inner.etag_failed.lock().unwrap().remove(url);
    }

    /// Mark the in-flight download as failed. `etag_changed` records whether the
    /// failure was due to the upstream file changing mid-download so that the
    /// coalescing retry loop can apply a dedicated (larger) budget.
    pub fn fail(&self, url: &str, etag_changed: bool) {
        self.inner.inflight.lock().unwrap().remove(url);
        self.inner.etag_failed
            .lock()
            .unwrap()
            .insert(url.to_string(), etag_changed);
    }

    /// True if the most recent `fail` for `url` was caused by an upstream
    /// generation change (`If-Match` 412).
    pub fn etag_failed_for(&self, url: &str) -> bool {
        *self.inner.etag_failed.lock().unwrap().get(url).unwrap_or(&false)
    }

    /// Check if a URL currently has an in-flight entry (for testing).
    #[cfg(test)]
    pub fn has_inflight(&self, url: &str) -> bool {
        self.inner.inflight.lock().unwrap().contains_key(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dummy_buffer() -> Arc<SegmentsBuffer> {
        let dir = std::env::temp_dir().join("apt-blitz-test-coalescer");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.download", uuid::Uuid::new_v4()));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let (buffer, _rx) = SegmentsBuffer::new(1024, file, path);
        buffer
    }

    #[test]
    fn test_register_leader_first_call() {
        let c = Coalescer::new();
        let _guard = c.register("http://example.com/file");
        assert!(matches!(_guard, RegisterResult::Leader(_)));
    }

    #[test]
    fn test_register_follower_then_leader() {
        let c = Coalescer::new();
        // First call is Leader
        let _guard1 = c.register("http://example.com/a");
        assert!(matches!(_guard1, RegisterResult::Leader(_)));
        // Second call for same URL should be Follower (pending)
        let r = c.register("http://example.com/a");
        match r {
            RegisterResult::Follower(_) => {}
            _ => panic!("expected Follower"),
        }
    }

    #[test]
    fn test_register_multiple_followers() {
        let c = Coalescer::new();
        let _guard = c.register("http://example.com/multi");
        // Register 5 followers
        for _ in 0..5 {
            match c.register("http://example.com/multi") {
                RegisterResult::Follower(_) => {}
                _ => panic!("expected Follower"),
            }
        }
    }

    #[test]
    fn test_attach_buffer_sends_to_followers() {
        let c = Coalescer::new();
        let url = "http://example.com/attach";
        let _guard = c.register(url);
        // Register a follower
        let mut rx = match c.register(url) {
            RegisterResult::Follower(r) => r,
            _ => panic!("expected Follower"),
        };
        let buffer = make_dummy_buffer();
        c.attach_buffer(url, buffer.clone());
        // Follower should receive the buffer
        let received = rx.try_recv().unwrap();
        assert!(Arc::ptr_eq(&received, &buffer));
    }

    #[test]
    fn test_attach_buffer_without_followers() {
        let c = Coalescer::new();
        let url = "http://example.com/no-followers";
        let _guard = c.register(url);
        let buffer = make_dummy_buffer();
        // Should not panic — transitions from Pending to Downloading with empty sender list
        c.attach_buffer(url, buffer);
        // State should now be Downloading
        let map = c.inner.inflight.lock().unwrap();
        assert!(matches!(map.get(url).unwrap(), Inflight::Downloading(_)));
    }

    #[test]
    fn test_follower_buffer_when_already_downloading() {
        let c = Coalescer::new();
        let url = "http://example.com/already-dl";
        let _guard = c.register(url);
        let buffer = make_dummy_buffer();
        c.attach_buffer(url, buffer.clone());
        // Now register again — should get FollowerBuffer
        match c.register(url) {
            RegisterResult::FollowerBuffer(b) => {
                assert!(Arc::ptr_eq(&b, &buffer));
            }
            _ => panic!("expected FollowerBuffer"),
        }
    }

    #[test]
    fn test_complete_removes_entry() {
        let c = Coalescer::new();
        let url = "http://example.com/complete";
        let _guard = c.register(url);
        assert!(c.inner.inflight.lock().unwrap().contains_key(url));
        c.complete(url);
        assert!(!c.inner.inflight.lock().unwrap().contains_key(url));
    }

    #[test]
    fn test_complete_nonexistent_url() {
        let c = Coalescer::new();
        // Should not panic
        c.complete("http://example.com/ghost");
    }

    #[test]
    fn test_max_inflight_overflow() {
        let c = Coalescer::new();
        // Fill up to MAX_INFLIGHT unique URLs
        for i in 0..MAX_INFLIGHT {
            let url = format!("http://example.com/unique-{}", i);
            let _guard = c.register(&url);
            assert!(matches!(_guard, RegisterResult::Leader(_)));
        }
        // On the next register, we should STILL get Leader
        // because the hashmap is full, so it returns Leader as a fallback
        let _guard = c.register("http://example.com/overflow");
        assert!(matches!(_guard, RegisterResult::Leader(_)));
    }

    #[test]
    fn test_different_urls_independent() {
        let c = Coalescer::new();
        let _g1 = c.register("http://a.com/1");
        assert!(matches!(_g1, RegisterResult::Leader(_)));
        let _g2 = c.register("http://b.com/2");
        assert!(matches!(_g2, RegisterResult::Leader(_)));
        // Register same URL again gets Follower
        assert!(matches!(c.register("http://a.com/1"), RegisterResult::Follower(_)));
        // Different URL still Leader
        let _g3 = c.register("http://c.com/3");
        assert!(matches!(_g3, RegisterResult::Leader(_)));
    }

    #[test]
    fn test_attach_buffer_then_register_follower_buffer() {
        let c = Coalescer::new();
        let url = "http://example.com/abc";
        let _guard = c.register(url);
        let buffer = make_dummy_buffer();
        c.attach_buffer(url, buffer.clone());
        // FollowerBuffer
        match c.register(url) {
            RegisterResult::FollowerBuffer(b) => {
                assert!(Arc::ptr_eq(&b, &buffer));
            }
            _ => panic!("expected FollowerBuffer"),
        }
        // Complete and re-register as Leader
        c.complete(url);
        let _g2 = c.register(url);
        assert!(matches!(_g2, RegisterResult::Leader(_)));
    }

    #[tokio::test]
    async fn test_follower_oneshot_dropped() {
        let c = Coalescer::new();
        let url = "http://example.com/drop";
        let _guard = c.register(url);
        let rx = match c.register(url) {
            RegisterResult::Follower(r) => r,
            _ => panic!("expected Follower"),
        };
        // Drop the receiver without ever receiving
        drop(rx);
        // Leader should not panic when sending
        let buffer = make_dummy_buffer();
        c.attach_buffer(url, buffer);
    }

    #[test]
    fn test_complete_then_register_again() {
        let c = Coalescer::new();
        let url = "http://example.com/cycle";
        let _g1 = c.register(url);
        assert!(matches!(_g1, RegisterResult::Leader(_)));
        c.complete(url);
        let _g2 = c.register(url);
        assert!(matches!(_g2, RegisterResult::Leader(_)));
        c.complete(url);
        // Multiple completes OK
        c.complete(url);
    }

    #[tokio::test]
    async fn test_concurrent_register_same_url() {
        let c = Arc::new(Coalescer::new());
        let url = "http://example.com/concurrent-same";
        let mut handles = Vec::new();
        for _ in 0..10 {
            let c = c.clone();
            let u = url.to_string();
            handles.push(tokio::spawn(async move {
                c.register(&u)
            }));
        }
        let mut leaders = 0u32;
        let mut followers = 0u32;
        for h in handles {
            match h.await.unwrap() {
                RegisterResult::Leader(_) => leaders += 1,
                RegisterResult::Follower(_) => followers += 1,
                RegisterResult::FollowerBuffer(_) => followers += 1,
            }
        }
        // Exactly one leader, rest are followers
        assert_eq!(leaders, 1, "expected exactly 1 leader, got {}", leaders);
        assert_eq!(followers, 9, "expected 9 followers, got {}", followers);
    }

    #[test]
    fn test_attach_buffer_without_any_register() {
        let c = Coalescer::new();
        let buffer = make_dummy_buffer();
        // Should not panic (no entry in map)
        c.attach_buffer("http://example.com/never-registered", buffer);
        // Nothing should happen, the function does nothing if not Pending
    }

    #[test]
    fn test_register_attach_complete_register_attach() {
        let c = Coalescer::new();
        let url = "http://example.com/full-cycle";
        let _g1 = c.register(url);
        assert!(matches!(_g1, RegisterResult::Leader(_)));
        let buffer = make_dummy_buffer();
        c.attach_buffer(url, buffer.clone());
        c.complete(url);
        // Second cycle
        let _g2 = c.register(url);
        assert!(matches!(_g2, RegisterResult::Leader(_)));
        let buffer2 = make_dummy_buffer();
        c.attach_buffer(url, buffer2.clone());
        c.complete(url);
    }

    #[tokio::test]
    async fn test_register_complete_immediately_re_register() {
        let c = Arc::new(Coalescer::new());
        let url = "http://example.com/rapid-cycle";
        // First leader registers and completes immediately
        let _g1 = c.register(url);
        assert!(matches!(_g1, RegisterResult::Leader(_)));
        c.complete(url);
        // Second registration should also be Leader (entry was removed)
        let _g2 = c.register(url);
        assert!(matches!(_g2, RegisterResult::Leader(_)));
    }

    #[test]
    fn test_max_inflight_overflow_still_works_for_new_url() {
        let c = Coalescer::new();
        for i in 0..MAX_INFLIGHT {
            c.register(&format!("http://example.com/prefill-{}", i));
        }
        // Overflow returns Leader but does NOT insert into map
        assert!(matches!(c.register("http://example.com/overflow"), RegisterResult::Leader(_)));
        // Check that overflow URL was NOT added
        let map = c.inner.inflight.lock().unwrap();
        assert!(!map.contains_key("http://example.com/overflow"));
    }

    #[tokio::test]
    async fn test_concurrent_register_different_urls() {
        let c = Arc::new(Coalescer::new());
        let mut handles = Vec::new();
        for i in 0..100 {
            let c = c.clone();
            handles.push(tokio::spawn(async move {
                c.register(&format!("http://example.com/conc-{}", i))
            }));
        }
        for h in handles {
            assert!(matches!(h.await.unwrap(), RegisterResult::Leader(_)));
        }
    }

    #[test]
    fn test_coalescer_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Coalescer>();
        assert_send::<Arc<Coalescer>>();
        assert_sync::<Arc<Coalescer>>();
    }
}