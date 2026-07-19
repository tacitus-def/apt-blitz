//! In-flight request deduplication (leader/follower pattern).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::buffer::SegmentsBuffer;

enum Inflight {
    Pending(Vec<oneshot::Sender<Arc<SegmentsBuffer>>>),
    Downloading(Arc<SegmentsBuffer>),
}

pub struct Coalescer {
    inflight: Mutex<HashMap<String, Inflight>>,
}

const MAX_INFLIGHT: usize = 1024;

#[derive(Debug)]
pub enum RegisterResult {
    Leader,
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
            inflight: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(&self, url: &str) -> RegisterResult {
        let mut map = self.inflight.lock().unwrap();

        if map.len() >= MAX_INFLIGHT {
            return RegisterResult::Leader;
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
        RegisterResult::Leader
    }

    pub fn attach_buffer(&self, url: &str, buffer: Arc<SegmentsBuffer>) {
        let mut map = self.inflight.lock().unwrap();
        if let Some(Inflight::Pending(senders)) = map.remove(url) {
            for sender in senders {
                let _ = sender.send(buffer.clone());
            }
            map.insert(url.to_string(), Inflight::Downloading(buffer));
        }
    }

    pub fn complete(&self, url: &str) {
        self.inflight.lock().unwrap().remove(url);
    }

    pub fn fail(&self, url: &str) {
        self.inflight.lock().unwrap().remove(url);
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
        match c.register("http://example.com/file") {
            RegisterResult::Leader => {}
            _ => panic!("expected Leader"),
        }
    }

    #[test]
    fn test_register_follower_then_leader() {
        let c = Coalescer::new();
        // First call is Leader
        assert!(matches!(c.register("http://example.com/a"), RegisterResult::Leader));
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
        c.register("http://example.com/multi");
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
        c.register(url);
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
        c.register(url);
        let buffer = make_dummy_buffer();
        // Should not panic — transitions from Pending to Downloading with empty sender list
        c.attach_buffer(url, buffer);
        // State should now be Downloading
        let map = c.inflight.lock().unwrap();
        assert!(matches!(map.get(url).unwrap(), Inflight::Downloading(_)));
    }

    #[test]
    fn test_follower_buffer_when_already_downloading() {
        let c = Coalescer::new();
        let url = "http://example.com/already-dl";
        c.register(url);
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
        c.register(url);
        assert!(c.inflight.lock().unwrap().contains_key(url));
        c.complete(url);
        assert!(!c.inflight.lock().unwrap().contains_key(url));
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
            match c.register(&url) {
                RegisterResult::Leader => {}
                other => panic!("expected Leader, got {:?} at {}", other, i),
            }
        }
        // On the next register, we should STILL get Leader
        // because the hashmap is full, so it returns Leader as a fallback
        match c.register("http://example.com/overflow") {
            RegisterResult::Leader => {}
            other => panic!("expected Leader (overflow), got {:?}", other),
        }
    }

    #[test]
    fn test_different_urls_independent() {
        let c = Coalescer::new();
        assert!(matches!(c.register("http://a.com/1"), RegisterResult::Leader));
        assert!(matches!(c.register("http://b.com/2"), RegisterResult::Leader));
        // Register same URL again gets Follower
        assert!(matches!(c.register("http://a.com/1"), RegisterResult::Follower(_)));
        // Different URL still Leader
        assert!(matches!(c.register("http://c.com/3"), RegisterResult::Leader));
    }

    #[test]
    fn test_attach_buffer_then_register_follower_buffer() {
        let c = Coalescer::new();
        let url = "http://example.com/abc";
        c.register(url);
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
        assert!(matches!(c.register(url), RegisterResult::Leader));
    }

    #[tokio::test]
    async fn test_follower_oneshot_dropped() {
        let c = Coalescer::new();
        let url = "http://example.com/drop";
        c.register(url);
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
        assert!(matches!(c.register(url), RegisterResult::Leader));
        c.complete(url);
        assert!(matches!(c.register(url), RegisterResult::Leader));
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
                RegisterResult::Leader => leaders += 1,
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
        assert!(matches!(c.register(url), RegisterResult::Leader));
        let buffer = make_dummy_buffer();
        c.attach_buffer(url, buffer.clone());
        c.complete(url);
        // Second cycle
        assert!(matches!(c.register(url), RegisterResult::Leader));
        let buffer2 = make_dummy_buffer();
        c.attach_buffer(url, buffer2.clone());
        c.complete(url);
    }

    #[tokio::test]
    async fn test_register_complete_immediately_re_register() {
        let c = Arc::new(Coalescer::new());
        let url = "http://example.com/rapid-cycle";
        // First leader registers and completes immediately
        assert!(matches!(c.register(url), RegisterResult::Leader));
        c.complete(url);
        // Second registration should also be Leader (entry was removed)
        assert!(matches!(c.register(url), RegisterResult::Leader));
    }

    #[test]
    fn test_max_inflight_overflow_still_works_for_new_url() {
        let c = Coalescer::new();
        for i in 0..MAX_INFLIGHT {
            c.register(&format!("http://example.com/prefill-{}", i));
        }
        // Overflow returns Leader but does NOT insert into map
        assert!(matches!(c.register("http://example.com/overflow"), RegisterResult::Leader));
        // Check that overflow URL was NOT added
        let map = c.inflight.lock().unwrap();
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
            assert!(matches!(h.await.unwrap(), RegisterResult::Leader));
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
