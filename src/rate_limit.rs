use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const UNLIMITED: usize = 1_000_000;
const NANOS_PER_SEC: u64 = 1_000_000_000;

fn epoch_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Thread-safe token bucket rate limiter.
///
/// Tokens are measured in bytes. Refill happens lazily on each `try_consume`
/// call based on elapsed wall time.
#[derive(Clone)]
pub struct TokenBucket {
    inner: Arc<TokenBucketInner>,
}

struct TokenBucketInner {
    tokens: AtomicU64,
    max_tokens: u64,
    refill_rate_nanos: u64,
    last_refill_nanos: AtomicU64,
}

impl TokenBucket {
    /// Create a new bucket.
    ///
    /// * `max_tokens` — burst capacity in bytes.
    /// * `refill_per_sec` — sustained rate in bytes/sec.
    pub fn new(max_tokens: u64, refill_per_sec: u64) -> Self {
        let refill_rate_nanos = if refill_per_sec == 0 {
            0
        } else {
            NANOS_PER_SEC / refill_per_sec
        };
        Self {
            inner: Arc::new(TokenBucketInner {
                tokens: AtomicU64::new(max_tokens),
                max_tokens,
                refill_rate_nanos,
                last_refill_nanos: AtomicU64::new(epoch_nanos()),
            }),
        }
    }

    /// Create an unlimited bucket (no rate limiting).
    pub fn unlimited() -> Self {
        Self {
            inner: Arc::new(TokenBucketInner {
                tokens: AtomicU64::new(u64::MAX),
                max_tokens: u64::MAX,
                refill_rate_nanos: 0,
                last_refill_nanos: AtomicU64::new(0),
            }),
        }
    }

    /// Try to consume `amount` tokens. Returns true if tokens were available.
    pub fn try_consume(&self, amount: u64) -> bool {
        if self.inner.max_tokens == u64::MAX && self.inner.refill_rate_nanos == 0 {
            return true; // unlimited bucket
        }
        if self.inner.refill_rate_nanos > 0 {
            self.inner.refill();
        }
        let prev = self.inner.tokens.load(Ordering::Acquire);
        if prev >= amount {
            self.inner
                .tokens
                .compare_exchange(prev, prev - amount, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        } else {
            false
        }
    }

    /// Available tokens (after refill).
    pub fn available(&self) -> u64 {
        if self.inner.max_tokens == u64::MAX && self.inner.refill_rate_nanos == 0 {
            return u64::MAX;
        }
        if self.inner.refill_rate_nanos > 0 {
            self.inner.refill();
        }
        self.inner.tokens.load(Ordering::Relaxed)
    }

    /// Consume `bytes` tokens, sleeping until available or cancelled.
    /// Returns `true` if consumed, `false` if cancelled.
    pub async fn wait_consume(&self, bytes: u64, cancel: &CancellationToken) -> bool {
        if self.try_consume(bytes) {
            return true;
        }
        let mut remaining = bytes;
        while remaining > 0 {
            if cancel.is_cancelled() {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let batch = remaining.min(64 * 1024);
            if self.try_consume(batch) {
                remaining -= batch;
            }
        }
        true
    }
}

impl TokenBucketInner {
    fn refill(&self) {
        let now = epoch_nanos();
        let last = self.last_refill_nanos.load(Ordering::Acquire);
        let elapsed = now.saturating_sub(last);
        if elapsed == 0 {
            return;
        }
        let _ = self.last_refill_nanos.compare_exchange(
            last,
            now,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        let refill = (elapsed / self.refill_rate_nanos).min(self.max_tokens);
        if refill == 0 {
            return;
        }
        let prev = self.tokens.load(Ordering::Acquire);
        let new = (prev + refill).min(self.max_tokens);
        let _ = self
            .tokens
            .compare_exchange(prev, new, Ordering::AcqRel, Ordering::Relaxed);
    }
}

pub struct IpPermit {
    _global: OwnedSemaphorePermit,
    _per_ip: Option<OwnedSemaphorePermit>,
    pub per_ip_bucket: Option<TokenBucket>,
}

/// Per-IP concurrency limiter.
pub struct IpRateLimiter {
    per_ip: Mutex<HashMap<IpAddr, Arc<Semaphore>>>,
    per_ip_limit: usize,
    global: Arc<Semaphore>,
    per_ip_bandwidth: u64,
    per_ip_buckets: Mutex<HashMap<IpAddr, TokenBucket>>,
}

impl IpRateLimiter {
    /// * `per_ip_limit` — max in-flight downloads per single IP (0 = unlimited).
    /// * `global_limit` — max total concurrent connections (0 = unlimited).
    /// * `per_ip_bandwidth` — per-IP bandwidth limit in bytes/sec (0 = unlimited).
    pub fn new(per_ip_limit: usize, global_limit: usize, per_ip_bandwidth: u64) -> Self {
        Self {
            per_ip: Mutex::new(HashMap::new()),
            per_ip_limit,
            global: Arc::new(Semaphore::new(if global_limit == 0 {
                UNLIMITED
            } else {
                global_limit
            })),
            per_ip_bandwidth,
            per_ip_buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Acquire a permit for `ip`. Returns `None` if either limit is reached.
    pub async fn acquire(&self, ip: IpAddr) -> Option<IpPermit> {
        let global_permit = Arc::clone(&self.global).try_acquire_owned().ok()?;

        if self.per_ip_limit == 0 {
            let per_ip_bucket = self.get_or_create_bucket(ip).await;
            return Some(IpPermit {
                _global: global_permit,
                _per_ip: None,
                per_ip_bucket,
            });
        }

        let sem = {
            let mut map = self.per_ip.lock().await;
            map.entry(ip)
                .or_insert_with(|| Arc::new(Semaphore::new(self.per_ip_limit)))
                .clone()
        };

        match sem.try_acquire_owned() {
            Ok(permit) => {
                let per_ip_bucket = self.get_or_create_bucket(ip).await;
                Some(IpPermit {
                    _global: global_permit,
                    _per_ip: Some(permit),
                    per_ip_bucket,
                })
            }
            Err(_) => {
                warn!(ip = %ip, "per-IP connection limit reached");
                None
            }
        }
    }

    async fn get_or_create_bucket(&self, ip: IpAddr) -> Option<TokenBucket> {
        if self.per_ip_bandwidth == 0 {
            return None;
        }
        let mut buckets = self.per_ip_buckets.lock().await;
        Some(
            buckets
                .entry(ip)
                .or_insert_with(|| TokenBucket::new(self.per_ip_bandwidth, self.per_ip_bandwidth))
                .clone(),
        )
    }
}

/// Worker concurrency limiter for multithreaded downloads.
#[derive(Clone)]
pub struct WorkerLimiter {
    semaphore: Arc<Semaphore>,
}

impl WorkerLimiter {
    /// * `max_workers` — max total workers across all downloads (0 = unlimited).
    pub fn new(max_workers: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(if max_workers == 0 {
                UNLIMITED
            } else {
                max_workers
            })),
        }
    }

    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("semaphore closed unexpectedly")
    }

    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.semaphore.clone().try_acquire_owned().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_unlimited() {
        let b = TokenBucket::unlimited();
        assert!(b.try_consume(u64::MAX));
        assert_eq!(b.available(), u64::MAX);
    }

    #[test]
    fn test_token_bucket_full() {
        let b = TokenBucket::new(1000, 100);
        assert!(b.try_consume(1000));
        assert!(!b.try_consume(1));
    }

    #[test]
    fn test_token_bucket_zero_rate() {
        let b = TokenBucket::new(100, 0);
        assert!(b.try_consume(100));
        assert!(!b.try_consume(1));
    }

    #[tokio::test]
    async fn test_ip_limiter_unlimited() {
        let limiter = IpRateLimiter::new(0, 0, 0);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let permit1 = limiter.acquire(ip).await;
        assert!(permit1.is_some());
        let permit2 = limiter.acquire(ip).await;
        assert!(permit2.is_some());
    }

    #[tokio::test]
    async fn test_ip_limiter_per_ip_limit() {
        let limiter = IpRateLimiter::new(1, 0, 0);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let permit1 = limiter.acquire(ip).await;
        assert!(permit1.is_some());
        let permit2 = limiter.acquire(ip).await;
        assert!(permit2.is_none());
    }

    #[tokio::test]
    async fn test_ip_limiter_global_limit() {
        let limiter = IpRateLimiter::new(10, 1, 0);
        let ip1: IpAddr = "127.0.0.1".parse().unwrap();
        let ip2: IpAddr = "127.0.0.2".parse().unwrap();
        let permit1 = limiter.acquire(ip1).await;
        assert!(permit1.is_some());
        let permit2 = limiter.acquire(ip2).await;
        assert!(permit2.is_none());
    }

    #[tokio::test]
    async fn test_worker_limiter() {
        let limiter = WorkerLimiter::new(2);
        let p1 = limiter.acquire().await;
        let p2 = limiter.acquire().await;
        assert!(limiter.try_acquire().is_none());
        drop(p1);
        assert!(limiter.try_acquire().is_some());
        drop(p2);
    }
}
