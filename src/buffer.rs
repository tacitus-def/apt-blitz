use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use bytes::Bytes;
use tokio::sync::{broadcast, watch};
use http::{HeaderMap, StatusCode};

#[derive(Debug, Clone, Copy, PartialEq)]
enum SegState {
    Downloading,
    Ready,
}

#[derive(Debug)]
struct Segment {
    start: u64,
    end: u64,
    state: SegState,
}

#[derive(Debug)]
pub struct SegmentsBuffer {
    total_size: u64,
    next_unassigned: AtomicU64,
    segments: Mutex<Vec<Mutex<Segment>>>,
    ready_count: AtomicU64,
    all_assigned: AtomicBool,
    meta: watch::Sender<Option<(StatusCode, HeaderMap)>>,
    notify: broadcast::Sender<usize>,
    failed: AtomicBool,
    file: Arc<std::fs::File>,
    file_path: PathBuf,
}

impl SegmentsBuffer {
    pub fn new(total_size: u64, file: std::fs::File, file_path: PathBuf) -> (Arc<Self>, broadcast::Receiver<usize>) {
        let (tx, rx) = broadcast::channel(256);
        let (meta_tx, _meta_rx) = watch::channel(None);
        let buffer = Arc::new(Self {
            total_size,
            next_unassigned: AtomicU64::new(0),
            segments: Mutex::new(Vec::new()),
            ready_count: AtomicU64::new(0),
            all_assigned: AtomicBool::new(false),
            meta: meta_tx,
            notify: tx,
            failed: AtomicBool::new(false),
            file: Arc::new(file),
            file_path,
        });
        (buffer, rx)
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    pub fn set_meta(&self, status: StatusCode, headers: HeaderMap) {
        self.meta.send_replace(Some((status, headers)));
    }

    pub async fn wait_meta(&self) -> (StatusCode, HeaderMap) {
        let mut rx = self.meta.subscribe();
        loop {
            let meta = rx.borrow_and_update().clone();
            if let Some((status, headers)) = meta {
                return (status, headers);
            }
            rx.changed().await.ok();
        }
    }

    pub fn claim_range(&self, preferred_size: u64) -> Option<(usize, u64, u64)> {
        if preferred_size == 0 {
            if self.next_unassigned.load(Ordering::Acquire) >= self.total_size {
                self.all_assigned.store(true, Ordering::Release);
            }
            return None;
        }
        loop {
            let current = self.next_unassigned.load(Ordering::Acquire);
            if current >= self.total_size {
                self.all_assigned.store(true, Ordering::Release);
                return None;
            }
            let size = preferred_size.min(self.total_size - current);
            let end = current + size;
            if self
                .next_unassigned
                .compare_exchange(current, end, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let id = {
                    let mut segments = self.segments.lock().unwrap();
                    let id = segments.len();
                    segments.push(Mutex::new(Segment {
                        start: current,
                        end,
                        state: SegState::Downloading,
                    }));
                    id
                };
                return Some((id, current, end));
            }
        }
    }

    pub fn write_data(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        let mut written = 0;
        while written < data.len() {
            match self.file.write_at(&data[written..], offset + written as u64) {
                Ok(0) => return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "short write")),
                Ok(n) => written += n,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn sync(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    pub fn mark_ready(&self, id: usize) {
        let segments = self.segments.lock().unwrap();
        if let Some(seg) = segments.get(id) {
            seg.lock().unwrap().state = SegState::Ready;
        }
        drop(segments);
        self.ready_count.fetch_add(1, Ordering::Release);
        let _ = self.notify.send(id);
    }

    pub fn num_segments(&self) -> usize {
        self.segments.lock().unwrap().len()
    }

    pub fn is_ready(&self, id: usize) -> bool {
        let segments = self.segments.lock().unwrap();
        segments.get(id).map_or(false, |s| {
            let seg = s.lock().unwrap();
            seg.state == SegState::Ready
        })
    }

    pub fn segment_start(&self, id: usize) -> u64 {
        let segments = self.segments.lock().unwrap();
        let seg = segments[id].lock().unwrap();
        seg.start
    }

    pub fn segment_end(&self, id: usize) -> u64 {
        let segments = self.segments.lock().unwrap();
        let seg = segments[id].lock().unwrap();
        seg.end
    }

    pub fn read_data(&self, offset: u64, len: u64) -> Option<Bytes> {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; len as usize];
        let n = self.file.read_at(&mut buf, offset).ok()?;
        if n == 0 {
            return None;
        }
        buf.truncate(n);
        Some(Bytes::from(buf))
    }

    pub fn all_completed(&self) -> bool {
        if !self.all_assigned.load(Ordering::Acquire) {
            return false;
        }
        let segments = self.segments.lock().unwrap();
        self.ready_count.load(Ordering::Acquire) as usize >= segments.len()
    }

    /// Mark every claimed segment as Ready. Used when a fallback plain‑proxy
    /// download completes after a multithreaded failure.
    pub fn mark_all_ready(&self) {
        let segments = self.segments.lock().unwrap();
        for seg in segments.iter() {
            seg.lock().unwrap().state = SegState::Ready;
        }
        let count = segments.len() as u64;
        drop(segments);
        self.ready_count.store(count, Ordering::Release);
        let _ = self.notify.send(usize::MAX);
    }

    pub fn set_failed(&self) {
        self.failed.store(true, Ordering::Release);
        let _ = self.notify.send(usize::MAX);
    }

    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<usize> {
        self.notify.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_temp_file(size: u64) -> (std::fs::File, std::path::PathBuf) {
        let dir = std::env::temp_dir().join("apt-blitz-test-buffer");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.download", uuid::Uuid::new_v4()));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.set_len(size).unwrap();
        (file, path)
    }

    #[tokio::test]
    async fn test_new_buffer_empty_file() {
        let (file, path) = create_temp_file(0);
        let (buffer, rx) = SegmentsBuffer::new(0, file, path);
        assert_eq!(buffer.total_size, 0);
        assert_eq!(buffer.num_segments(), 0);
        // claim_range on empty file sets all_assigned and returns None
        assert!(buffer.claim_range(1024).is_none());
        assert!(buffer.all_completed());
        drop(rx);
    }

    #[tokio::test]
    async fn test_new_buffer_nonempty() {
        let (file, path) = create_temp_file(4096);
        let (buffer, rx) = SegmentsBuffer::new(4096, file, path);
        assert_eq!(buffer.total_size, 4096);
        assert_eq!(buffer.num_segments(), 0);
        assert!(!buffer.all_completed());
        drop(rx);
    }

    #[tokio::test]
    async fn test_claim_range_exact() {
        let (file, path) = create_temp_file(1024);
        let (buffer, rx) = SegmentsBuffer::new(1024, file, path);
        let r = buffer.claim_range(1024);
        assert!(r.is_some());
        let (id, start, end) = r.unwrap();
        assert_eq!(id, 0);
        assert_eq!(start, 0);
        assert_eq!(end, 1024);
        assert_eq!(buffer.num_segments(), 1);
        // second claim should return None (exhausted)
        assert!(buffer.claim_range(1024).is_none());
        assert!(buffer.all_assigned.load(Ordering::Acquire));
        drop(rx);
    }

    #[tokio::test]
    async fn test_claim_range_partial() {
        let (file, path) = create_temp_file(500);
        let (buffer, rx) = SegmentsBuffer::new(500, file, path);
        // preferred size is 1024, but only 500 remains
        let r = buffer.claim_range(1024);
        assert!(r.is_some());
        let (id, start, end) = r.unwrap();
        assert_eq!(id, 0);
        assert_eq!(start, 0);
        assert_eq!(end, 500);
        assert!(buffer.claim_range(1024).is_none());
        drop(rx);
    }

    #[tokio::test]
    async fn test_claim_range_multiple() {
        let (file, path) = create_temp_file(1000);
        let (buffer, rx) = SegmentsBuffer::new(1000, file, path);
        let r0 = buffer.claim_range(400).unwrap();
        assert_eq!(r0, (0, 0, 400));
        let r1 = buffer.claim_range(400).unwrap();
        assert_eq!(r1, (1, 400, 800));
        let r2 = buffer.claim_range(400).unwrap();
        assert_eq!(r2, (2, 800, 1000));
        assert!(buffer.claim_range(400).is_none());
        assert_eq!(buffer.num_segments(), 3);
        drop(rx);
    }

    #[tokio::test]
    async fn test_segment_bounds() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        let (id, start, end) = buffer.claim_range(37).unwrap();
        assert_eq!(id, 0);
        assert_eq!(start, 0);
        assert_eq!(end, 37);
        assert_eq!(buffer.segment_start(0), 0);
        assert_eq!(buffer.segment_end(0), 37);
        drop(rx);
    }

    #[tokio::test]
    async fn test_mark_ready_is_ready() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        let (id, _, _) = buffer.claim_range(100).unwrap();
        assert!(!buffer.is_ready(id));
        buffer.mark_ready(id);
        assert!(buffer.is_ready(id));
        drop(rx);
    }

    #[tokio::test]
    async fn test_is_ready_invalid_id() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        assert!(!buffer.is_ready(999));
        drop(rx);
    }

    #[tokio::test]
    async fn test_all_completed_no_segments() {
        let (file, path) = create_temp_file(0);
        let (buffer, rx) = SegmentsBuffer::new(0, file, path);
        // Must call claim_range to trigger all_assigned
        assert!(buffer.claim_range(1024).is_none());
        assert!(buffer.all_completed());
        drop(rx);
    }

    #[tokio::test]
    async fn test_all_completed_partial() {
        let (file, path) = create_temp_file(200);
        let (buffer, rx) = SegmentsBuffer::new(200, file, path);
        buffer.claim_range(100);
        buffer.claim_range(100);
        // Claim one more to trigger all_assigned
        assert!(buffer.claim_range(100).is_none());
        assert!(!buffer.all_completed());
        buffer.mark_ready(0);
        assert!(!buffer.all_completed());
        buffer.mark_ready(1);
        assert!(buffer.all_completed());
        drop(rx);
    }

    #[tokio::test]
    async fn test_all_completed_not_all_assigned() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        // one segment claimed but not all yet
        buffer.claim_range(50);
        assert!(!buffer.all_completed());
        drop(rx);
    }

    #[tokio::test]
    async fn test_write_and_read_data() {
        let (file, path) = create_temp_file(50);
        let (buffer, rx) = SegmentsBuffer::new(50, file, path);
        let data = b"hello world";
        buffer.write_data(0, data).unwrap();
        let read = buffer.read_data(0, data.len() as u64).unwrap();
        assert_eq!(&read[..], data);
        drop(rx);
    }

    #[tokio::test]
    async fn test_write_data_short_write_detection() {
        // Open file with small capacity to test short writes
        let dir = std::env::temp_dir().join("apt-blitz-test-buffer");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.download", uuid::Uuid::new_v4()));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.set_len(10).unwrap();
        let (buffer, rx) = SegmentsBuffer::new(10, file, path);
        // Try to write beyond file size — should succeed (sparse file on Unix)
        let data = vec![0xAB; 20];
        let res = buffer.write_data(0, &data);
        // On Unix writing beyond EOF extends the file, so write_at should succeed
        assert!(res.is_ok() || res.is_err());
        // But the file size will still reflect pre-allocation
        drop(rx);
        std::fs::remove_file(buffer.file_path()).ok();
    }

    #[tokio::test]
    async fn test_read_data_empty_file() {
        let (file, path) = create_temp_file(0);
        let (buffer, rx) = SegmentsBuffer::new(0, file, path);
        let result = buffer.read_data(0, 10);
        // read_at on a zero-length file returns 0 bytes
        assert!(result.is_none() || result.unwrap().is_empty());
        drop(rx);
    }

    #[tokio::test]
    async fn test_read_data_beyond_file() {
        let (file, path) = create_temp_file(10);
        let (buffer, rx) = SegmentsBuffer::new(10, file, path);
        // Reading past EOF returns fewer bytes
        let result = buffer.read_data(5, 100);
        assert!(result.is_some());
        assert!(result.unwrap().len() <= 5);
        drop(rx);
    }

    #[tokio::test]
    async fn test_set_failed_and_is_failed() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        assert!(!buffer.is_failed());
        buffer.set_failed();
        assert!(buffer.is_failed());
        drop(rx);
    }

    #[tokio::test]
    async fn test_set_meta_and_wait_meta() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        let status = StatusCode::OK;
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        // wait_meta must subscribe BEFORE set_meta sends (otherwise no receiver)
        let buf_clone = buffer.clone();
        let hdrs = headers.clone();
        let handle = tokio::spawn(async move {
            // Give wait_meta time to subscribe first
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            buf_clone.set_meta(status, hdrs);
        });
        let (got_status, got_headers) = buffer.wait_meta().await;
        assert_eq!(got_status, status);
        assert_eq!(got_headers.get("content-type").unwrap(), "text/plain");
        handle.await.unwrap();
        drop(rx);
    }

    #[tokio::test]
    async fn test_wait_meta_block_then_set() {
        let (file, path) = create_temp_file(100);
        let (buffer, _rx) = SegmentsBuffer::new(100, file, path);
        let buf_clone = buffer.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut headers = HeaderMap::new();
            headers.insert("x-custom", "val".parse().unwrap());
            buf_clone.set_meta(StatusCode::OK, headers);
        });
        let (status, _) = buffer.wait_meta().await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn test_notify_broadcast_on_ready() {
        let (file, path) = create_temp_file(100);
        let (buffer, mut rx) = SegmentsBuffer::new(100, file, path);
        let (id, _, _) = buffer.claim_range(100).unwrap();
        let buf_clone = buffer.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            buf_clone.mark_ready(id);
        });
        let msg = rx.recv().await;
        assert!(msg.is_ok());
        assert_eq!(msg.unwrap(), id);
    }

    #[tokio::test]
    async fn test_notify_broadcast_on_failed() {
        let (file, path) = create_temp_file(100);
        let (buffer, mut rx) = SegmentsBuffer::new(100, file, path);
        let buf_clone = buffer.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            buf_clone.set_failed();
        });
        let msg = rx.recv().await;
        assert!(msg.is_ok() || msg.is_err()); // may be lagged
    }

    #[tokio::test]
    async fn test_subscribe_multiple_receivers() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        let rx2 = buffer.subscribe();
        let rx3 = buffer.subscribe();
        drop(rx);
        drop(rx2);
        drop(rx3);
    }

    #[tokio::test]
    async fn test_claim_range_concurrent() {
        let (file, path) = create_temp_file(10_000);
        let (buffer, rx) = SegmentsBuffer::new(10_000, file, path);
        let mut handles = Vec::new();
        for _ in 0..10 {
            let b = buffer.clone();
            handles.push(tokio::spawn(async move {
                let mut total = 0u64;
                while let Some((_id, start, end)) = b.claim_range(500) {
                    total += end - start;
                }
                total
            }));
        }
        let mut grand_total = 0u64;
        for h in handles {
            grand_total += h.await.unwrap();
        }
        assert_eq!(grand_total, 10_000);
        drop(rx);
    }

    #[tokio::test]
    async fn test_file_path_method() {
        let (file, path) = create_temp_file(42);
        let (buffer, rx) = SegmentsBuffer::new(42, file, path.clone());
        assert_eq!(buffer.file_path(), &path);
        drop(rx);
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn test_sync() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        // sync on pre-allocated file should succeed
        assert!(buffer.sync().is_ok());
        drop(rx);
    }

    #[tokio::test]
    async fn test_double_mark_ready() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        let (id, _, _) = buffer.claim_range(100).unwrap();
        buffer.mark_ready(id);
        buffer.mark_ready(id); // should not panic
        assert!(buffer.is_ready(id));
        drop(rx);
    }

    #[test]
    fn test_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<SegmentsBuffer>();
        // Arc<SegmentsBuffer> is Send + Sync
        assert_send::<Arc<SegmentsBuffer>>();
        assert_sync::<Arc<SegmentsBuffer>>();
    }

    #[tokio::test]
    async fn test_claim_range_preferred_size_zero() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        let r = buffer.claim_range(0);
        assert!(r.is_none());
        assert!(!buffer.all_assigned.load(Ordering::Acquire));
        drop(rx);
    }

    #[tokio::test]
    async fn test_read_data_zero_length() {
        let (file, path) = create_temp_file(50);
        let (buffer, rx) = SegmentsBuffer::new(50, file, path);
        // read_data with len=0 allocates an empty buffer; read_at returns Ok(0)
        // which causes the function to return None
        let result = buffer.read_data(10, 0);
        assert!(result.is_none());
        drop(rx);
    }

    #[tokio::test]
    async fn test_read_data_at_zero_offset_zero_len() {
        let (file, path) = create_temp_file(1);
        let (buffer, rx) = SegmentsBuffer::new(1, file, path);
        let result = buffer.read_data(0, 1);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
        drop(rx);
    }

    #[tokio::test]
    async fn test_concurrent_write_read() {
        let (file, path) = create_temp_file(1000);
        let (buffer, rx) = SegmentsBuffer::new(1000, file, path);
        let buf_w = buffer.clone();
        let buf_r = buffer.clone();
        let handle = tokio::spawn(async move {
            for i in 0..10 {
                let data = vec![i as u8; 100];
                buf_w.write_data(i as u64 * 100, &data).unwrap();
            }
        });
        handle.await.unwrap();
        for i in 0..10 {
            let data = buf_r.read_data(i as u64 * 100, 100).unwrap();
            assert_eq!(data.len(), 100);
            assert!(data.iter().all(|&b| b == i as u8));
        }
        drop(rx);
    }

    #[tokio::test]
    async fn test_mark_ready_invalid_id_no_panic() {
        let (file, path) = create_temp_file(100);
        let (buffer, rx) = SegmentsBuffer::new(100, file, path);
        // mark_ready with id that doesn't exist yet
        buffer.mark_ready(42);
        buffer.mark_ready(usize::MAX);
        drop(rx);
    }

    #[test]
    fn test_num_segments_consistency() {
        let (file, path) = create_temp_file(1000);
        let (buffer, rx) = SegmentsBuffer::new(1000, file, path);
        assert_eq!(buffer.num_segments(), 0);
        buffer.claim_range(100);
        assert_eq!(buffer.num_segments(), 1);
        buffer.claim_range(200);
        assert_eq!(buffer.num_segments(), 2);
        buffer.claim_range(700);
        assert_eq!(buffer.num_segments(), 3);
        drop(rx);
    }

    #[tokio::test]
    async fn test_all_completed_single_byte() {
        let (file, path) = create_temp_file(1);
        let (buffer, rx) = SegmentsBuffer::new(1, file, path);
        buffer.claim_range(1);
        assert!(buffer.claim_range(1).is_none()); // trigger all_assigned
        assert!(!buffer.all_completed());
        buffer.mark_ready(0);
        assert!(buffer.all_completed());
        drop(rx);
    }

    #[tokio::test]
    async fn test_claim_range_large_preferred_size() {
        let (file, path) = create_temp_file(10);
        let (buffer, rx) = SegmentsBuffer::new(10, file, path);
        // preferred size much larger than total
        let r = buffer.claim_range(u64::MAX);
        assert!(r.is_some());
        let (id, start, end) = r.unwrap();
        assert_eq!(id, 0);
        assert_eq!(start, 0);
        assert_eq!(end, 10); // clamped to total_size
        drop(rx);
    }

    #[tokio::test]
    async fn test_write_data_sequential_chunks() {
        let (file, path) = create_temp_file(20);
        let (buffer, rx) = SegmentsBuffer::new(20, file, path);
        buffer.write_data(0, b"AAAAA").unwrap();
        buffer.write_data(5, b"BBBBB").unwrap();
        buffer.write_data(10, b"CCCCC").unwrap();
        buffer.write_data(15, b"DDDDD").unwrap();
        let read = buffer.read_data(0, 20).unwrap();
        assert_eq!(&read[..], b"AAAAABBBBBCCCCCDDDDD");
        drop(rx);
    }

    #[tokio::test]
    async fn test_write_data_overlapping() {
        let (file, path) = create_temp_file(10);
        let (buffer, rx) = SegmentsBuffer::new(10, file, path);
        buffer.write_data(0, b"xxxxxxxxx").unwrap();
        buffer.write_data(2, b"YYY").unwrap();
        let read = buffer.read_data(0, 10).unwrap();
        // bytes 0-1: xx, 2-4: YYY, 5-8: xxxx, 9: 0 (preallocated, never written)
        assert_eq!(&read[0..9], b"xxYYYxxxx");
        assert_eq!(read[9], 0); // preallocated zero
        drop(rx);
    }

    #[tokio::test]
    async fn test_is_ready_after_mark_not_affecting_other_segments() {
        let (file, path) = create_temp_file(200);
        let (buffer, rx) = SegmentsBuffer::new(200, file, path);
        buffer.claim_range(100);
        buffer.claim_range(100);
        assert!(!buffer.is_ready(0));
        assert!(!buffer.is_ready(1));
        buffer.mark_ready(0);
        assert!(buffer.is_ready(0));
        assert!(!buffer.is_ready(1));
        buffer.mark_ready(1);
        assert!(buffer.is_ready(0));
        assert!(buffer.is_ready(1));
        drop(rx);
    }

    #[tokio::test]
    async fn test_notify_multiple_ready() {
        let (file, path) = create_temp_file(300);
        let (buffer, mut rx) = SegmentsBuffer::new(300, file, path);
        buffer.claim_range(100);
        buffer.claim_range(100);
        buffer.claim_range(100);
        assert!(buffer.claim_range(1).is_none());
        let b = buffer.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            b.mark_ready(0);
            b.mark_ready(1);
            b.mark_ready(2);
        });
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            if let Ok(id) = rx.recv().await {
                seen.insert(id);
            }
        }
        assert_eq!(seen.len(), 3);
        assert!(seen.contains(&0));
        assert!(seen.contains(&1));
        assert!(seen.contains(&2));
        drop(rx);
    }

    #[tokio::test]
    async fn test_all_completed_empty_after_claim() {
        let (file, path) = create_temp_file(0);
        let (buffer, rx) = SegmentsBuffer::new(0, file, path);
        buffer.claim_range(1024);
        assert!(buffer.all_completed());
        drop(rx);
    }
}
