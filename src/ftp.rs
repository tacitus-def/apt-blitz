use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::buffer::SegmentsBuffer;

#[derive(Debug, Clone)]
pub struct FtpUrl {
    pub scheme: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub host: String,
    pub port: u16,
    pub path: String,
}

pub fn parse_ftp_url(url: &str) -> anyhow::Result<FtpUrl> {
    let (scheme, rest) = if let Some(s) = url.strip_prefix("ftps://") {
        ("ftps".to_string(), s)
    } else if let Some(s) = url.strip_prefix("ftp://") {
        ("ftp".to_string(), s)
    } else {
        anyhow::bail!("not an FTP URL: {url}");
    };

    // Last `@` before the first `/` separates userinfo from hostportpath
    let first_slash = rest.find('/');
    let at_before_slash = rest[..first_slash.unwrap_or(rest.len())].rfind('@');
    let (userinfo, hostportpath) = if let Some(idx) = at_before_slash {
        (&rest[..idx], &rest[idx + 1..])
    } else {
        ("", rest)
    };

    let (username, password) = if userinfo.is_empty() {
        (None, None)
    } else if let Some((u, p)) = userinfo.split_once(':') {
        (Some(u.to_string()), Some(p.to_string()))
    } else {
        (Some(userinfo.to_string()), None)
    };

    let (hostport, path) = if let Some(idx) = hostportpath.find('/') {
        (&hostportpath[..idx], &hostportpath[idx..])
    } else {
        (hostportpath, "/")
    };

    let (host, port) = if let Some((h, p)) = hostport.split_once(':') {
        let port: u16 = p.parse()?;
        (h.to_string(), port)
    } else {
        (hostport.to_string(), if scheme == "ftps" { 990 } else { 21 })
    };

    Ok(FtpUrl { scheme, username, password, host, port, path: path.to_string() })
}

// ---- minimal FTP control connection ----

#[derive(Debug)]
struct FtpControl {
    reader: BufReader<TcpStream>,
}

impl FtpControl {
    async fn connect(host: &str, port: u16) -> anyhow::Result<Self> {
        let stream = TcpStream::connect((host, port)).await
            .map_err(|e| anyhow::anyhow!("FTP connect to {host}:{port} failed: {e}"))?;
        let mut ctrl = FtpControl { reader: BufReader::new(stream) };
        ctrl.read_response(None).await?; // welcome banner
        Ok(ctrl)
    }

    async fn cmd(&mut self, line: &str) -> anyhow::Result<String> {
        info!("FTP >>> {line}");
        self.reader.get_mut().write_all(line.as_bytes()).await
            .map_err(|e| anyhow::anyhow!("FTP write failed: {e}"))?;
        self.reader.get_mut().write_all(b"\r\n").await?;
        self.read_response(None).await
    }

    async fn read_response(&mut self, expected_prefix: Option<&str>) -> anyhow::Result<String> {
        let mut lines = Vec::new();
        loop {
            let mut line = String::new();
            self.reader.read_line(&mut line).await
                .map_err(|e| anyhow::anyhow!("FTP read failed: {e}"))?;
            let trimmed = line.trim_end_matches("\r\n").to_string();
            lines.push(trimmed.clone());

            // Multi-line response ends with "nnn <text>" (space after code)
            // Continuation lines have "nnn-<text>" (dash after code)
            if trimmed.len() >= 4 {
                let sep = trimmed[3..4].chars().next().unwrap_or(' ');
                if sep == ' ' {
                    // Last line
                    if let Some(prefix) = expected_prefix {
                        // Check the first line's code
                        let first_code = &lines[0][..3];
                        if first_code != prefix {
                            anyhow::bail!("FTP expected {prefix}xx, got: {}", lines.join(" | "));
                        }
                    }
                    return Ok(trimmed);
                }
            }
        }
    }

    async fn raw_stream(&mut self) -> &mut TcpStream {
        self.reader.get_mut()
    }
}

async fn connect_and_login(url: &FtpUrl) -> anyhow::Result<FtpControl> {
    if url.scheme == "ftps" {
        anyhow::bail!("FTPS is not yet supported; use plain ftp:// instead");
    }
    let mut ftp = FtpControl::connect(&url.host, url.port).await?;
    let user = url.username.as_deref().unwrap_or("anonymous");
    let pass = url.password.as_deref().unwrap_or("anonymous@");
    ftp.cmd(&format!("USER {user}")).await?;
    ftp.cmd(&format!("PASS {pass}")).await?;
    ftp.cmd("TYPE I").await?;
    Ok(ftp)
}

// ---- public API ----

#[derive(Debug)]
pub enum FtpDownloadError {
    Io(std::io::Error),
    Ftp(String),
    BufferFailed,
    Cancelled,
}

impl From<std::io::Error> for FtpDownloadError {
    fn from(e: std::io::Error) -> Self { FtpDownloadError::Io(e) }
}

pub async fn check_ftp_size(url: &FtpUrl) -> anyhow::Result<u64> {
    let mut ftp = connect_and_login(url).await?;
    let resp = ftp.cmd(&format!("SIZE {}", url.path)).await?;
    let size_str = resp.strip_prefix("213 ")
        .or_else(|| resp.strip_prefix("213 "))
        .ok_or_else(|| anyhow::anyhow!("unexpected SIZE response: {resp}"))?;
    let size: u64 = size_str.trim().parse()
        .map_err(|e| anyhow::anyhow!("invalid SIZE: {resp}: {e}"))?;
    ftp.cmd("QUIT").await.ok();
    Ok(size)
}

/// Parse PASV response: "227 Entering Passive Mode (h1,h2,h3,h4,p1,p2)"
fn parse_pasv(resp: &str) -> anyhow::Result<(String, u16)> {
    let body = resp.find('(')
        .and_then(|s| resp[s..].find(')').map(|e| &resp[s + 1..s + e]))
        .or_else(|| {
            // No parentheses: extract digits/comma after the status code
            let after_code = resp.strip_prefix("227 ").unwrap_or(resp);
            Some(after_code)
        })
        .unwrap_or(resp);
    let nums: Vec<u16> = body.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    if nums.len() < 6 {
        anyhow::bail!("cannot parse PASV response: {resp}");
    }
    let ip = format!("{}.{}.{}.{}", nums[0], nums[1], nums[2], nums[3]);
    let port = (nums[4] << 8) | nums[5];
    Ok((ip, port))
}

async fn download_range(
    ftp: &mut FtpControl,
    data_addr: (String, u16),
    start: u64,
    _path: &str,
    buffer: &SegmentsBuffer,
    offset: u64,
    size: u64,
) -> Result<(), FtpDownloadError> {
    if start > 0 {
        ftp.cmd(&format!("REST {start}")).await
            .map_err(|e| FtpDownloadError::Ftp(e.to_string()))?;
    }

    let stream = ftp.raw_stream().await;
    stream.write_all(format!("RETR {_path}\r\n").as_bytes()).await
        .map_err(|e| FtpDownloadError::Ftp(format!("RETR write failed: {e}")))?;

    // Open data connection to server
    let mut data = TcpStream::connect((data_addr.0.as_str(), data_addr.1)).await
        .map_err(|e| FtpDownloadError::Ftp(format!("data connect failed: {e}")))?;

    // Read response to RETR
    let retr_resp = ftp.read_response(Some("150")).await
        .map_err(|e| FtpDownloadError::Ftp(e.to_string()))?;
    info!("FTP <<< {retr_resp}");

    let mut buf = vec![0u8; 65536];
    let mut written = 0u64;
    while written < size {
        let to_read = std::cmp::min(buf.len() as u64, size - written) as usize;
        let n = data.read(&mut buf[..to_read]).await?;
        if n == 0 { break; }
        buffer.write_data(offset + written, &buf[..n])
            .map_err(|_| FtpDownloadError::BufferFailed)?;
        written += n as u64;
    }

    drop(data);

    // Read transfer complete response
    ftp.read_response(Some("226")).await.ok();
    Ok(())
}

pub async fn download_ftp_single(
    url: &FtpUrl,
    buffer: Arc<SegmentsBuffer>,
) -> Result<(), FtpDownloadError> {
    let mut ftp = connect_and_login(url).await
        .map_err(|e| FtpDownloadError::Ftp(e.to_string()))?;

    let (id, start, end) = match buffer.claim_range(u64::MAX) {
        Some(r) => r,
        None => return Ok(()),
    };

    let resp = ftp.cmd("PASV").await
        .map_err(|e| FtpDownloadError::Ftp(e.to_string()))?;
    let data_addr = parse_pasv(&resp)
        .map_err(|e| FtpDownloadError::Ftp(e.to_string()))?;

    download_range(&mut ftp, data_addr, start, &url.path, &buffer, start, end - start).await?;
    buffer.mark_ready(id);
    ftp.cmd("QUIT").await.ok();
    Ok(())
}

pub async fn download_ftp_multithreaded(
    url: &FtpUrl,
    buffer: Arc<SegmentsBuffer>,
    num_connections: usize,
) -> Result<(), FtpDownloadError> {
    let cancel = CancellationToken::new();
    let mut handles = Vec::with_capacity(num_connections);

    for i in 0..num_connections {
        let url = url.clone();
        let buffer = buffer.clone();
        let child_token = cancel.child_token();
        handles.push(tokio::spawn(async move {
            ftp_worker(&url, buffer, child_token, i).await
        }));
    }

    let mut errors = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(FtpDownloadError::Cancelled)) => {}
            Ok(Err(e)) => {
                error!(error = ?e, "ftp worker failed");
                errors.push(e);
                cancel.cancel();
            }
            Err(e) => {
                error!(error = ?e, "ftp worker panicked");
                errors.push(FtpDownloadError::BufferFailed);
                cancel.cancel();
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        buffer.set_failed();
        Err(FtpDownloadError::BufferFailed)
    }
}

async fn ftp_worker(
    url: &FtpUrl,
    buffer: Arc<SegmentsBuffer>,
    cancel: CancellationToken,
    worker_id: usize,
) -> Result<(), FtpDownloadError> {
    let mut ftp = connect_and_login(url).await
        .map_err(|e| FtpDownloadError::Ftp(e.to_string()))?;

    let mut preferred_size = 1024u64 * 1024;

    loop {
        if cancel.is_cancelled() {
            return Err(FtpDownloadError::Cancelled);
        }

        let range = buffer.claim_range(preferred_size);
        let (id, start, end) = match range {
            Some(r) => r,
            None => break,
        };
        let size = end - start;

        info!(worker = worker_id, segment = id, start, size, "ftp downloading segment");

        let start_time = Instant::now();

        let resp = ftp.cmd("PASV").await
            .map_err(|e| FtpDownloadError::Ftp(e.to_string()))?;
        let data_addr = parse_pasv(&resp)
            .map_err(|e| FtpDownloadError::Ftp(e.to_string()))?;

        download_range(&mut ftp, data_addr, start, &url.path, &buffer, start, size).await?;
        buffer.mark_ready(id);

        let elapsed = start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let speed = size as f64 / elapsed;
            let preferred = (speed * 2.0) as u64;
            preferred_size = preferred.clamp(64 * 1024, 4 * 1024 * 1024);
        }
    }

    ftp.cmd("QUIT").await.ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_ftp_url ---

    #[test]
    fn test_parse_ftp_url_anonymous() {
        let u = parse_ftp_url("ftp://ftp.example.com/pub/file.deb").unwrap();
        assert_eq!(u.scheme, "ftp");
        assert_eq!(u.host, "ftp.example.com");
        assert_eq!(u.port, 21);
        assert_eq!(u.path, "/pub/file.deb");
        assert!(u.username.is_none());
    }

    #[test]
    fn test_parse_ftp_url_with_auth() {
        let u = parse_ftp_url("ftp://user:pass@mirror.example.com:2121/archive.iso").unwrap();
        assert_eq!(u.username.as_deref(), Some("user"));
        assert_eq!(u.password.as_deref(), Some("pass"));
        assert_eq!(u.host, "mirror.example.com");
        assert_eq!(u.port, 2121);
        assert_eq!(u.path, "/archive.iso");
    }

    #[test]
    fn test_parse_ftp_url_user_only() {
        let u = parse_ftp_url("ftp://anon@host/path").unwrap();
        assert_eq!(u.username.as_deref(), Some("anon"));
        assert!(u.password.is_none());
    }

    #[test]
    fn test_parse_ftp_url_no_path() {
        let u = parse_ftp_url("ftp://server").unwrap();
        assert_eq!(u.path, "/");
        assert_eq!(u.port, 21);
    }

    #[test]
    fn test_parse_ftp_url_root_path() {
        let u = parse_ftp_url("ftp://server/").unwrap();
        assert_eq!(u.path, "/");
    }

    #[test]
    fn test_parse_ftp_url_deep_path() {
        let u = parse_ftp_url("ftp://a/b/c/d/e/f.iso").unwrap();
        assert_eq!(u.path, "/b/c/d/e/f.iso");
    }

    #[test]
    fn test_parse_ftp_url_ftps_default_port() {
        let u = parse_ftp_url("ftps://secure.example.com/file").unwrap();
        assert_eq!(u.scheme, "ftps");
        assert_eq!(u.port, 990);
    }

    #[test]
    fn test_parse_ftp_url_invalid_scheme() {
        assert!(parse_ftp_url("http://example.com/file").is_err());
        assert!(parse_ftp_url("unknown://x").is_err());
    }

    #[test]
    fn test_parse_ftp_url_empty() {
        assert!(parse_ftp_url("").is_err());
    }

    #[test]
    fn test_parse_ftp_url_garbage() {
        assert!(parse_ftp_url("not a url at all").is_err());
        assert!(parse_ftp_url("http://example.com").is_err());
    }

    #[test]
    fn test_parse_ftp_url_at_in_path() {
        let u = parse_ftp_url("ftp://host/path@withat/file").unwrap();
        assert!(u.username.is_none());
        assert_eq!(u.path, "/path@withat/file");
    }

    #[test]
    fn test_parse_ftp_url_auth_at_in_pass() {
        let u = parse_ftp_url("ftp://user:pa@ss@host/path").unwrap();
        assert_eq!(u.username.as_deref(), Some("user"));
        assert_eq!(u.password.as_deref(), Some("pa@ss"));
    }

    // --- parse_pasv ---

    #[test]
    fn test_parse_pasv_normal() {
        let (ip, port) = parse_pasv("227 Entering Passive Mode (192,168,1,1,10,0)").unwrap();
        assert_eq!(ip, "192.168.1.1");
        assert_eq!(port, 2560); // 10 << 8 | 0
    }

    #[test]
    fn test_parse_pasv_no_parens() {
        let (ip, port) = parse_pasv("227 10,0,0,1,30,39").unwrap();
        assert_eq!(ip, "10.0.0.1");
        assert_eq!(port, (30 << 8) | 39);
    }

    #[test]
    fn test_parse_pasv_max_port() {
        let (ip, port) = parse_pasv("227 (255,255,255,255,255,255)").unwrap();
        assert_eq!(ip, "255.255.255.255");
        assert_eq!(port, 65535);
    }

    #[test]
    fn test_parse_pasv_too_few_nums() {
        assert!(parse_pasv("227 (1,2,3,4,5)").is_err());
    }

    #[test]
    fn test_parse_pasv_empty() {
        assert!(parse_pasv("").is_err());
    }

    #[test]
    fn test_parse_pasv_garbage() {
        assert!(parse_pasv("227 no numbers here").is_err());
    }

    // --- FtpControl with mock TCP server ---

    async fn mock_ftp_server() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            reader.get_mut().write_all(b"220 Mock FTP ready\r\n").await.unwrap();

            let mut line = String::new();
            loop {
                line.clear();
                let n = reader.read_line(&mut line).await.unwrap();
                if n == 0 { break; }
                let cmd = line.trim().to_string();
                if cmd.starts_with("USER") {
                    reader.get_mut().write_all(b"230 Login successful\r\n").await.unwrap();
                } else if cmd.starts_with("PASS") {
                    reader.get_mut().write_all(b"230 Already logged in\r\n").await.unwrap();
                } else if cmd.starts_with("TYPE") {
                    reader.get_mut().write_all(b"200 Type set\r\n").await.unwrap();
                } else if cmd.starts_with("SIZE") {
                    reader.get_mut().write_all(b"213 123456\r\n").await.unwrap();
                } else if cmd.starts_with("QUIT") {
                    reader.get_mut().write_all(b"221 Bye\r\n").await.unwrap();
                    break;
                } else {
                    reader.get_mut().write_all(b"500 Unknown\r\n").await.unwrap();
                }
            }
        });
        port
    }

    #[tokio::test]
    async fn test_ftp_control_connect_and_welcome() {
        let port = mock_ftp_server().await;
        let mut ftp = FtpControl::connect("127.0.0.1", port).await.unwrap();
        let resp = ftp.cmd("NOOP").await.unwrap();
        assert_eq!(resp, "500 Unknown");
    }

    #[tokio::test]
    async fn test_ftp_control_cmd_roundtrip() {
        let port = mock_ftp_server().await;
        let mut ftp = FtpControl::connect("127.0.0.1", port).await.unwrap();
        let resp = ftp.cmd("USER test").await.unwrap();
        assert_eq!(resp, "230 Login successful");
    }

    #[tokio::test]
    async fn test_ftp_control_connect_refused() {
        let result = FtpControl::connect("127.0.0.1", 1).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Connection refused") || err.contains("failed") || err.contains("reset"));
    }

    #[tokio::test]
    async fn test_check_ftp_size_with_mock() {
        let port = mock_ftp_server().await;
        let url = FtpUrl {
            scheme: "ftp".into(),
            username: None,
            password: None,
            host: "127.0.0.1".into(),
            port,
            path: "/bigfile.bin".into(),
        };
        let size = check_ftp_size(&url).await.unwrap();
        assert_eq!(size, 123456);
    }

    #[tokio::test]
    async fn test_check_ftp_size_refused() {
        let url = FtpUrl {
            scheme: "ftp".into(),
            username: None,
            password: None,
            host: "127.0.0.1".into(),
            port: 1,
            path: "/x".into(),
        };
        let result = check_ftp_size(&url).await;
        assert!(result.is_err());
    }

    // --- FTP mock with PASV + DATA ---

    async fn mock_ftp_with_data(file_size: u64) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
            let data = Arc::new(data);

            loop {
                let (stream, _) = match tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    listener.accept(),
                )
                .await
                {
                    Ok(Ok(s)) => s,
                    _ => break,
                };
                let data = Arc::clone(&data);
                tokio::spawn(async move {
                    handle_ftp_control(stream, data).await;
                });
            }
        });
        port
    }

    async fn handle_ftp_control(stream: tokio::net::TcpStream, data: Arc<Vec<u8>>) {
        let mut reader = tokio::io::BufReader::new(stream);
        reader.get_mut().write_all(b"220 Mock FTP ready\r\n").await.unwrap();

        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line).await.unwrap();
            if n == 0 {
                break;
            }
            let cmd = line.trim().to_string();

            if cmd.starts_with("USER") {
                reader.get_mut().write_all(b"230 Login successful\r\n").await.unwrap();
            } else if cmd.starts_with("PASS") {
                reader.get_mut().write_all(b"230 Already logged in\r\n").await.unwrap();
            } else if cmd.starts_with("TYPE") {
                reader.get_mut().write_all(b"200 Type set\r\n").await.unwrap();
            } else if cmd.starts_with("SIZE") {
                let resp = format!("213 {}\r\n", data.len());
                reader.get_mut().write_all(resp.as_bytes()).await.unwrap();
            } else if cmd.starts_with("PASV") {
                let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let data_port = data_listener.local_addr().unwrap().port();
                let p1 = data_port >> 8;
                let p2 = data_port & 0xFF;
                let resp = format!("227 Entering Passive Mode (127,0,0,1,{p1},{p2})\r\n");
                reader.get_mut().write_all(resp.as_bytes()).await.unwrap();

                // Next command: REST or RETR
                line.clear();
                let n = reader.read_line(&mut line).await.unwrap();
                if n == 0 {
                    break;
                }
                let next_cmd = line.trim().to_string();

                let start_pos: u64 = if next_cmd.starts_with("REST ") {
                    let rest_val: u64 = next_cmd[5..].trim().parse().unwrap_or(0);
                    reader.get_mut().write_all(b"350 Ready for resume\r\n").await.unwrap();
                    // Read RETR
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    rest_val
                } else {
                    assert!(next_cmd.starts_with("RETR "), "expected RETR, got: {next_cmd}");
                    0u64
                };

                // Accept data connection
                let (mut data_stream, _) = match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    data_listener.accept(),
                )
                .await
                {
                    Ok(Ok(s)) => s,
                    _ => {
                        reader.get_mut().write_all(b"425 Can't open data connection\r\n").await.unwrap();
                        continue;
                    }
                };

                let start = start_pos as usize;
                let to_send = &data[start..];
                reader.get_mut().write_all(b"150 Opening data connection\r\n").await.unwrap();
                data_stream.write_all(to_send).await.unwrap();
                drop(data_stream);

                reader.get_mut().write_all(b"226 Transfer complete\r\n").await.unwrap();
            } else if cmd.starts_with("QUIT") {
                reader.get_mut().write_all(b"221 Bye\r\n").await.unwrap();
                break;
            } else {
                reader.get_mut().write_all(b"500 Unknown\r\n").await.unwrap();
            }
        }
    }

    async fn create_temp_buffer(file_size: u64) -> Arc<SegmentsBuffer> {
        let dir = std::env::temp_dir().join("apt-blitz-test-ftp-buffer");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.download", uuid::Uuid::new_v4()));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.set_len(file_size).unwrap();
        let (buffer, _) = SegmentsBuffer::new(file_size, file, path);
        buffer
    }

    #[tokio::test]
    async fn test_download_ftp_single_with_mock() {
        let file_size = 100;
        let port = mock_ftp_with_data(file_size).await;
        let url = FtpUrl {
            scheme: "ftp".into(),
            username: None,
            password: None,
            host: "127.0.0.1".into(),
            port,
            path: "/file.dat".into(),
        };
        let buffer = create_temp_buffer(file_size).await;
        // download_ftp_single calls claim_range internally
        download_ftp_single(&url, buffer.clone()).await.unwrap();
        assert!(buffer.is_ready(0));
        let read = buffer.read_data(0, file_size).unwrap();
        let expected: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
        assert_eq!(read.to_vec(), expected);
        let _ = std::fs::remove_file(buffer.file_path());
    }

    #[tokio::test]
    async fn test_download_ftp_multithreaded_with_mock() {
        let file_size = 500;
        let port = mock_ftp_with_data(file_size).await;
        let url = FtpUrl {
            scheme: "ftp".into(),
            username: None,
            password: None,
            host: "127.0.0.1".into(),
            port,
            path: "/bigfile.dat".into(),
        };
        let buffer = create_temp_buffer(file_size).await;
        download_ftp_multithreaded(&url, buffer.clone(), 4).await.unwrap();
        assert!(buffer.all_completed());
        let read = buffer.read_data(0, file_size).unwrap();
        let expected: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
        assert_eq!(read.to_vec(), expected);
        let _ = std::fs::remove_file(buffer.file_path());
    }

    #[tokio::test]
    async fn test_download_ftp_single_with_auth() {
        let file_size = 50;
        let port = mock_ftp_with_data(file_size).await;
        let url = FtpUrl {
            scheme: "ftp".into(),
            username: Some("testuser".into()),
            password: Some("secret".into()),
            host: "127.0.0.1".into(),
            port,
            path: "/auth.dat".into(),
        };
        let buffer = create_temp_buffer(file_size).await;
        download_ftp_single(&url, buffer.clone()).await.unwrap();
        assert!(buffer.is_ready(0));
        let read = buffer.read_data(0, file_size).unwrap();
        assert_eq!(read.len() as u64, file_size);
        let _ = std::fs::remove_file(buffer.file_path());
    }

    #[tokio::test]
    async fn test_ftp_pasv_data_connect_refused() {
        let file_size = 100;
        // Start a mock that returns PASV pointing to a closed port
        let ctrl_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = ctrl_listener.local_addr().unwrap().port();
        // A port that's definitely closed
        let dead_port = 1;
        let p1 = dead_port >> 8;
        let p2 = dead_port & 0xFF;

        tokio::spawn(async move {
            let (stream, _) = ctrl_listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            reader.get_mut().write_all(b"220 Mock\r\n").await.unwrap();
            let mut line = String::new();
            loop {
                line.clear();
                let n = reader.read_line(&mut line).await.unwrap();
                if n == 0 { break; }
                let cmd = line.trim();
                if cmd.starts_with("USER") {
                    reader.get_mut().write_all(b"230 OK\r\n").await.unwrap();
                } else if cmd.starts_with("PASS") {
                    reader.get_mut().write_all(b"230 OK\r\n").await.unwrap();
                } else if cmd.starts_with("TYPE") {
                    reader.get_mut().write_all(b"200 OK\r\n").await.unwrap();
                } else if cmd.starts_with("PASV") {
                    let resp = format!("227 Entering Passive Mode (127,0,0,1,{p1},{p2})\r\n");
                    reader.get_mut().write_all(resp.as_bytes()).await.unwrap();
                    // Client sends RETR then fails to connect data → error
                    // Read and discard RETR
                    line.clear();
                    let _ = reader.read_line(&mut line).await;
                    break;
                }
            }
        });

        let url = FtpUrl {
            scheme: "ftp".into(),
            username: None,
            password: None,
            host: "127.0.0.1".into(),
            port,
            path: "/file.dat".into(),
        };
        let buffer = create_temp_buffer(file_size).await;
        let result = download_ftp_single(&url, buffer.clone()).await;
        assert!(result.is_err());
        let _ = std::fs::remove_file(buffer.file_path());
    }

    #[tokio::test]
    async fn test_check_ftp_size_ftps_rejected() {
        let url = FtpUrl {
            scheme: "ftps".into(),
            username: None,
            password: None,
            host: "127.0.0.1".into(),
            port: 990,
            path: "/x".into(),
        };
        let result = check_ftp_size(&url).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("FTPS"));
    }

    // --- FtpDownloadError ---

    #[test]
    fn test_ftp_download_error_debug() {
        let e = FtpDownloadError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        let s = format!("{e:?}");
        assert!(s.contains("Io") || s.contains("test"));
    }

    #[test]
    fn test_ftp_download_error_from_io() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let e: FtpDownloadError = io.into();
        assert!(matches!(e, FtpDownloadError::Io(_)));
    }

    #[test]
    fn test_ftp_download_error_variants() {
        assert!(matches!(FtpDownloadError::Ftp("err".into()), FtpDownloadError::Ftp(_)));
        assert!(matches!(FtpDownloadError::BufferFailed, FtpDownloadError::BufferFailed));
        assert!(matches!(FtpDownloadError::Cancelled, FtpDownloadError::Cancelled));
    }
}
