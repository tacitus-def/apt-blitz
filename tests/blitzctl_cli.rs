//! End-to-end check of the `blitzctl` binary against a populated cache.

use std::process::Command;

use apt_blitz::cache::Cache;
use http::HeaderMap;

const BIN: &str = env!("CARGO_BIN_EXE_blitzctl");

async fn populate(dir: &std::path::Path) {
    let cache = Cache::new(dir.to_path_buf(), 10_000_000).unwrap();
    let tmp = dir.join("tmp");
    std::fs::create_dir_all(&tmp).unwrap();

    let entries = [
        (
            "http://deb.debian.org/pool/main/a/apt_1.0_all.deb",
            "application/octet-stream",
            2048u64,
        ),
        (
            "http://deb.debian.org/pool/main/b/b.deb",
            "application/octet-stream",
            1024u64,
        ),
        (
            "http://security.debian.org/pool/x.deb",
            "application/gzip",
            4096u64,
        ),
    ];
    for (i, (url, ct, size)) in entries.iter().enumerate() {
        let p = tmp.join(format!("e{}.download", i));
        std::fs::write(&p, vec![0u8; *size as usize]).unwrap();
        let mut h = HeaderMap::new();
        h.insert("content-type", ct.parse().unwrap());
        cache.store(url, &p, &h).await.unwrap();
    }
    drop(cache);
}

fn run(dir: &std::path::Path, args: &[&str]) -> String {
    let out = Command::new(BIN)
        .args(["--cache-dir", dir.to_str().unwrap()])
        .args(args)
        .output()
        .expect("failed to run blitzctl");
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[tokio::test]
async fn blitzctl_cache_workflow() {
    let dir = std::env::temp_dir().join("apt-blitz-test-blitzctl-cli");
    let _ = std::fs::remove_dir_all(&dir);
    populate(&dir).await;

    // hosts
    let out = run(&dir, &["cache", "hosts"]);
    assert!(out.contains("deb.debian.org"), "hosts: {}", out);
    assert!(out.contains("security.debian.org"), "hosts: {}", out);

    // tree (level 2) — files are hidden by default, shown with --files
    let out = run(&dir, &["cache", "tree", "deb.debian.org"]);
    assert!(out.contains("pool/"), "tree: {}", out);
    assert!(
        !out.contains("apt_1.0_all.deb"),
        "tree should hide files by default: {}",
        out
    );

    let out = run(&dir, &["cache", "tree", "deb.debian.org", "--files"]);
    assert!(out.contains("apt_1.0_all.deb"), "tree --files: {}", out);

    // info (exact file)
    let out = run(
        &dir,
        &[
            "cache",
            "info",
            "http://deb.debian.org/pool/main/a/apt_1.0_all.deb",
        ],
    );
    assert!(
        out.contains("Host:        deb.debian.org"),
        "info: {}",
        out
    );
    assert!(
        out.contains("Content-Type:application/octet-stream"),
        "info: {}",
        out
    );

    // selective clear by host
    let out = run(&dir, &["cache", "clear", "deb.debian.org", "--yes"]);
    assert!(out.contains("removed 2"), "clear host: {}", out);

    // security host remains
    let out = run(&dir, &["cache", "hosts"]);
    assert!(
        !out.contains("deb.debian.org"),
        "deb host should be gone: {}",
        out
    );
    assert!(
        out.contains("security.debian.org"),
        "security should remain: {}",
        out
    );

    // full clear
    let out = run(&dir, &["cache", "clear", "--yes"]);
    assert!(out.contains("cleared all"), "full clear: {}", out);

    // empty afterwards
    let out = run(&dir, &["cache", "hosts"]);
    assert!(
        out.contains("no cached entries"),
        "cache should be empty: {}",
        out
    );

    std::fs::remove_dir_all(&dir).ok();
}
