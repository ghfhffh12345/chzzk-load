mod common;

use chzzk_load::consolidation::loopback::{
    DEFAULT_LOOPBACK_STARTUP_TIMEOUT, EphemeralLoopbackServer, parse_loopback_port,
};
use common::mock_rclone::get_mock_rclone_bin;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn create_temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{}_{}", prefix, rand::random::<u32>()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn test_parse_loopback_port_formats() {
    // 1. Real rclone notice output format
    let real_rclone_line = "2026/10/09 15:56:08 NOTICE: Local file system at //?/C:/Users/test: HTTP Server started on [http://127.0.0.1:28522/]";
    assert_eq!(parse_loopback_port(real_rclone_line), Some(28522));

    // 2. Direct notice format from mock
    let mock_line = "NOTICE: HTTP Server started on [http://127.0.0.1:8080/]";
    assert_eq!(parse_loopback_port(mock_line), Some(8080));

    // 3. Port without trailing slash
    let no_slash_line = "HTTP Server started on [http://127.0.0.1:9090]";
    assert_eq!(parse_loopback_port(no_slash_line), Some(9090));

    // 4. Plain host:port format
    let plain_line = "Listening on 127.0.0.1:12345 for incoming requests";
    assert_eq!(parse_loopback_port(plain_line), Some(12345));

    // 5. Port 0 is ignored (unbound bind address)
    let bind_zero = "rclone serve http remote:path --addr 127.0.0.1:0 --read-only";
    assert_eq!(parse_loopback_port(bind_zero), None);

    // 6. Multiple IPs where first is port 0 and second is dynamic assigned port
    let multi_line = "Binding 127.0.0.1:0 -> successfully assigned 127.0.0.1:44332";
    assert_eq!(parse_loopback_port(multi_line), Some(44332));

    // 7. Non-IP or invalid lines return None
    assert_eq!(parse_loopback_port("random stderr line without ip"), None);
    assert_eq!(parse_loopback_port("127.0.0.1:not_a_port"), None);
    assert_eq!(parse_loopback_port("127.0.0.1:99999999999"), None);
    assert_eq!(parse_loopback_port(""), None);
}

#[test]
fn test_loopback_server_chunk_url_formatting() {
    let base_a = "http://127.0.0.1:12345/";
    let base_b = "http://127.0.0.1:12345";

    // Test URL construction logic directly
    assert_eq!(
        format!(
            "{}/{}",
            base_a.trim_end_matches('/'),
            "chunk_0000.ts".trim_start_matches('/')
        ),
        "http://127.0.0.1:12345/chunk_0000.ts"
    );
    assert_eq!(
        format!(
            "{}/{}",
            base_b.trim_end_matches('/'),
            "/chunk_0000.ts".trim_start_matches('/')
        ),
        "http://127.0.0.1:12345/chunk_0000.ts"
    );
}

#[tokio::test]
async fn test_loopback_server_with_mock_rclone_startup_and_liveness() {
    let mock_bin = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_loopback_liveness");
    let chunk_path = temp_dir.join("chunk_0000.ts");
    tokio::fs::write(&chunk_path, b"test-mpegts-stream-payload")
        .await
        .unwrap();

    let remote_target = format!(
        "mockremote:{}",
        temp_dir.to_string_lossy().replace('\\', "/")
    );

    let server = EphemeralLoopbackServer::start_with_timeout(
        &remote_target,
        Some(mock_bin.to_str().unwrap()),
        DEFAULT_LOOPBACK_STARTUP_TIMEOUT,
    )
    .await
    .expect("EphemeralLoopbackServer should start and pass liveness check");

    let port = server.port();
    assert!(port > 0, "Assigned loopback port must be non-zero");
    assert!(
        server.base_url().contains(&port.to_string()),
        "base_url must contain assigned port"
    );
    assert_eq!(
        server.chunk_url("chunk_0000.ts"),
        format!("http://127.0.0.1:{port}/chunk_0000.ts")
    );

    // Verify chunk retrieval over HTTP loopback
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("Must connect to loopback server port");

    let req = format!(
        "GET /chunk_0000.ts HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response_str = String::from_utf8_lossy(&response);

    assert!(
        response_str.starts_with("HTTP/1.1 200 OK"),
        "Server must return 200 OK"
    );
    assert!(
        response_str.contains("test-mpegts-stream-payload"),
        "Response body must match chunk payload"
    );

    drop(server);
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_loopback_server_raii_drop_terminates_child() {
    let mock_bin = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_loopback_drop");
    let remote_target = format!(
        "mockremote:{}",
        temp_dir.to_string_lossy().replace('\\', "/")
    );

    let server = EphemeralLoopbackServer::start(&remote_target, Some(mock_bin.to_str().unwrap()))
        .await
        .expect("Server should start");

    let port = server.port();
    assert!(port > 0);

    // Verify port is open
    let conn = TcpStream::connect(("127.0.0.1", port)).await;
    assert!(conn.is_ok(), "Port must be open while server is active");

    // Drop the server - RAII Drop must kill the child
    drop(server);

    // Allow process kill to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify port is closed and rejects new connections
    let dead_conn = TcpStream::connect(("127.0.0.1", port)).await;
    assert!(
        dead_conn.is_err(),
        "Port must reject connections after server is dropped"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_loopback_server_premature_exit_reports_error() {
    let mock_bin = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_loopback_fail");
    // "fail_serve" triggers simulated startup exit 1 in mock_rclone
    let remote_target = format!(
        "mockremote:{}/fail_serve",
        temp_dir.to_string_lossy().replace('\\', "/")
    );

    let err = EphemeralLoopbackServer::start(&remote_target, Some(mock_bin.to_str().unwrap()))
        .await
        .expect_err("Server with fail_serve must return error");

    let err_msg = err.to_string();
    assert!(
        err_msg.contains("rclone serve http exited prematurely")
            || err_msg.contains("failed to serve http"),
        "Error message should reflect premature exit: {err_msg}"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_loopback_server_timeout_kills_child() {
    let mock_bin = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_loopback_hang");
    // "hang_serve" triggers sleep without output in mock_rclone
    let remote_target = format!(
        "mockremote:{}/hang_serve",
        temp_dir.to_string_lossy().replace('\\', "/")
    );

    let start = std::time::Instant::now();
    let err = EphemeralLoopbackServer::start_with_timeout(
        &remote_target,
        Some(mock_bin.to_str().unwrap()),
        Duration::from_millis(150),
    )
    .await
    .expect_err("Hanging server must time out");

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "Timeout must enforce deadline quickly (took {:?})",
        elapsed
    );
    assert!(
        err.to_string().contains("Timed out"),
        "Error must mention timeout: {err}"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
