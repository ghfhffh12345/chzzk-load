use chzzk_load::chzzk::chat::{ChzzkChatClient, compute_server_id, parse_chat_packet};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

#[test]
fn test_compute_server_id() {
    // Check distribution 1..=9
    let id1 = compute_server_id("N12345");
    assert!((1..=9).contains(&id1));
    let id2 = compute_server_id("abcdef");
    assert!((1..=9).contains(&id2));
}

#[tokio::test]
async fn test_mock_websocket_handshake_and_chat_receiving() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{addr}");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // 1. Expect CONNECT packet (cmd: 100)
        let msg = ws.next().await.unwrap().unwrap();
        let text = msg.to_text().unwrap();
        assert!(text.contains(r#""cmd":100"#) || text.contains(r#""cmd": 100"#));

        // 2. Respond with CONNECTED (cmd: 10100)
        let resp = serde_json::json!({
            "cmd": 10100,
            "bdy": { "sid": "session_test_xyz" }
        });
        ws.send(Message::Text(resp.to_string().into()))
            .await
            .unwrap();

        // 3. Send a CHAT packet (cmd: 93101)
        let chat_packet = serde_json::json!({
            "cmd": 93101,
            "bdy": [
                {
                    "msg": "Hello integration test!",
                    "msgTime": 1727268158000u64,
                    "msgTypeCode": 1,
                    "profile": "{\"nickname\":\"TestViewer\",\"userIdHash\":\"hash123\"}",
                    "extras": "{}"
                }
            ]
        });
        ws.send(Message::Text(chat_packet.to_string().into()))
            .await
            .unwrap();

        // 4. Send PING (cmd: 0)
        let ping_packet = serde_json::json!({ "cmd": 0, "ver": "2" });
        ws.send(Message::Text(ping_packet.to_string().into()))
            .await
            .unwrap();

        // 5. Expect PONG (cmd: 10000)
        let pong = ws.next().await.unwrap().unwrap();
        assert!(pong.to_text().unwrap().contains("10000"));
    });

    let cancel_token = CancellationToken::new();
    let temp_dir = std::env::temp_dir().join(format!("test_ws_chat_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    let chat_file = temp_dir.join("chat_0000.jsonl");

    let client = ChzzkChatClient::new(
        "mock_channel".to_string(),
        "mock_access_token".to_string(),
        temp_dir.clone(),
        Duration::from_secs(3600),
        Duration::from_millis(50),
        cancel_token.clone(),
    )
    .with_custom_ws_url(ws_url);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        cancel_clone.cancel();
    });

    let total = client.run(None, None).await.unwrap();
    assert_eq!(total, 1);

    server_task.await.unwrap();

    let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
    assert!(content.contains("Hello integration test!"));
    assert!(content.contains("TestViewer"));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[test]
fn test_parse_chat_packet_types_and_donations() {
    let packet = serde_json::json!({
        "cmd": 93102,
        "bdy": [
            {
                "msg": "Thanks for the stream!",
                "msgTime": 1727268158000u64,
                "msgTypeCode": 10,
                "payAmount": 10000u64,
                "profile": "{\"nickname\":\"GenerousDonor\",\"userIdHash\":\"donor_hash\"}",
                "extras": "{\"payAmount\":10000,\"donationType\":\"CHAT\"}"
            }
        ]
    });

    let msgs = parse_chat_packet(&packet);
    assert_eq!(msgs.len(), 1);
    let msg = &msgs[0];
    assert_eq!(msg.content, "Thanks for the stream!");
    assert_eq!(msg.nickname, "GenerousDonor");
    assert_eq!(msg.user_id_hash.as_deref(), Some("donor_hash"));
    assert_eq!(msg.msg_type, "DONATION");
    assert_eq!(msg.donation_amount, Some(10000));
}

#[tokio::test]
async fn test_handshake_failure_reconnects_with_backoff() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{addr}");

    let connection_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let conn_count_clone = connection_count.clone();

    let server_task = tokio::spawn(async move {
        // Connection 1: Accept, receive CONNECT, but close immediately without CONNECTED
        if let Ok((stream, _)) = listener.accept().await {
            conn_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                let _ = ws.next().await; // Read CONNECT
                let _ = ws.close(None).await;
            }
        }

        // Connection 2: Accept, handshake properly, send a chat message
        if let Ok((stream, _)) = listener.accept().await {
            conn_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                let _ = ws.next().await; // Read CONNECT
                let resp = serde_json::json!({
                    "cmd": 10100,
                    "bdy": { "sid": "session_reconnect_xyz" }
                });
                let _ = ws.send(Message::Text(resp.to_string().into())).await;

                let chat_packet = serde_json::json!({
                    "cmd": 93101,
                    "bdy": [
                        {
                            "msg": "Message after reconnect!",
                            "msgTime": 1727268158000u64,
                            "msgTypeCode": 1,
                            "profile": "{\"nickname\":\"ReconnectViewer\"}",
                            "extras": "{}"
                        }
                    ]
                });
                let _ = ws.send(Message::Text(chat_packet.to_string().into())).await;
            }
        }
    });

    let cancel_token = CancellationToken::new();
    let temp_dir =
        std::env::temp_dir().join(format!("test_ws_reconnect_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    let chat_file = temp_dir.join("chat_0000.jsonl");

    let client = ChzzkChatClient::new(
        "mock_channel".to_string(),
        "mock_access_token".to_string(),
        temp_dir.clone(),
        Duration::from_secs(3600),
        Duration::from_millis(50),
        cancel_token.clone(),
    )
    .with_custom_ws_url(ws_url);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        // Allow enough time for connection 1 failure, backoff sleep (1s), and connection 2 success
        tokio::time::sleep(Duration::from_millis(1600)).await;
        cancel_clone.cancel();
    });

    let total = client.run(None, None).await.unwrap();
    assert_eq!(total, 1);
    assert_eq!(
        connection_count.load(std::sync::atomic::Ordering::SeqCst),
        2
    );

    let _ = server_task.await;

    let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
    assert!(content.contains("Message after reconnect!"));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_chat_telemetry_non_blocking_when_receiver_full() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{addr}");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // 1. Handshake
        let _ = ws.next().await;
        let resp = serde_json::json!({
            "cmd": 10100,
            "bdy": { "sid": "session_stats_test" }
        });
        ws.send(Message::Text(resp.to_string().into()))
            .await
            .unwrap();

        // 2. Send multiple chat packets without delay
        for i in 0..10 {
            let chat_packet = serde_json::json!({
                "cmd": 93101,
                "bdy": [
                    {
                        "msg": format!("Spam msg {}", i),
                        "msgTime": 1727268158000u64 + i as u64,
                        "msgTypeCode": 1,
                        "profile": "{\"nickname\":\"StatsTester\"}",
                        "extras": "{}"
                    }
                ]
            });
            ws.send(Message::Text(chat_packet.to_string().into()))
                .await
                .unwrap();
        }
    });

    let cancel_token = CancellationToken::new();
    let temp_dir = std::env::temp_dir().join(format!("test_ws_stats_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    let client = ChzzkChatClient::new(
        "mock_channel".to_string(),
        "mock_access_token".to_string(),
        temp_dir.clone(),
        Duration::from_secs(3600),
        Duration::from_millis(50),
        cancel_token.clone(),
    )
    .with_custom_ws_url(ws_url);

    // Channel with buffer 1: deliberately full and never read from!
    let (stats_tx, _stats_rx) = tokio::sync::mpsc::channel::<u64>(1);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel_clone.cancel();
    });

    // Run must NOT block or deadlock even though stats_tx is saturated
    let total = client.run(Some(stats_tx), None).await.unwrap();
    assert_eq!(total, 10);

    let _ = server_task.await;
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_chat_client_emits_sealed_chunks() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{addr}");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // 1. Read CONNECT
        let _ = ws.next().await.unwrap().unwrap();

        // 2. Respond CONNECTED
        let resp = serde_json::json!({
            "cmd": 10100,
            "bdy": { "sid": "session_test_xyz" }
        });
        ws.send(Message::Text(resp.to_string().into()))
            .await
            .unwrap();

        // 3. Send message for chunk 0
        let chat_packet = serde_json::json!({
            "cmd": 93101,
            "bdy": [
                {
                    "msg": "Chunk 0 message",
                    "msgTime": 1727268158000u64,
                    "msgTypeCode": 1,
                    "profile": "{\"nickname\":\"Viewer0\"}",
                    "extras": "{}"
                }
            ]
        });
        ws.send(Message::Text(chat_packet.to_string().into()))
            .await
            .unwrap();

        // Wait a bit, then send message for chunk 1
        tokio::time::sleep(Duration::from_millis(150)).await;
        let chat_packet2 = serde_json::json!({
            "cmd": 93101,
            "bdy": [
                {
                    "msg": "Chunk 1 message",
                    "msgTime": 1727268159000u64,
                    "msgTypeCode": 1,
                    "profile": "{\"nickname\":\"Viewer1\"}",
                    "extras": "{}"
                }
            ]
        });
        ws.send(Message::Text(chat_packet2.to_string().into()))
            .await
            .unwrap();

        // Drain until close
        while let Some(Ok(_)) = ws.next().await {}
    });

    let cancel_token = CancellationToken::new();
    let temp_dir = std::env::temp_dir().join(format!("test_ws_sealed_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    // 100ms chunk duration
    let client = ChzzkChatClient::new(
        "mock_channel".to_string(),
        "mock_access_token".to_string(),
        temp_dir.clone(),
        Duration::from_millis(100),
        Duration::from_millis(20),
        cancel_token.clone(),
    )
    .with_custom_ws_url(ws_url);

    let (sealed_tx, mut sealed_rx) = tokio::sync::mpsc::channel::<std::path::PathBuf>(10);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        cancel_clone.cancel();
    });

    let total = client.run(None, Some(sealed_tx)).await.unwrap();
    assert_eq!(total, 2);

    let chunk0 = sealed_rx.recv().await.expect("Expected chunk 0 sealed");
    assert_eq!(chunk0, temp_dir.join("chat_0000.jsonl"));
    assert!(tokio::fs::try_exists(&chunk0).await.unwrap_or(false));

    let chunk1 = sealed_rx.recv().await.expect("Expected chunk 1 sealed");
    assert_eq!(chunk1, temp_dir.join("chat_0001.jsonl"));
    assert!(tokio::fs::try_exists(&chunk1).await.unwrap_or(false));

    let _ = server_task.await;
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
