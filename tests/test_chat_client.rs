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
    let ws_url = format!("ws://{}", addr);

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
    let chat_file = temp_dir.join("chat.jsonl");

    let client = ChzzkChatClient::new(
        "mock_channel".to_string(),
        "mock_access_token".to_string(),
        chat_file.clone(),
        Duration::from_millis(50),
        cancel_token.clone(),
    )
    .with_custom_ws_url(ws_url);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        cancel_clone.cancel();
    });

    let total = client.run(None).await.unwrap();
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
