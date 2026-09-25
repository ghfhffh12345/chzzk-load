# Design Specification: Real-Time Chat Recording for `chzzk-load`

## 1. Overview & Goals

`chzzk-load` records Naver Chzzk live video broadcasts into losslessly segmented MPEG-TS chunks and concurrently uploads them to Google Drive with a strictly bounded local disk footprint.

This feature adds real-time chat recording alongside the live broadcast video stream. The chat capture operates losslessly, storing chat events into a structured JSON Lines file (`chat.jsonl`), optimizes file I/O to protect microSD cards on low-spec Single Board Computers (SBCs), integrates cleanly into the existing recording session lifecycle, and uploads the final chat log to Google Drive upon broadcast completion.

### Key Goals
1. **Lossless Real-Time Capture**: Connect directly to Chzzk's WebSocket chat infrastructure to receive chat messages, donations, subscriptions, and system notices without missing bursts.
2. **Flash & MicroSD Protection**: Provide in-memory batched buffering (by time and size thresholds) to reduce disk write cycles by over 98%, preserving microSD card longevity on Raspberry Pi and other SBCs.
3. **Structured Archival Format**: Save messages in standard JSON Lines (`chat.jsonl`), preserving timestamps, user nicknames, badges, text content, donation amounts, and raw event payloads for future replay tools.
4. **Session Lifecycle & Drive Upload**: Seamlessly start and stop with the video recording session, cleanly flush all buffered messages upon completion, and upload `chat.jsonl` to the broadcast's Google Drive folder.
5. **Standalone Binary Preservation**: Maintain pure Rust async implementation via `tokio` and `tokio-tungstenite`, preserving `chzzk-load`'s single standalone executable architecture across Windows, Linux (x86_64, aarch64 musl), and macOS.

---

## 2. Protocol & Architecture

### 2.1. Chzzk Chat Protocol Specification
Chzzk's chat system communicates over secure WebSockets (`wss://`) using a JSON packet envelope:

```json
{
  "cmd": 100,
  "ver": "2",
  "svcid": "game",
  "cid": "<chat_channel_id>",
  "tid": 1,
  "bdy": { ... }
}
```

#### Protocol Command Codes (`ChatCmd`)
| Code | Name | Direction | Description |
|---|---|---|---|
| `0` | `PING` | Server $\to$ Client or Client $\to$ Server | Keep-alive heartbeat request |
| `10000` | `PONG` | Client $\to$ Server or Server $\to$ Client | Keep-alive heartbeat response |
| `100` | `CONNECT` | Client $\to$ Server | Initial handshake and authentication request |
| `10100` | `CONNECTED` | Server $\to$ Client | Handshake acknowledgment containing session ID (`sid`) |
| `93101` | `CHAT` | Server $\to$ Client | Real-time chat message broadcast |
| `93102` | `DONATION` | Server $\to$ Client | Sponsorship / donation event |
| `93103` | `SUBSCRIPTION` | Server $\to$ Client | Channel subscription event |
| `94008` | `BLIND` | Server $\to$ Client | Message deletion / moderation event |

#### Protocol Message Types (`ChatTypeCode`)
- `1`: Normal text chat (`TEXT`)
- `10`: Paid donation message (`DONATION`)
- `11`: Channel subscription notice (`SUBSCRIPTION`)
- `30`: Official system message (`SYSTEM_MESSAGE`)

### 2.2. Authentication & Server Discovery Flow
1. **Chat Channel ID**:
   Extracted from the `live-detail` API response (`content.chat_channel_id`). Fallback to `polling/v2/channels/{channel_id}/live-status` if missing.
2. **Access Token**:
   `ChzzkClient::get_chat_access_token(&self, chat_channel_id: &str)` performs an HTTP GET request to:
   ```
   https://comm-api.game.naver.com/nng_main/v1/chats/access-token?channelId={chat_channel_id}&chatType=STREAMING
   ```
   Supplying user cookies (`NID_AUT`, `NID_SES`) if configured. The response yields `accessToken` and optional `extraToken`.
3. **Server Assignment**:
   Distributed across 9 WebSocket servers:
   $$\text{server\_id} = \left(\sum \text{char\_code}(\text{char}) \bmod 9\right) + 1$$
   Endpoint: `wss://kr-ss{server_id}.chat.naver.com/chat`.
4. **Handshake**:
   Send `cmd: 100` (`CONNECT`) packet with `accTkn`, `auth: "READ"`, `devType: 2001`, `uid: null` (or user ID hash if authenticated). Server returns `cmd: 10100` (`CONNECTED`) with session ID `sid`.
5. **Heartbeat Maintenance**:
   - Reply to server `cmd: 0` (`PING`) with `{"cmd": 10000, "ver": "2"}` (`PONG`).
   - Emit client-initiated `{"cmd": 0, "ver": "2"}` every 20 seconds.

---

## 3. MicroSD & Flash-Friendly Batched Writer (`ChatWriter`)

To prevent flash cell wear and high I/O latency on SBCs (e.g. Raspberry Pi running from microSD):
1. **In-Memory Batching**:
   - Received and parsed chat entries are serialized into JSON lines and queued in an in-memory buffer (`Vec<String>`).
2. **Dual-Trigger Disk Flush**:
   - Flushes occur only when **either**:
     - The in-memory buffer reaches capacity (**500 messages** or **64 KB**), OR
     - The periodic timer expires (**every 30 seconds**, configurable via `chat_flush_interval_seconds`).
3. **Sequential Appends**:
   - Employs `tokio::fs::OpenOptions::new().create(true).append(true)` wrapped in `tokio::io::BufWriter`.
   - Avoids synchronous hardware `fsync` calls during live broadcasting, allowing Linux kernel page caches to coalesce operations.
4. **Guaranteed Termination Flush**:
   - On broadcast completion or cancellation (`cancel_token`), all remaining messages in the buffer are flushed and the file is cleanly closed before Google Drive upload proceeds.

### Output Schema: `chat.jsonl`
Each line is a single JSON object:
```json
{
  "time_ms": 1727268158000,
  "datetime": "2026-09-25 21:42:38",
  "msg_type": "TEXT",
  "nickname": "Viewer123",
  "user_id_hash": "a1b2c3d4...",
  "content": "Hello world!",
  "donation_amount": null,
  "extras": { ... },
  "raw": { ... }
}
```

---

## 4. Engine & Session Orchestrator Integration

### 4.1. Lifecycle Management (`src/engine/mod.rs`)
- When `EngineOrchestrator::spawn_recording_session` starts:
  - If `settings.general.record_chat` is `true`:
    1. Spawn the FFmpeg child process for video chunking.
    2. Concurrently spawn `run_chat_session(...)` as an asynchronous Tokio task sharing the session's `CancellationToken`.
    3. The chat task maintains connection, reconnects on transient network drops with backoff, parses packets, feeds `ChatWriter`, and periodically emits `AppEvent::ChatStats { channel_id, message_count }`.

### 4.2. Google Drive Upload on Termination
- When the live broadcast ends (or user cancels via TUI):
  1. The chat task flushes and closes `<session_dir>/chat.jsonl`.
  2. If Google Drive is enabled (`drive_opt` is `Some`):
     - Resolves the session's Drive subfolder (`[YYYY-MM-DD_HHMM] Streamer - Title`).
     - Uploads `chat.jsonl` using `drive.upload_resumable(...)` or `drive.upload_text_file(...)`.
     - Logs: `[DRIVE] Uploaded 'chat.jsonl' (N messages) for <channel_id>`.
     - Deletes local `chat.jsonl` after confirmed upload to uphold the strictly bounded disk footprint invariant.
  3. If Google Drive is disabled:
     - Preserves `chat.jsonl` in `<recordings_dir>/<session_dir>/` along with video chunks.

---

## 5. Configuration & TUI Dashboard

### 5.1. Settings (`src/config.rs`)
Add configuration options to `GeneralConfig` with backward-compatible defaults:
```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneralConfig {
    // ... existing fields ...
    #[serde(default = "default_record_chat")]
    pub record_chat: bool,
    #[serde(default = "default_chat_flush_interval")]
    pub chat_flush_interval_seconds: u64,
}

fn default_record_chat() -> bool { true }
fn default_chat_flush_interval() -> u64 { 30 }
```

### 5.2. TUI Dashboard (`src/tui/`)
1. **`[CHAT]` Log Category**:
   - `LogEntry::chat(msg)` rendered in Cyan/Magenta.
   - Restrict logging to lifecycle milestones (`Connected`, `Flushed`, `Completed`) to prevent log pane spam.
2. **Channel Table Indicator**:
   - In active recording rows, display chat message counts (e.g. `14 segs (842 chats)`).

---

## 6. Testing & Quality Assurance Plan

### 6.1. Unit & Mock Integration Tests
- **Mock Token Endpoint**: Use `tiny_http::Server::http("127.0.0.1:0")` to mock the access token API.
- **Mock WebSocket Server**: Use `tokio_tungstenite` over a local ephemeral port to test:
  - `CONNECT (100)` handshake and response `CONNECTED (10100)`.
  - Heartbeat `PING (0)` / `PONG (10000)` handling.
  - Multi-packet event parsing (`CHAT`, `DONATION`, `SUBSCRIPTION`).
- **Batched Writer Verification**:
  - Test time-based flush triggers.
  - Test capacity-based flush triggers (500 messages / 64 KB).
  - Verify zero-data-loss shutdown flushes.
  - Verify JSON Lines validity for output files.
- **Engine Lifecycle Test**:
  - Verify chat task starts with recording session and terminates with cancellation token.
  - Verify `chat.jsonl` Google Drive upload when enabled.

### 6.2. Validation Commands
- `cargo check --all-targets`
- `cargo test --all-targets`
- `cargo clippy --all-targets -- -D warnings`
- `cargo fmt --check`
- `node scripts/test-npm-packages.js`
