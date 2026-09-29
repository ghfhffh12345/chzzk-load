# Chat Chunking and Incremental Upload Architecture Specification

- **Date**: 2026-09-29
- **Status**: Approved Design
- **Target Components**: `src/recorder/chat_writer.rs`, `src/chzzk/chat.rs`, `src/engine.rs`, `src/uploader.rs`, `tests/`

---

## 1. Problem Statement & Motivation

In long-running live broadcasts (lasting multiple hours or days) or high-traffic streams, accumulating all chat messages into a single local file (`chat.jsonl`) causes unbounded disk space growth until the stream completes. On single-board computers (SBCs like Raspberry Pi) or environments with constrained storage, this unbounded growth risks exhausting disk space. Furthermore, if a crash or power outage occurs mid-stream, none of the recorded chat data has been uploaded to cloud storage.

In contrast, video segments (`chunk_0000.ts`, `chunk_0001.ts`, ...) are segmented every `chunk_duration_seconds` and uploaded sequentially in FIFO order, keeping local video disk footprint strictly bounded to 1–2 segments.

This specification refactors the chat recording and upload architecture so that live chat is progressively segmented into indexed chunk files (`chat_0000.jsonl`, `chat_0001.jsonl`, ...), sealed upon interval completion, enqueued into the cloud upload pipeline in real time, and deleted locally upon confirmed upload.

---

## 2. Goals & Non-Goals

### Goals
- **Strictly Bounded Local Disk Footprint**: At any given time, only active/in-flight chat chunks reside on disk. Local chat chunks are immediately removed once confirmed uploaded by the backend.
- **Timeline & Index Alignment**: Chat chunks are rotated at intervals matching video chunk duration (`chunk_duration_seconds`), producing `chat_%04d.jsonl` matching `chunk_%04d.ts`.
- **Zero Empty Files (Skip Silent Intervals)**: If zero chat messages arrive during an interval, no file is created or uploaded. The chunk index advances monotonically so that subsequent active intervals maintain time-aligned index numbers.
- **SBC Flash Longevity**: Preserve in-memory batching (capacity threshold: 500 messages or 64 KB, plus periodic timer flush) before disk I/O to avoid flash storage wear.
- **Zero Message Loss**: Full drain and flush of lingering in-memory messages upon stream termination, manifest EOF, or user cancellation.
- **Dual Cloud & Local-Only Support**: In cloud-enabled mode, chunks are uploaded and deleted. In local-only mode (`remote_path == ""`), sealed chunks remain preserved in the local session folder.

### Non-Goals
- Changing the underlying chat message serialization schema (`RecordedChatMessage`).
- Concatenating chat files on cloud storage remotes (chunks are uploaded as separate `.jsonl` files alongside `.ts` chunks).
- Supporting complex user-configurable chat chunk intervals (reusing `chunk_duration_seconds` eliminates redundant configuration and guarantees timeline parity with video).

---

## 3. Architecture & Data Flow

```
                              ┌───────────────────────────────────┐
                              │     Chzzk Chat WebSocket Server   │
                              └─────────────────┬─────────────────┘
                                                │ (WebSocket JSON Frames)
                                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ ChzzkChatClient                                                                 │
│                                                                                 │
│   • Manages WS handshake (cmd 100) & heartbeat ping/pong                        │
│   • Parses chat packets via parse_chat_packet_owned                             │
│   • Periodically calls maybe_flush_timer() and maybe_rotate()                   │
│                                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │ ChatWriter (Rotating)                                                   │   │
│   │                                                                         │   │
│   │   • buffer: Vec<u8> (64 KB capacity)                                    │   │
│   │   • chunk_index: usize (0, 1, 2, ...)                                   │   │
│   │   • chunk_start: Instant                                                │   │
│   │   • chunk_duration: Duration (from chunk_duration_seconds)              │   │
│   │   • messages_in_current_chunk: u64                                      │   │
│   │                                                                         │   │
│   │   Interval Elapsed:                                                     │   │
│   │     - Flush buffer to chat_%04d.jsonl                                   │   │
│   │     - If messages > 0: emit sealed PathBuf                              │   │
│   │     - Increment chunk_index += 1                                        │   │
│   │     - Reset chunk_start                                                 │   │
│   └────────────────────────────────────┬────────────────────────────────────┘   │
└────────────────────────────────────────┼────────────────────────────────────────┘
                                         │ (mpsc::channel<PathBuf>)
                                         │ Sealed chat chunk path
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ EngineOrchestrator                                                              │
│                                                                                 │
│   • Wraps sealed PathBuf into UploadTask:                                       │
│       UploadTask {                                                              │
│           channel_id, session_folder_id, remote_dir,                            │
│           chunk_path: sealed_path,                                              │
│           chunk_name: "chat_0000.jsonl",                                        │
│           streamer_name,                                                        │
│       }                                                                         │
│   • Emits AppEvent::ChunkSealed & AppEvent::Log                                 │
│   • Sends to upload_tx (UploadWorker)                                           │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ (mpsc::Sender<UploadTask>)
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ UploadWorker (Upload Consumer)                                                  │
│                                                                                 │
│   ├── [Cloud Backend Active]                                                    │
│   │     1. RcloneBackend::upload_file_and_delete(...)                            │
│   │     2. Emits AppEvent::UploadCompleted                                      │
│   │     3. Local file deleted immediately on confirmed HTTP 200                 │
│   │                                                                             │
│   └── [Local-Only Mode]                                                         │
│         Retains chat_0000.jsonl in local session directory                      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Details

### 4.1. `ChatWriter` (`src/recorder/chat_writer.rs`)

The `ChatWriter` is enhanced to manage file rotation across time intervals while keeping its high-performance in-memory byte buffer:

1. **State**:
   ```rust
   pub struct ChatWriter {
       session_dir: PathBuf,
       chunk_index: usize,
       chunk_duration: Duration,
       chunk_start: Instant,
       messages_in_current_chunk: u64,
       buffer: Vec<u8>,
       buffered_count: usize,
       capacity_threshold: usize,
       max_bytes_threshold: usize,
       flush_interval: Duration,
       last_flush: Instant,
       total_written: u64,
       dir_created: bool,
   }
   ```
2. **Rotating Constructor**:
   ```rust
   impl ChatWriter {
       pub fn new_rotating(
           session_dir: PathBuf,
           chunk_duration: Duration,
           flush_interval: Duration,
           capacity_threshold: usize,
       ) -> Self;
   }
   ```
3. **Lazy File Creation**:
   - `chat_%04d.jsonl` is only opened on disk when `flush()` is called with `buffered_count > 0`.
   - If an interval completes with `messages_in_current_chunk == 0`, no file was ever created on disk.
4. **Rotation & Sealing Logic (`maybe_rotate`)**:
   ```rust
   pub async fn maybe_rotate(&mut self) -> Result<Option<PathBuf>> {
       if self.chunk_start.elapsed() < self.chunk_duration {
           return Ok(None);
       }

       // Flush any remaining buffered messages in memory to disk
       self.flush().await?;

       let sealed = if self.messages_in_current_chunk > 0 {
           let path = self.current_chunk_path();
           Some(path)
       } else {
           None
       };

       self.chunk_index += 1;
       self.messages_in_current_chunk = 0;
       self.chunk_start = Instant::now();

       Ok(sealed)
   }
   ```
5. **Session Completion (`flush_and_close`)**:
   ```rust
   pub async fn flush_and_close(&mut self) -> Result<(u64, Option<PathBuf>)> {
       self.flush().await?;
       let sealed = if self.messages_in_current_chunk > 0 {
           Some(self.current_chunk_path())
       } else {
           None
       };
       Ok((self.total_written, sealed))
   }
   ```

### 4.2. `ChzzkChatClient` (`src/chzzk/chat.rs`)

1. **Constructor & Runner**:
   - Accepts `session_dir: PathBuf`, `chunk_duration: Duration`, `flush_interval: Duration`, and `cancel_token: CancellationToken`.
   - `run` accepts `sealed_tx: Option<tokio::sync::mpsc::Sender<PathBuf>>` alongside `on_stats: Option<Sender<u64>>`.
2. **Loop Execution**:
   - Runs a periodic 200ms tick:
     1. Calls `writer.maybe_flush_timer().await`.
     2. Calls `writer.maybe_rotate().await`. If a chunk is sealed, sends its `PathBuf` to `sealed_tx`.
   - On cancellation or loop termination:
     1. Calls `writer.flush_and_close().await`.
     2. If a final chunk is returned, sends its `PathBuf` to `sealed_tx`.
     3. Forwards total messages to `on_stats`.

### 4.3. `EngineOrchestrator` (`src/engine.rs`)

1. **Session Setup (`spawn_recording_session`)**:
   - Allocates an `mpsc::channel::<PathBuf>(32)` (`chat_sealed_tx`, `chat_sealed_rx`).
   - Passes `chat_sealed_tx` to `ChzzkChatClient::run`.
   - Spawns an asynchronous forwarder task to receive sealed chat chunk paths:
     ```rust
     let mut chat_rx = chat_sealed_rx;
     let upload_tx = upload_tx.clone();
     let event_tx = event_tx.clone();
     let channel_id = channel_id.clone();
     let session_folder = session_folder_name.clone();
     let streamer = info.streamer_name.clone();
     let backend_active = backend_opt.is_some();

     tokio::spawn(async move {
         while let Some(chat_path) = chat_rx.recv().await {
             if let Some(chunk_name) = chat_path.file_name().and_then(|n| n.to_str()) {
                 let size = tokio::fs::metadata(&chat_path)
                     .await
                     .map(|m| m.len())
                     .unwrap_or(0);

                 let _ = event_tx.send(AppEvent::ChunkSealed {
                     chunk_name: chunk_name.to_string(),
                     size_bytes: size,
                 }).await;

                 if backend_active {
                     let _ = upload_tx.send(UploadTask {
                         channel_id: channel_id.clone(),
                         session_folder_id: session_folder.clone(),
                         remote_dir: session_folder.clone(),
                         chunk_path: chat_path,
                         chunk_name: chunk_name.to_string(),
                         streamer_name: streamer.clone(),
                     }).await;

                     let _ = event_tx.send(AppEvent::Log(LogEntry::rec(format!(
                         "{chunk_name} sealed. Pushed to cloud upload queue."
                     )))).await;
                 } else {
                     let _ = event_tx.send(AppEvent::Log(LogEntry::rec(format!(
                         "{chunk_name} sealed (saved locally)."
                     )))).await;
                 }
             }
         }
     });
     ```
2. **Session Teardown**:
   - Completely removes the legacy monolithic upload code (`backend.upload_file_and_delete(&chat_path, ...)`).
   - Once the chat client finishes and closes its sender, `chat_rx` drains all remaining sealed chunks into `upload_tx`.
   - Strict FIFO upload order ensures all video chunks and chat chunks are uploaded before session directory cleanup.

---

## 5. Invariants & Guarantees

1. **Bounded Disk Footprint**: Video segments and chat segments are deleted immediately upon backend upload completion. At any time, no more than 1 chat segment resides on disk per stream.
2. **Zero-Byte File Suppression**: Intervals with zero chat messages produce no file on disk and no upload tasks.
3. **Monotonic Index Synchronization**: `chunk_index` tracks interval sequence $(0, 1, 2, \dots)$ so non-empty chat files match corresponding video chunk time intervals.
4. **SBC Flash Protection**: Buffered serialization (`Vec<u8>`) avoids small, frequent writes to flash memory.
5. **No Terminal Pollution**: All status, sealing, and upload events are routed strictly via `AppEvent` to the Ratatui TUI dashboard.

---

## 6. Testing Strategy

1. **`tests/test_chat_writer.rs`**:
   - `test_chat_writer_rotates_on_interval`: Write messages across time intervals; verify `chat_0000.jsonl` and `chat_0001.jsonl` are sealed and emitted with accurate message counts.
   - `test_chat_writer_skips_empty_intervals`: Advance interval with zero messages; verify no file is created on disk and next active interval uses incremented index.
   - `test_chat_writer_flush_and_close_final_chunk`: Verify final buffered messages are flushed and sealed upon termination.
2. **`tests/test_chat_client.rs`**:
   - `test_chat_client_sealed_chunks_emission`: Connect to mock WebSocket server, push messages, trigger interval rotation, verify `sealed_tx` receives sealed chunk paths.
3. **`tests/test_engine_chat.rs`**:
   - `test_engine_orchestrator_chat_chunk_upload_and_delete`: Verify `EngineOrchestrator` receives sealed chat chunks, pushes to `MockUploadBackend`, and local files are deleted upon upload.
   - `test_engine_orchestrator_chat_chunk_local_only`: Verify in local-only mode, chunks remain safely preserved in the session folder.
