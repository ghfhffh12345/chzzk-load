# Chat Chunking and Incremental Upload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor chat archiving in `chzzk-load` from a single monolithic `chat.jsonl` uploaded at recording completion into time-aligned, bounded chunk files (`chat_0000.jsonl`, `chat_0001.jsonl`, ...) that upload and delete incrementally in real time.

**Architecture:** Extend `ChatWriter` to lazily write indexed `chat_%04d.jsonl` files rotated every `chunk_duration_seconds`. Connect `ChzzkChatClient` to `EngineOrchestrator` via an async `mpsc::channel<PathBuf>` so sealed chat chunks are immediately enqueued into the existing `UploadTask` pipeline, uploaded via `RcloneBackend`, and deleted upon confirmation (or retained on disk in local-only mode).

**Tech Stack:** Rust 2024 / 1.75+, Tokio async runtime, `serde_json`, `anyhow`, `tokio_tungstenite`.

**Spec:** [`docs/superpowers/specs/2026-09-29-chat-chunking-and-rotation-design.md`](file:///C:/Users/official/Documents/Code/chzzk-load/docs/superpowers/specs/2026-09-29-chat-chunking-and-rotation-design.md)

## Global Constraints

- **File Naming**: Chat chunks must be named `chat_%04d.jsonl` (e.g. `chat_0000.jsonl`, `chat_0001.jsonl`) to match `chunk_%04d.ts`.
- **Interval Alignment**: Rotation duration must match `chunk_duration_seconds` from `GeneralConfig`.
- **Zero Empty Files**: Intervals with 0 chat messages must never create a file on disk or emit an upload task.
- **Monotonic Indexing**: `chunk_index` must increment unconditionally on each elapsed interval ($0, 1, 2, \dots$) so chat chunk numbers remain time-aligned with video chunks.
- **SBC Flash Longevity**: Preserve in-memory buffer batching (500 messages / 64 KB) with periodic timer flushing before writing to disk.
- **Bounded Disk Footprint**: In cloud mode, uploaded chat chunks must be deleted immediately after upload confirmation.
- **Zero Direct Terminal I/O**: All logging must route through `AppEvent::Log` / `AppEvent::ChunkSealed`.

## Review Focus

1. **Zero-message quiet interval followed by busy interval**: Verify `chat_0000.jsonl` is created, interval 1 is skipped without any file, and interval 2 produces `chat_0002.jsonl`.
2. **Session cancellation mid-interval**: Verify any buffered messages in memory are flushed to `chat_%04d.jsonl` and emitted as the final sealed chunk.
3. **Session cancellation with 0 messages in current interval**: Verify no empty file is created and no final chunk is emitted.
4. **Local-only recording mode (`remote_path == ""`)**: Verify sealed `chat_%04d.jsonl` files remain preserved in the local session folder.
5. **Drain order on stream termination**: Verify all sealed chat chunks are enqueued to `upload_tx` before session directory cleanup is evaluated.

---

### Task 1: Refactor `ChatWriter` for Rotating Chunk Management

**Files:**
- Modify: `src/recorder/chat_writer.rs`
- Test: `tests/test_chat_writer.rs`

**Interfaces:**
- Produces:
  ```rust
  impl ChatWriter {
      pub fn new_rotating(
          session_dir: PathBuf,
          chunk_duration: Duration,
          flush_interval: Duration,
          capacity_threshold: usize,
      ) -> Self;
      pub fn current_chunk_index(&self) -> usize;
      pub fn current_chunk_path(&self) -> PathBuf;
      pub async fn push(&mut self, msg: RecordedChatMessage) -> Result<()>;
      pub async fn maybe_flush_timer(&mut self) -> Result<bool>;
      pub async fn maybe_rotate(&mut self) -> Result<Option<PathBuf>>;
      pub async fn flush_and_close(&mut self) -> Result<(u64, Option<PathBuf>)>;
  }
  ```

- [ ] **Step 1: Write failing unit tests for rotating `ChatWriter`**

In `tests/test_chat_writer.rs`, add tests:
- `test_chat_writer_rotates_on_interval`: Write messages, advance mock time/interval, call `maybe_rotate()`, verify `chat_0000.jsonl` exists and is sealed, verify next chunk is `chat_0001.jsonl`.
- `test_chat_writer_skips_empty_interval`: Call `maybe_rotate()` when no messages were pushed; verify `None` returned, no file created on disk, and `current_chunk_index()` is `1`.
- `test_chat_writer_flush_and_close_with_lingering_messages`: Push message, call `flush_and_close()`, verify final chunk is returned and written.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_chat_writer`
Expected: FAIL due to missing `new_rotating`, `maybe_rotate`, etc.

- [ ] **Step 3: Implement rotating chunk logic in `src/recorder/chat_writer.rs`**

- Add fields: `session_dir: PathBuf`, `chunk_index: usize`, `chunk_duration: Duration`, `chunk_start: Instant`, `messages_in_current_chunk: u64`.
- Keep existing `new(target_path, ...)` as a convenience or update it to delegate to single-file mode for existing caller compatibility.
- In `flush()`: lazily create `session_dir` and open `current_chunk_path()` when `self.buffered_count > 0`.
- In `maybe_rotate()`: check `chunk_start.elapsed() >= chunk_duration`. If elapsed, `flush().await?`, check if `messages_in_current_chunk > 0`. If so, return `Some(path)`. Increment `chunk_index += 1`, reset `messages_in_current_chunk = 0`, reset `chunk_start = Instant::now()`.
- In `flush_and_close()`: call `flush().await?`, return `(total_written, if messages_in_current_chunk > 0 { Some(path) } else { None })`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_chat_writer`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/recorder/chat_writer.rs tests/test_chat_writer.rs
git commit -m "feat(recorder): implement rotating chunk management in ChatWriter"
```

---

### Task 2: Update `ChzzkChatClient` to Rotate and Emit Sealed Chunks

**Files:**
- Modify: `src/chzzk/chat.rs`
- Test: `tests/test_chat_client.rs`

**Interfaces:**
- Consumes: `ChatWriter::new_rotating`, `ChatWriter::maybe_rotate`, `ChatWriter::flush_and_close` from Task 1.
- Produces:
  ```rust
  impl ChzzkChatClient {
      pub fn new(
          chat_channel_id: impl Into<String>,
          access_token: impl Into<String>,
          session_dir: impl Into<PathBuf>,
          chunk_duration: Duration,
          flush_interval: Duration,
          cancel_token: CancellationToken,
      ) -> Self;
      pub async fn run(
          &self,
          on_stats: Option<tokio::sync::mpsc::Sender<u64>>,
          sealed_tx: Option<tokio::sync::mpsc::Sender<PathBuf>>,
      ) -> Result<u64>;
  }
  ```

- [ ] **Step 1: Write failing integration test in `tests/test_chat_client.rs`**

Add `test_chat_client_emits_sealed_chunks`:
- Spin up mock WebSocket server.
- Instantiate `ChzzkChatClient` with a short `chunk_duration` (e.g. 500ms).
- Provide `(sealed_tx, mut sealed_rx) = mpsc::channel(10)`.
- Send mock chat packets across intervals.
- Await `sealed_rx.recv()`, verify received `chat_0000.jsonl` exists and is non-empty.
- Cancel client, verify final chunk emitted if non-empty.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_chat_client test_chat_client_emits_sealed_chunks`
Expected: FAIL due to signature differences.

- [ ] **Step 3: Implement chunk rotation and sealed notification in `src/chzzk/chat.rs`**

- Update `ChzzkChatClient` fields to hold `session_dir: PathBuf` and `chunk_duration: Duration`.
- In `run(...)`:
  - Construct `writer = ChatWriter::new_rotating(self.session_dir.clone(), self.chunk_duration, self.flush_interval, 500)`.
  - In periodic 200ms `flush_timer`:
    - `writer.maybe_flush_timer().await`
    - `if let Ok(Some(sealed_path)) = writer.maybe_rotate().await { if let Some(ref tx) = sealed_tx { let _ = tx.send(sealed_path).await; } }`
  - On loop exit:
    - `let (total, final_sealed) = writer.flush_and_close().await?;`
    - `if let Some(final_path) = final_sealed { if let Some(ref tx) = sealed_tx { let _ = tx.send(final_path).await; } }`
    - Forward `total` to `on_stats`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test test_chat_client`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/chzzk/chat.rs tests/test_chat_client.rs
git commit -m "feat(chat): emit sealed chat chunks via mpsc in ChzzkChatClient"
```

---

### Task 3: Integrate Incremental Chat Upload in `EngineOrchestrator`

**Files:**
- Modify: `src/engine.rs`
- Test: `tests/test_engine_chat.rs`

**Interfaces:**
- Consumes: `ChzzkChatClient::new` and `ChzzkChatClient::run(on_stats, sealed_tx)` from Task 2.
- Produces: Live dispatch of `UploadTask { chunk_path, chunk_name: "chat_%04d.jsonl", ... }` into `upload_tx`.

- [ ] **Step 1: Write failing test in `tests/test_engine_chat.rs`**

Add `test_engine_orchestrator_chat_incremental_upload_and_delete`:
- Configure `MockUploadBackend`.
- Set `chunk_duration_seconds = 1`, `record_chat = true`.
- Start session with mock chat WS server sending messages.
- Wait for at least 1 chat chunk to rotate and seal.
- Verify `MockUploadBackend::uploads` contains `chat_0000.jsonl`.
- Verify `chat_0000.jsonl` was deleted from disk after upload.
- Verify legacy single `chat.jsonl` is not uploaded or created.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_engine_chat test_engine_orchestrator_chat_incremental_upload_and_delete`
Expected: FAIL

- [ ] **Step 3: Implement chat chunk queueing and remove legacy monolithic upload in `src/engine.rs`**

- In `spawn_recording_session`:
  - Allocate `(chat_sealed_tx, mut chat_sealed_rx) = mpsc::channel::<PathBuf>(32)`.
  - Pass `chat_sealed_tx` to `client.run(Some(stats_tx), Some(chat_sealed_tx))`.
  - Pass `chunk_dur = Duration::from_secs(settings.general.chunk_duration_seconds)` and `session_dir.clone()` to `ChzzkChatClient::new`.
  - Spawn an asynchronous chunk forwarder task consuming `chat_sealed_rx`:
    - For each `chat_path` received:
      - Emit `AppEvent::ChunkSealed`.
      - If `backend_opt.is_some()`:
        - Send `UploadTask` with `chunk_name: chat_path.file_name()`, `chunk_path`.
        - Emit `AppEvent::Log("{chunk_name} sealed. Pushed to cloud upload queue.")`.
      - If local-only mode:
        - Emit `AppEvent::Log("{chunk_name} sealed (saved locally).")`.
  - In session teardown:
    - Remove the old `backend.upload_file_and_delete(&session_dir.join("chat.jsonl"), ...)` block (lines 1126–1150).
    - Await chat task completion so all sealed chunks (including final lingering chunk) are delivered to `chat_sealed_rx` and pushed to `upload_tx`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_engine_chat`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs tests/test_engine_chat.rs
git commit -m "feat(engine): stream sealed chat chunks incrementally into upload pipeline"
```

---

### Task 4: Full Test Suite Verification, Documentation & Linting

**Files:**
- Modify: `README.md`, `README.ko.md`, `AGENTS.md`
- Test: All test suites (`cargo test --all-targets`)

**Interfaces:**
- Verifies system invariants across all components.

- [ ] **Step 1: Update documentation in README and AGENTS.md**

- Update `AGENTS.md`: Update section 1 and 3.3 to document `chat_%04d.jsonl` incremental rotation and upload lifecycle, replacing descriptions of the legacy monolithic `chat.jsonl`.
- Update `README.md` and `README.ko.md` if chat recording output structure is described.

- [ ] **Step 2: Run linter and formatting checks**

Run: `cargo clippy --all-targets -- -D warnings`
Run: `cargo fmt --check`
Expected: Zero warnings, zero formatting discrepancies.

- [ ] **Step 3: Run the complete test suite**

Run: `cargo test`
Expected: All unit, integration, and engine tests PASS.

- [ ] **Step 4: Commit**

```bash
git add README.md README.ko.md AGENTS.md
git commit -m "docs: update architecture documentation for incremental chat chunks"
```
