use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::path::Path;

use anyhow::Context;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::chzzk::models_chat::RecordedChatMessage;
use crate::consolidation::manifest::{ConsolidationChunk, TargetLocation, resolve_rclone_bin};

/// Default sliding window duration for chat deduplication: 10 seconds (10,000 ms).
pub const DEFAULT_CHAT_DEDUP_WINDOW_MS: u64 = 10_000;

/// Composite identity key for chat deduplication: `(time_ms, user_id_hash, content)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChatMessageKey {
    pub time_ms: u64,
    pub user_id_hash: Option<String>,
    pub content: String,
}

impl ChatMessageKey {
    pub fn from_message(msg: &RecordedChatMessage) -> Self {
        Self {
            time_ms: msg.time_ms,
            user_id_hash: msg.user_id_hash.clone(),
            content: msg.content.clone(),
        }
    }
}

/// Statistics for chat consolidation and deduplication.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ChatConsolidationStats {
    pub total_messages: usize,
    pub deduplicated_messages: usize,
    pub malformed_messages: usize,
    pub emitted_messages: usize,
}

/// Pure in-memory sliding-window chat deduplicator.
///
/// Deduplicates chat messages based on `(time_ms, user_id_hash, content)` within a bounded
/// sliding window (default 10s) with $O(1)$ memory consumption and out-of-order timestamp tolerance.
pub struct ChatDeduplicator {
    window_ms: u64,
    strict: bool,
    max_time_ms: u64,
    seen: HashSet<ChatMessageKey>,
    heap: BinaryHeap<Reverse<(u64, ChatMessageKey)>>,
    stats: ChatConsolidationStats,
    warnings: Vec<String>,
}

impl ChatDeduplicator {
    /// Creates a new `ChatDeduplicator` with the specified window duration in milliseconds.
    pub fn new(window_ms: u64, strict: bool) -> Self {
        Self {
            window_ms,
            strict,
            max_time_ms: 0,
            seen: HashSet::new(),
            heap: BinaryHeap::new(),
            stats: ChatConsolidationStats::default(),
            warnings: Vec::new(),
        }
    }

    /// Creates a new `ChatDeduplicator` with the default 10-second sliding window.
    pub fn with_default_window(strict: bool) -> Self {
        Self::new(DEFAULT_CHAT_DEDUP_WINDOW_MS, strict)
    }

    pub fn window_ms(&self) -> u64 {
        self.window_ms
    }

    pub fn is_strict(&self) -> bool {
        self.strict
    }

    pub fn stats(&self) -> &ChatConsolidationStats {
        &self.stats
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Consumes the deduplicator and returns the final statistics.
    pub fn into_stats(self) -> ChatConsolidationStats {
        self.stats
    }

    /// Processes an individual `RecordedChatMessage`.
    ///
    /// Returns `true` if the message is unique and recorded within the sliding window,
    /// or `false` if it is a duplicate within the window.
    pub fn process_message(&mut self, msg: &RecordedChatMessage) -> bool {
        let key = ChatMessageKey::from_message(msg);
        let time_ms = msg.time_ms;

        // Check if message is already in current sliding window
        if self.seen.contains(&key) {
            return false;
        }

        // Advance max timestamp seen so far
        if time_ms > self.max_time_ms {
            self.max_time_ms = time_ms;
        }

        let cutoff = self.max_time_ms.saturating_sub(self.window_ms);

        // If within window, track in seen set and min-heap for eviction
        if time_ms >= cutoff {
            self.seen.insert(key.clone());
            self.heap.push(Reverse((time_ms, key)));
        }

        // Evict older entries from heap whose time_ms < cutoff
        while let Some(Reverse((t, _))) = self.heap.peek() {
            if *t < cutoff {
                if let Some(Reverse((_, evicted_key))) = self.heap.pop() {
                    self.seen.remove(&evicted_key);
                }
            } else {
                break;
            }
        }

        true
    }

    /// Processes a single raw JSON line.
    ///
    /// - Blank or whitespace lines are skipped (`Ok(None)`).
    /// - Malformed JSON is skipped and warned by default, or errors immediately if `strict: true`.
    /// - Valid unique lines return `Ok(Some(trimmed_line))`.
    /// - Valid duplicate lines return `Ok(None)`.
    pub fn process_line<'a>(&mut self, line: &'a str) -> anyhow::Result<Option<&'a str>> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        match serde_json::from_str::<RecordedChatMessage>(trimmed) {
            Ok(msg) => {
                self.stats.total_messages += 1;
                if self.process_message(&msg) {
                    self.stats.emitted_messages += 1;
                    Ok(Some(trimmed))
                } else {
                    self.stats.deduplicated_messages += 1;
                    Ok(None)
                }
            }
            Err(err) => {
                if self.strict {
                    anyhow::bail!("Malformed chat message JSON: {err} (input: {trimmed})");
                } else {
                    let msg = format!("Skipping malformed chat message JSON: {err}");
                    eprintln!("[WARN] {msg}");
                    self.warnings.push(msg);
                    self.stats.malformed_messages += 1;
                    Ok(None)
                }
            }
        }
    }
}

/// Helper function to join a remote base path with a filename, respecting rclone bucket syntax.
pub fn join_remote_path(remote_base: &str, file_name: &str) -> String {
    let base = remote_base.trim_end_matches('/');
    if base.ends_with(':') {
        format!("{base}{file_name}")
    } else {
        format!("{base}/{file_name}")
    }
}

/// Consolidates chat chunks for a recording session into a single deduplicated `consolidated.jsonl`.
///
/// Supports both local directories and remote rclone paths. Employs staged atomic finalization
/// (`consolidated.jsonl.part` -> `consolidated.jsonl`) and clean abort cleanup. Original chat chunks
/// are left untouched.
pub async fn consolidate_chat(
    target: &TargetLocation,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    rclone_bin: Option<&str>,
) -> anyhow::Result<ChatConsolidationStats> {
    if chat_chunks.is_empty() {
        return Ok(ChatConsolidationStats::default());
    }

    match target {
        TargetLocation::Local(dir) => consolidate_chat_local(dir, chat_chunks, strict).await,
        TargetLocation::Remote(remote_base) => {
            consolidate_chat_remote(remote_base, chat_chunks, strict, rclone_bin).await
        }
    }
}

/// Consolidates local chat chunks into `consolidated.jsonl` via staged `.part` file.
pub async fn consolidate_chat_local(
    dir: &Path,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
) -> anyhow::Result<ChatConsolidationStats> {
    let part_path = dir.join("consolidated.jsonl.part");
    let final_path = dir.join("consolidated.jsonl");

    // Clean up any stale .part file from previous aborted runs
    if part_path.exists() {
        let _ = tokio::fs::remove_file(&part_path).await;
    }

    let mut deduplicator = ChatDeduplicator::new(DEFAULT_CHAT_DEDUP_WINDOW_MS, strict);

    let stream_res = async {
        let part_file = tokio::fs::File::create(&part_path).await.with_context(|| {
            format!("Failed to create staged part file: {}", part_path.display())
        })?;
        let mut writer = tokio::io::BufWriter::new(part_file);

        for chunk in chat_chunks {
            let chunk_path = dir.join(&chunk.name);
            let file = tokio::fs::File::open(&chunk_path)
                .await
                .with_context(|| format!("Failed to open chat chunk: {}", chunk_path.display()))?;
            let mut reader = tokio::io::BufReader::new(file);
            let mut line = String::new();

            loop {
                line.clear();
                let bytes_read = reader.read_line(&mut line).await.with_context(|| {
                    format!(
                        "Failed to read line from chat chunk: {}",
                        chunk_path.display()
                    )
                })?;
                if bytes_read == 0 {
                    break;
                }
                if let Some(deduped_line) = deduplicator.process_line(&line)? {
                    writer.write_all(deduped_line.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                }
            }
        }

        writer.flush().await.with_context(|| {
            format!("Failed to flush staged part file: {}", part_path.display())
        })?;
        drop(writer);

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(err) = stream_res {
        // Clean up staged .part file on failure; original chunks remain untouched
        let _ = tokio::fs::remove_file(&part_path).await;
        return Err(err);
    }

    // Atomic finalization: rename .part to final target
    if final_path.exists() {
        let _ = tokio::fs::remove_file(&final_path).await;
    }
    tokio::fs::rename(&part_path, &final_path)
        .await
        .with_context(|| {
            format!(
                "Failed to atomically rename {} to {}",
                part_path.display(),
                final_path.display()
            )
        })?;

    Ok(deduplicator.into_stats())
}

/// Consolidates remote chat chunks via pure streaming (`rclone cat` -> deduplication -> `rclone rcat`).
pub async fn consolidate_chat_remote(
    remote_base: &str,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    rclone_bin: Option<&str>,
) -> anyhow::Result<ChatConsolidationStats> {
    let bin = resolve_rclone_bin(rclone_bin);
    let remote_part_path = join_remote_path(remote_base, "consolidated.jsonl.part");
    let remote_final_path = join_remote_path(remote_base, "consolidated.jsonl");

    let mut deduplicator = ChatDeduplicator::new(DEFAULT_CHAT_DEDUP_WINDOW_MS, strict);

    // Spawn rclone rcat sink process
    let mut rcat_cmd = tokio::process::Command::new(&bin);
    rcat_cmd.kill_on_drop(true);
    rcat_cmd.stdin(std::process::Stdio::piped());
    rcat_cmd.stdout(std::process::Stdio::null());
    rcat_cmd.stderr(std::process::Stdio::piped());
    rcat_cmd.arg("rcat").arg(&remote_part_path);

    let mut rcat_child = rcat_cmd
        .spawn()
        .with_context(|| format!("Failed to spawn '{bin} rcat {remote_part_path}'"))?;

    let mut rcat_stdin = rcat_child
        .stdin
        .take()
        .context("Failed to open stdin pipe for rclone rcat")?;

    let stream_res = async {
        for chunk in chat_chunks {
            let chunk_remote_path = join_remote_path(remote_base, &chunk.name);
            let mut cat_cmd = tokio::process::Command::new(&bin);
            cat_cmd.kill_on_drop(true);
            cat_cmd.stdin(std::process::Stdio::null());
            cat_cmd.stdout(std::process::Stdio::piped());
            cat_cmd.stderr(std::process::Stdio::piped());
            cat_cmd.arg("cat").arg(&chunk_remote_path);

            let mut cat_child = cat_cmd
                .spawn()
                .with_context(|| format!("Failed to spawn '{bin} cat {chunk_remote_path}'"))?;

            let cat_stdout = cat_child
                .stdout
                .take()
                .context("Failed to open stdout pipe for rclone cat")?;
            let mut reader = tokio::io::BufReader::new(cat_stdout);
            let mut line = String::new();

            loop {
                line.clear();
                let bytes_read = reader.read_line(&mut line).await.with_context(|| {
                    format!("Failed to read line from rclone cat {chunk_remote_path}")
                })?;
                if bytes_read == 0 {
                    break;
                }
                if let Some(deduped_line) = deduplicator.process_line(&line)? {
                    rcat_stdin.write_all(deduped_line.as_bytes()).await?;
                    rcat_stdin.write_all(b"\n").await?;
                }
            }

            let cat_output = cat_child
                .wait_with_output()
                .await
                .with_context(|| format!("Failed to await rclone cat for {chunk_remote_path}"))?;

            if !cat_output.status.success() {
                let stderr = String::from_utf8_lossy(&cat_output.stderr);
                anyhow::bail!(
                    "rclone cat for {chunk_remote_path} failed with status {}: {}",
                    cat_output.status,
                    stderr.trim()
                );
            }
        }

        rcat_stdin
            .flush()
            .await
            .context("Failed to flush stdin to rclone rcat")?;
        drop(rcat_stdin);

        let rcat_output = rcat_child
            .wait_with_output()
            .await
            .context("Failed to await rclone rcat completion")?;

        if !rcat_output.status.success() {
            let stderr = String::from_utf8_lossy(&rcat_output.stderr);
            anyhow::bail!(
                "rclone rcat failed with status {}: {}",
                rcat_output.status,
                stderr.trim()
            );
        }

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(err) = stream_res {
        // Error abort cleanup: remove remote .part file, leave original chunks untouched
        delete_remote_file(&bin, &remote_part_path).await;
        return Err(err);
    }

    // Atomic finalization: rclone moveto <part> <final>
    let mut move_cmd = tokio::process::Command::new(&bin);
    move_cmd.kill_on_drop(true);
    move_cmd.stdin(std::process::Stdio::null());
    move_cmd.stdout(std::process::Stdio::piped());
    move_cmd.stderr(std::process::Stdio::piped());
    move_cmd
        .arg("moveto")
        .arg(&remote_part_path)
        .arg(&remote_final_path);

    let move_output = move_cmd.output().await.with_context(|| {
        format!("Failed to execute '{bin} moveto {remote_part_path} {remote_final_path}'")
    })?;

    if !move_output.status.success() {
        let stderr = String::from_utf8_lossy(&move_output.stderr);
        anyhow::bail!(
            "rclone moveto failed with status {}: {}",
            move_output.status,
            stderr.trim()
        );
    }

    Ok(deduplicator.into_stats())
}

/// Helper function to safely delete a remote file via `rclone deletefile`.
pub async fn delete_remote_file(bin: &str, remote_path: &str) {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.kill_on_drop(true);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.arg("deletefile").arg(remote_path);

    if let Ok(output) = cmd.output().await {
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!(
                "[WARN] rclone deletefile for '{}' returned status {}: {}",
                remote_path,
                output.status,
                stderr.trim()
            );
        }
    }
}
