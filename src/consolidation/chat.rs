use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Context;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::chzzk::models_chat::RecordedChatMessage;
use crate::consolidation::manifest::{
    ConsolidationChunk, TargetLocation, join_remote_path, resolve_rclone_bin,
};
use crate::uploader::{rename_local_file_with_retry, unlink_local_file_with_retry};
use tokio_util::sync::CancellationToken;

use super::progress::ChatProgressUpdate;

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

/// Configuration options for chat consolidation.
#[derive(Debug, Clone, Default)]
pub struct ChatConsolidationOptions {
    pub rclone_bin: Option<String>,
    pub staging_dir: Option<PathBuf>,
    pub progress_sender: Option<tokio::sync::mpsc::UnboundedSender<ChatProgressUpdate>>,
}

impl ChatConsolidationOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_rclone_bin(mut self, bin: impl Into<String>) -> Self {
        self.rclone_bin = Some(bin.into());
        self
    }

    pub fn with_staging_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.staging_dir = Some(dir.into());
        self
    }

    pub fn with_progress_sender(
        mut self,
        sender: tokio::sync::mpsc::UnboundedSender<ChatProgressUpdate>,
    ) -> Self {
        self.progress_sender = Some(sender);
        self
    }
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

/// Atomically finalizes the staged `chat.jsonl.part` into `chat.jsonl`.
///
/// In local mode: unlinks any pre-existing destination and renames with Windows retry logic.
/// In remote mode: executes `rclone moveto <staged> <final>`.
pub async fn finalize_staged_chat(
    target: &TargetLocation,
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    match target {
        TargetLocation::Local(dir) => {
            let part_path = dir.join("chat.jsonl.part");
            let final_path = dir.join("chat.jsonl");
            let _ = unlink_local_file_with_retry(&final_path).await;
            rename_local_file_with_retry(&part_path, &final_path)
                .await
                .with_context(|| {
                    format!(
                        "Failed to atomically rename {} to {}",
                        part_path.display(),
                        final_path.display()
                    )
                })?;
            Ok(())
        }
        TargetLocation::Remote(remote_base) => {
            let bin = resolve_rclone_bin(rclone_bin);
            let remote_part_path = join_remote_path(remote_base, "chat.jsonl.part");
            let remote_final_path = join_remote_path(remote_base, "chat.jsonl");

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
            Ok(())
        }
    }
}

/// Deletes the temporary staged `chat.jsonl.part` if present upon error or abort.
///
/// In local mode: unlinks `chat.jsonl.part` with retry (treating NotFound as success).
/// In remote mode: unlinks local staged `chat.jsonl.part` and executes `rclone deletefile <staged>` on remote.
/// Finalized `chat.jsonl` is preserved to enable resilient upload resume (Ticket #72, ADR 0011).
pub async fn cleanup_staged_chat(
    target: &TargetLocation,
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    cleanup_staged_chat_in(target, None::<&Path>, rclone_bin).await
}

/// Deletes temporary staged chat part files with an optional custom staging directory path.
pub async fn cleanup_staged_chat_in<P: AsRef<Path>>(
    target: &TargetLocation,
    custom_temp_dir: Option<P>,
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    match target {
        TargetLocation::Local(dir) => {
            let part_path = dir.join("chat.jsonl.part");
            let _ = unlink_local_file_with_retry(&part_path).await;
            Ok(())
        }
        TargetLocation::Remote(remote_base) => {
            let staging_dir = super::staging::resolve_staging_dir(target, custom_temp_dir);
            let staged_local_part = staging_dir.join("chat.jsonl.part");
            let _ = unlink_local_file_with_retry(&staged_local_part).await;

            let bin = resolve_rclone_bin(rclone_bin);
            let remote_part_path = join_remote_path(remote_base, "chat.jsonl.part");
            delete_remote_file(&bin, &remote_part_path).await;
            Ok(())
        }
    }
}

/// Consolidates chat chunks for a recording session with full configuration options.
pub async fn consolidate_chat_with_options(
    target: &TargetLocation,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    options: &ChatConsolidationOptions,
    cancel_token: CancellationToken,
) -> anyhow::Result<ChatConsolidationStats> {
    if chat_chunks.is_empty() {
        return Ok(ChatConsolidationStats::default());
    }

    match target {
        TargetLocation::Local(dir) => {
            consolidate_chat_local_with_progress(
                dir,
                chat_chunks,
                strict,
                cancel_token,
                options.progress_sender.clone(),
            )
            .await
        }
        TargetLocation::Remote(remote_base) => {
            consolidate_chat_remote_with_options(
                remote_base,
                chat_chunks,
                strict,
                options,
                cancel_token,
            )
            .await
        }
    }
}

/// Consolidates chat chunks for a recording session into a staged deduplicated `chat.jsonl.part`.
///
/// Supports both local directories and remote rclone paths. Employs staged streaming write
/// into `chat.jsonl.part` without final rename, allowing atomic two-stage finalization
/// in `run_consolidation`. Original chat chunks are left untouched.
pub async fn consolidate_chat(
    target: &TargetLocation,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    rclone_bin: Option<&str>,
    cancel_token: CancellationToken,
) -> anyhow::Result<ChatConsolidationStats> {
    consolidate_chat_with_options(
        target,
        chat_chunks,
        strict,
        &ChatConsolidationOptions {
            rclone_bin: rclone_bin.map(str::to_string),
            staging_dir: None,
            progress_sender: None,
        },
        cancel_token,
    )
    .await
}

/// Consolidates chat chunks with optional progress telemetry reporting.
pub async fn consolidate_chat_with_progress(
    target: &TargetLocation,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    rclone_bin: Option<&str>,
    cancel_token: CancellationToken,
    progress_sender: Option<tokio::sync::mpsc::UnboundedSender<ChatProgressUpdate>>,
) -> anyhow::Result<ChatConsolidationStats> {
    consolidate_chat_with_options(
        target,
        chat_chunks,
        strict,
        &ChatConsolidationOptions {
            rclone_bin: rclone_bin.map(str::to_string),
            staging_dir: None,
            progress_sender,
        },
        cancel_token,
    )
    .await
}

/// Consolidates local chat chunks into staged `chat.jsonl.part`.
pub async fn consolidate_chat_local(
    dir: &Path,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    cancel_token: CancellationToken,
) -> anyhow::Result<ChatConsolidationStats> {
    consolidate_chat_local_with_progress(dir, chat_chunks, strict, cancel_token, None).await
}

/// Consolidates local chat chunks into staged `chat.jsonl.part` with progress telemetry.
pub async fn consolidate_chat_local_with_progress(
    dir: &Path,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    cancel_token: CancellationToken,
    progress_sender: Option<tokio::sync::mpsc::UnboundedSender<ChatProgressUpdate>>,
) -> anyhow::Result<ChatConsolidationStats> {
    let part_path = dir.join("chat.jsonl.part");

    // Clean up any stale .part file from previous aborted runs
    let _ = unlink_local_file_with_retry(&part_path).await;

    if cancel_token.is_cancelled() {
        anyhow::bail!("Chat consolidation cancelled by cooperative cancellation token");
    }

    let mut deduplicator = ChatDeduplicator::new(DEFAULT_CHAT_DEDUP_WINDOW_MS, strict);

    let stream_res = async {
        let part_file = tokio::fs::File::create(&part_path).await.with_context(|| {
            format!("Failed to create staged part file: {}", part_path.display())
        })?;
        let mut writer = tokio::io::BufWriter::new(part_file);

        for (i, chunk) in chat_chunks.iter().enumerate() {
            if cancel_token.is_cancelled() {
                anyhow::bail!("Chat consolidation cancelled by cooperative cancellation token");
            }
            let chunk_path = dir.join(&chunk.name);
            let file = tokio::fs::File::open(&chunk_path)
                .await
                .with_context(|| format!("Failed to open chat chunk: {}", chunk_path.display()))?;
            let mut reader = tokio::io::BufReader::new(file);
            let mut line = String::new();

            loop {
                if cancel_token.is_cancelled() {
                    anyhow::bail!("Chat consolidation cancelled by cooperative cancellation token");
                }
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

            if let Some(ref tx) = progress_sender {
                let stats = deduplicator.stats();
                let _ = tx.send(ChatProgressUpdate {
                    chunks_read: i + 1,
                    total_messages: stats.total_messages,
                    deduplicated_messages: stats.deduplicated_messages,
                    emitted_messages: stats.emitted_messages,
                });
            }
        }

        writer.flush().await.with_context(|| {
            format!("Failed to flush staged part file: {}", part_path.display())
        })?;
        drop(writer);

        Ok::<(), anyhow::Error>(())
    };

    let outcome = tokio::select! {
        _ = cancel_token.cancelled() => {
            Err(anyhow::anyhow!("Chat consolidation cancelled by cooperative cancellation token"))
        }
        res = stream_res => res,
    };

    if let Err(err) = outcome {
        cancel_token.cancel();
        // Clean up staged .part file on failure; original chunks remain untouched
        let _ = unlink_local_file_with_retry(&part_path).await;
        return Err(err);
    }

    // Do NOT rename to chat.jsonl here!
    // Staged output is retained in chat.jsonl.part for atomic two-stage finalization.
    Ok(deduplicator.into_stats())
}

/// Consolidates remote chat chunks via local staging into staged `chat.jsonl.part` and
/// atomically finalizes to `chat.jsonl` upon zero-exit completion.
pub async fn consolidate_chat_remote(
    remote_base: &str,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    rclone_bin: Option<&str>,
    cancel_token: CancellationToken,
) -> anyhow::Result<ChatConsolidationStats> {
    consolidate_chat_remote_with_options(
        remote_base,
        chat_chunks,
        strict,
        &ChatConsolidationOptions {
            rclone_bin: rclone_bin.map(str::to_string),
            staging_dir: None,
            progress_sender: None,
        },
        cancel_token,
    )
    .await
}

/// Consolidates remote chat chunks with progress telemetry reporting.
pub async fn consolidate_chat_remote_with_progress(
    remote_base: &str,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    rclone_bin: Option<&str>,
    cancel_token: CancellationToken,
    progress_sender: Option<tokio::sync::mpsc::UnboundedSender<ChatProgressUpdate>>,
) -> anyhow::Result<ChatConsolidationStats> {
    consolidate_chat_remote_with_options(
        remote_base,
        chat_chunks,
        strict,
        &ChatConsolidationOptions {
            rclone_bin: rclone_bin.map(str::to_string),
            staging_dir: None,
            progress_sender,
        },
        cancel_token,
    )
    .await
}

/// Consolidates remote chat chunks into local staging `chat.jsonl.part` and
/// atomically finalizes to `chat.jsonl` upon zero-exit completion.
pub async fn consolidate_chat_remote_with_options(
    remote_base: &str,
    chat_chunks: &[ConsolidationChunk],
    strict: bool,
    options: &ChatConsolidationOptions,
    cancel_token: CancellationToken,
) -> anyhow::Result<ChatConsolidationStats> {
    if chat_chunks.is_empty() {
        return Ok(ChatConsolidationStats::default());
    }

    let bin = resolve_rclone_bin(options.rclone_bin.as_deref());
    let target = TargetLocation::Remote(remote_base.to_string());
    let staging_dir = options
        .staging_dir
        .clone()
        .unwrap_or_else(|| super::staging::resolve_staging_dir(&target, None::<&Path>));

    tokio::fs::create_dir_all(&staging_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to create staging directory '{}'",
                staging_dir.display()
            )
        })?;

    let staged_path = staging_dir.join("chat.jsonl.part");
    let final_path = staging_dir.join("chat.jsonl");

    // Clean up any stale staged file from prior runs
    let _ = unlink_local_file_with_retry(&staged_path).await;

    if cancel_token.is_cancelled() {
        anyhow::bail!("Chat consolidation cancelled by cooperative cancellation token");
    }

    let mut deduplicator = ChatDeduplicator::new(DEFAULT_CHAT_DEDUP_WINDOW_MS, strict);

    let stream_res = async {
        let part_file = tokio::fs::File::create(&staged_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to create staged part file: {}",
                    staged_path.display()
                )
            })?;
        let mut writer = tokio::io::BufWriter::new(part_file);

        for (i, chunk) in chat_chunks.iter().enumerate() {
            if cancel_token.is_cancelled() {
                anyhow::bail!("Chat consolidation cancelled by cooperative cancellation token");
            }
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
                if cancel_token.is_cancelled() {
                    let _ = cat_child.kill().await;
                    anyhow::bail!("Chat consolidation cancelled by cooperative cancellation token");
                }
                line.clear();
                let bytes_read = reader.read_line(&mut line).await.with_context(|| {
                    format!("Failed to read line from rclone cat {chunk_remote_path}")
                })?;
                if bytes_read == 0 {
                    break;
                }
                if let Some(deduped_line) = deduplicator.process_line(&line)? {
                    writer.write_all(deduped_line.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
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

            if let Some(ref tx) = options.progress_sender {
                let stats = deduplicator.stats();
                let _ = tx.send(ChatProgressUpdate {
                    chunks_read: i + 1,
                    total_messages: stats.total_messages,
                    deduplicated_messages: stats.deduplicated_messages,
                    emitted_messages: stats.emitted_messages,
                });
            }
        }

        writer.flush().await.with_context(|| {
            format!(
                "Failed to flush staged part file: {}",
                staged_path.display()
            )
        })?;
        drop(writer);

        // Atomically rename local staged .part to final .jsonl upon zero-exit completion
        let _ = unlink_local_file_with_retry(&final_path).await;
        rename_local_file_with_retry(&staged_path, &final_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to rename staged chat '{}' to '{}'",
                    staged_path.display(),
                    final_path.display()
                )
            })?;

        Ok::<(), anyhow::Error>(())
    };

    let outcome = tokio::select! {
        _ = cancel_token.cancelled() => {
            Err(anyhow::anyhow!("Chat consolidation cancelled by cooperative cancellation token"))
        }
        res = stream_res => res,
    };

    if let Err(err) = outcome {
        cancel_token.cancel();
        let _ = unlink_local_file_with_retry(&staged_path).await;
        return Err(err);
    }

    Ok(deduplicator.into_stats())
}

/// Safely deletes a remote file via `rclone deletefile`, returning `Ok(())` on success.
///
/// Exit code 4 in rclone denotes "directory or object not found", which is
/// treated as success (identical to local `io::ErrorKind::NotFound`).
/// Any other non-zero exit code or spawn failure is returned as an `Err`.
pub async fn delete_remote_file_checked(bin: &str, remote_path: &str) -> anyhow::Result<()> {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.kill_on_drop(true);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.arg("deletefile").arg(remote_path);

    let output = cmd
        .output()
        .await
        .with_context(|| format!("Failed to execute rclone deletefile for '{remote_path}'"))?;

    if !output.status.success() {
        if output.status.code() == Some(4) {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "rclone deletefile for '{}' returned status {}: {}",
            remote_path,
            output.status,
            stderr.trim()
        );
    }
    Ok(())
}

/// Helper function to safely delete a remote file via `rclone deletefile`.
///
/// Exit code 4 in rclone denotes "directory or object not found", which is
/// treated as a silent no-op (identical to local `io::ErrorKind::NotFound`).
/// Any non-fatal deletion failures are logged as warnings.
pub async fn delete_remote_file(bin: &str, remote_path: &str) {
    if let Err(err) = delete_remote_file_checked(bin, remote_path).await {
        eprintln!("[WARN] {err:#}");
    }
}
