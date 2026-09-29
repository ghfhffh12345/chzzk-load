use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::chzzk::models_chat::RecordedChatMessage;

/// In-memory batched writer for live chat messages (`chat_%04d.jsonl`).
///
/// Designed to minimize disk I/O and extend flash memory / microSD longevity
/// on single-board computers (SBCs) by using dual-trigger flushing:
/// - Message count threshold (capacity_threshold)
/// - Maximum buffered byte threshold (64 KB default)
/// - Periodic time interval threshold (`maybe_flush_timer`)
///
/// Supports rotating output files aligned with stream chunk intervals,
/// ensuring only non-empty intervals produce files on disk and sealed chunk paths
/// are emitted for incremental cloud upload.
pub struct ChatWriter {
    session_dir: PathBuf,
    custom_target_path: Option<PathBuf>,
    current_target_path: PathBuf,
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

impl ChatWriter {
    /// Creates a new `ChatWriter` targeting a single fixed `target_path`.
    pub fn new(target_path: PathBuf, flush_interval: Duration, capacity_threshold: usize) -> Self {
        let session_dir = target_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));

        Self {
            session_dir,
            current_target_path: target_path.clone(),
            custom_target_path: Some(target_path),
            chunk_index: 0,
            chunk_duration: Duration::from_secs(365 * 24 * 3600), // effectively infinite
            chunk_start: Instant::now(),
            messages_in_current_chunk: 0,
            buffer: Vec::with_capacity(64 * 1024),
            buffered_count: 0,
            capacity_threshold,
            max_bytes_threshold: 64 * 1024, // 64 KB
            flush_interval,
            last_flush: Instant::now(),
            total_written: 0,
            dir_created: false,
        }
    }

    /// Creates a rotating `ChatWriter` targeting `session_dir` with time-based chunks `chat_%04d.jsonl`.
    pub fn new_rotating(
        session_dir: PathBuf,
        chunk_duration: Duration,
        flush_interval: Duration,
        capacity_threshold: usize,
    ) -> Self {
        let initial_target = session_dir.join("chat_0000.jsonl");
        Self {
            session_dir,
            custom_target_path: None,
            current_target_path: initial_target,
            chunk_index: 0,
            chunk_duration,
            chunk_start: Instant::now(),
            messages_in_current_chunk: 0,
            buffer: Vec::with_capacity(64 * 1024),
            buffered_count: 0,
            capacity_threshold,
            max_bytes_threshold: 64 * 1024, // 64 KB
            flush_interval,
            last_flush: Instant::now(),
            total_written: 0,
            dir_created: false,
        }
    }

    /// Customizes the maximum in-memory byte threshold before triggering a flush.
    #[must_use]
    pub fn with_max_bytes_threshold(mut self, max_bytes: usize) -> Self {
        self.max_bytes_threshold = max_bytes;
        self
    }

    /// Returns a reference to the active destination path.
    pub fn target_path(&self) -> &Path {
        &self.current_target_path
    }

    /// Returns the current chunk index.
    pub fn current_chunk_index(&self) -> usize {
        self.chunk_index
    }

    /// Returns the file path for the current chunk.
    pub fn current_chunk_path(&self) -> PathBuf {
        self.current_target_path.clone()
    }

    /// Returns the total number of chat messages written to disk so far.
    pub fn total_written(&self) -> u64 {
        self.total_written
    }

    /// Returns the number of currently buffered messages awaiting flush.
    pub fn buffered_count(&self) -> usize {
        self.buffered_count
    }

    /// Returns the number of bytes currently buffered awaiting flush.
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Appends a recorded chat message to the in-memory buffer.
    ///
    /// If the buffer reaches `capacity_threshold` or `max_bytes_threshold`,
    /// an asynchronous flush to disk is triggered automatically.
    pub async fn push(&mut self, msg: RecordedChatMessage) -> Result<()> {
        serde_json::to_writer(&mut self.buffer, &msg)
            .context("Failed to serialize RecordedChatMessage")?;
        self.buffer.push(b'\n');
        self.buffered_count += 1;
        self.messages_in_current_chunk += 1;

        if self.buffered_count >= self.capacity_threshold
            || self.buffer.len() >= self.max_bytes_threshold
        {
            self.flush().await?;
        }
        Ok(())
    }

    /// Checks if the periodic flush interval has elapsed.
    ///
    /// If buffered messages exist and `flush_interval` has passed since the last flush,
    /// flushes to disk and returns `Ok(true)`. Otherwise returns `Ok(false)`.
    pub async fn maybe_flush_timer(&mut self) -> Result<bool> {
        if self.buffered_count > 0 && self.last_flush.elapsed() >= self.flush_interval {
            self.flush().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Checks if the current chunk duration interval has elapsed.
    ///
    /// If elapsed:
    /// - Flushes any buffered messages to disk.
    /// - If messages were written during this chunk, returns `Some(sealed_path)`.
    /// - If 0 messages were received during this interval, skips file creation and returns `None`.
    /// - Increments `chunk_index`, resets the timer, and updates `current_target_path`.
    pub async fn maybe_rotate(&mut self) -> Result<Option<PathBuf>> {
        if self.custom_target_path.is_some() || self.chunk_start.elapsed() < self.chunk_duration {
            return Ok(None);
        }

        self.flush().await?;

        let sealed = if self.messages_in_current_chunk > 0 {
            Some(self.current_target_path.clone())
        } else {
            None
        };

        self.chunk_index += 1;
        self.messages_in_current_chunk = 0;
        self.chunk_start = Instant::now();
        self.current_target_path = self
            .session_dir
            .join(format!("chat_{:04}.jsonl", self.chunk_index));

        Ok(sealed)
    }

    /// Flushes all buffered messages to the target file on disk.
    ///
    /// Appends each serialized message followed by a newline (`\n`).
    /// Ensures parent directories exist. Resets the flush timer and buffer.
    pub async fn flush(&mut self) -> Result<()> {
        if self.buffered_count == 0 {
            self.last_flush = Instant::now();
            return Ok(());
        }

        if !self.dir_created {
            if let Some(parent) = self
                .current_target_path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
            {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("Failed to create directory {}", parent.display()))?;
            }
            self.dir_created = true;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.current_target_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to open chat file {}",
                    self.current_target_path.display()
                )
            })?;

        file.write_all(&self.buffer)
            .await
            .context("Failed to write chat batch to disk")?;
        file.flush().await.context("Failed to flush chat file")?;

        self.total_written += self.buffered_count as u64;
        self.buffer.clear();
        self.buffered_count = 0;
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Flushes any lingering buffered messages and closes the writer,
    /// returning the total number of messages written and the final chunk's path (if non-empty).
    pub async fn flush_and_close(&mut self) -> Result<(u64, Option<PathBuf>)> {
        self.flush().await?;
        let sealed = if self.messages_in_current_chunk > 0 {
            Some(self.current_target_path.clone())
        } else {
            None
        };
        Ok((self.total_written, sealed))
    }
}
