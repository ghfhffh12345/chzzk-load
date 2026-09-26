use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::chzzk::models_chat::RecordedChatMessage;

/// In-memory batched writer for live chat messages (`chat.jsonl`).
///
/// Designed to minimize disk I/O and extend flash memory / microSD longevity
/// on single-board computers (SBCs) by using dual-trigger flushing:
/// - Message count threshold (capacity_threshold)
/// - Maximum buffered byte threshold (64 KB default)
/// - Periodic time interval threshold (`maybe_flush_timer`)
pub struct ChatWriter {
    target_path: PathBuf,
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
    /// Creates a new `ChatWriter` targeting `target_path`.
    pub fn new(target_path: PathBuf, flush_interval: Duration, capacity_threshold: usize) -> Self {
        Self {
            target_path,
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

    /// Returns a reference to the destination path.
    pub fn target_path(&self) -> &Path {
        &self.target_path
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
                .target_path
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
            .open(&self.target_path)
            .await
            .with_context(|| format!("Failed to open chat file {}", self.target_path.display()))?;

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
    /// returning the total number of messages written to disk across the entire session.
    pub async fn flush_and_close(&mut self) -> Result<u64> {
        self.flush().await?;
        Ok(self.total_written)
    }
}
