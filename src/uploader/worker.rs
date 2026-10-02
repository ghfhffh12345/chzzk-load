use futures_util::FutureExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::tui::event::{AppEvent, LogEntry};
use crate::uploader::backend::{ProgressCallback, UploadBackend};
use crate::uploader::{UploadTask, broadcast_identifier};

/// Provider function type for checking free disk space in GB for a given path.
pub type DiskSpaceProvider = Arc<dyn Fn(&Path) -> f64 + Send + Sync>;

fn default_disk_space_provider() -> DiskSpaceProvider {
    Arc::new(|path: &Path| {
        crate::disk::get_disk_space(path)
            .map(|s| s.available_gb())
            .unwrap_or(f64::MAX)
    })
}

#[derive(Clone)]
pub struct DlqConfig {
    pub initial_delay: Duration,
    pub max_backoff: Duration,
    pub max_tasks_per_channel: usize,
    pub circuit_breaker_failures: usize,
    pub circuit_breaker_cooldown: Duration,
    pub min_free_disk_gb: f64,
    pub disk_space_provider: DiskSpaceProvider,
}

impl std::fmt::Debug for DlqConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DlqConfig")
            .field("initial_delay", &self.initial_delay)
            .field("max_backoff", &self.max_backoff)
            .field("max_tasks_per_channel", &self.max_tasks_per_channel)
            .field("circuit_breaker_failures", &self.circuit_breaker_failures)
            .field("circuit_breaker_cooldown", &self.circuit_breaker_cooldown)
            .field("min_free_disk_gb", &self.min_free_disk_gb)
            .finish()
    }
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(2),
            max_backoff: Duration::from_secs(300),
            max_tasks_per_channel: 20,
            circuit_breaker_failures: 5,
            circuit_breaker_cooldown: Duration::from_secs(30),
            min_free_disk_gb: 2.0,
            disk_space_provider: default_disk_space_provider(),
        }
    }
}

impl DlqConfig {
    /// Computes the exponential backoff delay for a given retry attempt,
    /// capped at `max_backoff`.
    pub fn backoff_delay(&self, retry_attempt: usize) -> Duration {
        if retry_attempt == 0 {
            return self.initial_delay.min(self.max_backoff);
        }
        let multiplier = 1u32
            .checked_shl(retry_attempt.min(30) as u32)
            .unwrap_or(u32::MAX);
        self.initial_delay
            .saturating_mul(multiplier)
            .min(self.max_backoff)
    }

    /// Builder method to inject a custom disk space provider closure.
    pub fn with_disk_space_provider<F>(mut self, provider: F) -> Self
    where
        F: Fn(&Path) -> f64 + Send + Sync + 'static,
    {
        self.disk_space_provider = Arc::new(provider);
        self
    }

    /// Builder method to inject a fixed free disk space value for testing.
    pub fn with_free_disk_gb(mut self, free_gb: f64) -> Self {
        self.disk_space_provider = Arc::new(move |_| free_gb);
        self
    }
}

#[derive(Debug, Clone)]
enum TaskKind {
    Primary,
    Dlq { retry_attempt: usize },
}

#[derive(Debug)]
struct DlqTask {
    task: UploadTask,
    retry_attempt: usize,
    next_retry_at: tokio::time::Instant,
}

#[derive(Debug)]
struct WorkerTaskOutcome {
    channel_id: String,
    task: UploadTask,
    kind: TaskKind,
    result: Result<u64, String>,
}

/// Represents paired physical video `.ts` chunk and chat `.jsonl` chunk paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairedChunkPaths {
    pub ts_path: PathBuf,
    pub jsonl_path: PathBuf,
}

/// Resolves paired physical video `.ts` chunk and chat `.jsonl` chunk paths.
pub fn resolve_paired_paths(chunk_path: &Path) -> PairedChunkPaths {
    let parent = chunk_path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = chunk_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    if let Some(base) = file_name.strip_suffix(".ts") {
        let ts_path = chunk_path.to_path_buf();
        let jsonl_name = if let Some(suffix) = base.strip_prefix("chunk_") {
            format!("chat_{suffix}.jsonl")
        } else {
            format!("{base}.jsonl")
        };
        PairedChunkPaths {
            ts_path,
            jsonl_path: parent.join(jsonl_name),
        }
    } else if let Some(base) = file_name.strip_suffix(".jsonl") {
        let jsonl_path = chunk_path.to_path_buf();
        let ts_name = if let Some(suffix) = base.strip_prefix("chat_") {
            format!("chunk_{suffix}.ts")
        } else {
            format!("{base}.ts")
        };
        PairedChunkPaths {
            ts_path: parent.join(ts_name),
            jsonl_path,
        }
    } else {
        PairedChunkPaths {
            ts_path: chunk_path.with_extension("ts"),
            jsonl_path: chunk_path.with_extension("jsonl"),
        }
    }
}

/// Parses the numeric chunk index from either a `chunk_XXXX.ts` or `chat_XXXX.jsonl` filename.
pub fn parse_task_chunk_index(file_name: &str) -> Option<u64> {
    let path = Path::new(file_name);
    let stem = path.file_stem()?.to_str()?;
    let index_str = stem
        .strip_prefix("chunk_")
        .or_else(|| stem.strip_prefix("chat_"))?;
    index_str.parse().ok()
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TaskAgeKey<'a> {
    file_time: std::time::SystemTime,
    session_id: &'a str,
    chunk_index: u64,
    channel_id: &'a str,
}

/// Enforces the global disk-aware eviction strategy under disk pressure.
/// If free disk space drops below `min_free_disk_gb`, it identifies the globally oldest
/// chunk across the DLQ, permanently deletes both the `.ts` and `.jsonl` files from disk,
/// and purges the corresponding envelopes from both DLQ and primary queues.
async fn enforce_dlq_disk_eviction(
    dlq_queues: &mut HashMap<String, VecDeque<DlqTask>>,
    channel_queues: &mut HashMap<String, VecDeque<UploadTask>>,
    dlq_config: &DlqConfig,
    event_tx: &Sender<AppEvent>,
) {
    if dlq_config.min_free_disk_gb <= 0.0 || dlq_queues.is_empty() {
        return;
    }

    let Some(sample_path) = dlq_queues
        .values()
        .flat_map(|q| q.front())
        .map(|t| &t.task.chunk_path)
        .next()
    else {
        return;
    };

    let free_space = (dlq_config.disk_space_provider)(sample_path);
    if free_space >= dlq_config.min_free_disk_gb {
        return;
    }

    let mut oldest: Option<(TaskAgeKey<'_>, UploadTask)> = None;

    for (channel_id, queue) in dlq_queues.iter() {
        for dlq_task in queue {
            let file_time = std::fs::metadata(&dlq_task.task.chunk_path)
                .and_then(|m| m.modified().or_else(|_| m.created()))
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let chunk_index = parse_task_chunk_index(&dlq_task.task.chunk_name).unwrap_or(u64::MAX);
            let key = TaskAgeKey {
                file_time,
                session_id: &dlq_task.task.session_folder_id,
                chunk_index,
                channel_id,
            };

            if oldest.as_ref().is_none_or(|(best_key, _)| &key < best_key) {
                oldest = Some((key, dlq_task.task.clone()));
            }
        }
    }

    let Some((_, oldest_task)) = oldest else {
        return;
    };

    let paired = resolve_paired_paths(&oldest_task.chunk_path);

    if paired.ts_path.exists() {
        let _ = tokio::fs::remove_file(&paired.ts_path).await;
    }
    if paired.jsonl_path.exists() {
        let _ = tokio::fs::remove_file(&paired.jsonl_path).await;
    }

    // Prune envelopes from DLQ
    let mut channels_to_remove = Vec::new();
    for (cid, queue) in dlq_queues.iter_mut() {
        queue.retain(|item| {
            item.task.chunk_path != paired.ts_path && item.task.chunk_path != paired.jsonl_path
        });
        if queue.is_empty() {
            channels_to_remove.push(cid.clone());
        }
    }
    for cid in channels_to_remove {
        dlq_queues.remove(&cid);
    }

    // Also prune envelopes from primary queues to avoid dispatching deleted files
    for queue in channel_queues.values_mut() {
        queue.retain(|task| {
            task.chunk_path != paired.ts_path && task.chunk_path != paired.jsonl_path
        });
    }

    let ts_name = paired
        .ts_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&oldest_task.chunk_name);
    let _ = event_tx.try_send(AppEvent::Log(LogEntry::warn(format!(
        "[DLQ] Disk space critically low ({:.2} GB < {:.2} GB). Evicted oldest chunk {} and coupled chat log.",
        free_space, dlq_config.min_free_disk_gb, ts_name
    ))));
}

/// Background upload worker managing per-channel FIFO serialization,
/// non-blocking Dead-Letter Queue (DLQ) retry backoff, and fair cross-channel concurrency.
pub struct UploadWorker;

impl UploadWorker {
    /// Spawns an upload worker task with the default concurrency level (3) and default DLQ config.
    pub fn spawn(
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        upload_rx: Receiver<UploadTask>,
    ) -> tokio::task::JoinHandle<()> {
        Self::spawn_with_concurrency(backend_opt, event_tx, upload_rx, 3)
    }

    /// Spawns an upload worker task with a configurable channel concurrency limit.
    pub fn spawn_with_concurrency(
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        upload_rx: Receiver<UploadTask>,
        concurrency: usize,
    ) -> tokio::task::JoinHandle<()> {
        Self::spawn_with_options(
            backend_opt,
            event_tx,
            upload_rx,
            concurrency,
            DlqConfig::default(),
        )
    }

    /// Spawns an upload worker task with full options including concurrency and DLQ config.
    pub fn spawn_with_options(
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        mut upload_rx: Receiver<UploadTask>,
        concurrency: usize,
        dlq_config: DlqConfig,
    ) -> tokio::task::JoinHandle<()> {
        let concurrency = concurrency.max(1);
        tokio::spawn(async move {
            let mut active_channels: HashSet<String> = HashSet::new();
            let mut channel_queues: HashMap<String, VecDeque<UploadTask>> = HashMap::new();
            let mut ready_channels: VecDeque<String> = VecDeque::new();
            let mut dlq_queues: HashMap<String, VecDeque<DlqTask>> = HashMap::new();
            let mut join_set: tokio::task::JoinSet<WorkerTaskOutcome> = tokio::task::JoinSet::new();
            let mut rx_closed = false;
            let mut consecutive_failures: usize = 0;
            let mut circuit_breaker_until: Option<tokio::time::Instant> = None;

            let handle_outcome = |outcome: WorkerTaskOutcome,
                                  active_channels: &mut HashSet<String>,
                                  channel_queues: &mut HashMap<String, VecDeque<UploadTask>>,
                                  ready_channels: &mut VecDeque<String>,
                                  dlq_queues: &mut HashMap<String, VecDeque<DlqTask>>,
                                  consecutive_failures: &mut usize,
                                  circuit_breaker_until: &mut Option<tokio::time::Instant>,
                                  event_tx: &Sender<AppEvent>,
                                  dlq_config: &DlqConfig| {
                active_channels.remove(&outcome.channel_id);

                match outcome.result {
                    Ok(_) => {
                        *consecutive_failures = 0;
                        if let TaskKind::Dlq { retry_attempt } = outcome.kind {
                            let _ = event_tx.try_send(AppEvent::Log(LogEntry::clean(format!(
                                "[DLQ] Successfully uploaded {} on retry {}",
                                outcome.task.chunk_name, retry_attempt
                            ))));
                        }
                    }
                    Err(_) => {
                        *consecutive_failures += 1;
                        if *consecutive_failures >= dlq_config.circuit_breaker_failures {
                            *circuit_breaker_until = Some(
                                tokio::time::Instant::now() + dlq_config.circuit_breaker_cooldown,
                            );
                            let _ = event_tx.try_send(AppEvent::Log(LogEntry::warn(format!(
                                "[CIRCUIT BREAKER] {} consecutive upload failures detected across channels. Pausing uploads for {}s to check remote health.",
                                dlq_config.circuit_breaker_failures,
                                dlq_config.circuit_breaker_cooldown.as_secs()
                            ))));
                        }

                        let (next_attempt, delay, log_msg) = match outcome.kind {
                            TaskKind::Primary => {
                                let delay = dlq_config.backoff_delay(0);
                                (
                                    1,
                                    delay,
                                    format!(
                                        "[DLQ] Transferred {} to DLQ (retry 1 in {}s)",
                                        outcome.task.chunk_name,
                                        delay.as_secs()
                                    ),
                                )
                            }
                            TaskKind::Dlq { retry_attempt } => {
                                let next_attempt = retry_attempt.saturating_add(1);
                                let delay = dlq_config.backoff_delay(retry_attempt);
                                (
                                    next_attempt,
                                    delay,
                                    format!(
                                        "[DLQ] Retry {} failed for {}. Scheduled retry {} in {}s",
                                        retry_attempt,
                                        outcome.task.chunk_name,
                                        next_attempt,
                                        delay.as_secs()
                                    ),
                                )
                            }
                        };

                        let dlq = dlq_queues.entry(outcome.channel_id.clone()).or_default();
                        if dlq.len() >= dlq_config.max_tasks_per_channel {
                            if let Some(old) = dlq.pop_front() {
                                let _ = event_tx.try_send(AppEvent::Log(LogEntry::warn(format!(
                                    "[DLQ] DLQ capacity reached ({}) for channel {}. Evicting oldest task {}.",
                                    dlq_config.max_tasks_per_channel,
                                    outcome.channel_id,
                                    old.task.chunk_name
                                ))));
                            }
                        }
                        dlq.push_back(DlqTask {
                            task: outcome.task.clone(),
                            retry_attempt: next_attempt,
                            next_retry_at: tokio::time::Instant::now() + delay,
                        });
                        let _ = event_tx.try_send(AppEvent::Log(LogEntry::warn(log_msg)));
                    }
                }

                // Advance primary channel queue
                if let Some(queue) = channel_queues.get_mut(&outcome.channel_id) {
                    if queue.is_empty() {
                        channel_queues.remove(&outcome.channel_id);
                    } else if !ready_channels.contains(&outcome.channel_id) {
                        ready_channels.push_back(outcome.channel_id.clone());
                    }
                }
            };

            let spawn_upload_task =
                |task: UploadTask,
                 kind: TaskKind,
                 backend_opt: Option<Arc<dyn UploadBackend>>,
                 event_tx: Sender<AppEvent>,
                 join_set: &mut tokio::task::JoinSet<WorkerTaskOutcome>| {
                    let task_clone = task.clone();
                    let kind_clone = kind.clone();
                    let tx_panic = event_tx.clone();
                    let cid_panic = task.channel_id.clone();
                    let name_panic = task.chunk_name.clone();
                    let streamer_panic = task.streamer_name.clone();

                    join_set.spawn(async move {
                    let unwind_res = std::panic::AssertUnwindSafe(async {
                        if let Some(ref backend) = backend_opt {
                            let tx = event_tx.clone();
                            let n = task.chunk_name.clone();
                            let c = task.channel_id.clone();
                            let s = task.streamer_name.clone();

                            let progress_cb: ProgressCallback =
                                Box::new(move |uploaded, total, speed| {
                                    let _ = tx.try_send(AppEvent::UploadProgress {
                                        channel_id: c.clone(),
                                        chunk_name: n.clone(),
                                        streamer_name: s.clone(),
                                        uploaded_bytes: uploaded,
                                        total_bytes: total,
                                        speed_mb_s: speed,
                                    });
                                });

                            let upload_res = backend
                                .upload_file_and_delete(
                                    &task.chunk_path,
                                    &task.remote_dir,
                                    progress_cb,
                                )
                                .await;

                            let target = broadcast_identifier(&task.streamer_name, &task.channel_id);
                            match upload_res {
                                Ok(reclaimed) => {
                                    let _ = event_tx
                                        .send(AppEvent::UploadCompleted {
                                            channel_id: task.channel_id.clone(),
                                            chunk_name: task.chunk_name.clone(),
                                            reclaimed_bytes: reclaimed,
                                        })
                                        .await;
                                    let _ = event_tx
                                        .send(AppEvent::Log(LogEntry::clean(format!(
                                            "[{target}] Uploaded & deleted {} (reclaimed {:.1} MB)",
                                            task.chunk_name,
                                            reclaimed as f64 / 1_048_576.0
                                        ))))
                                        .await;

                                    if let Some(parent) = task.chunk_path.parent() {
                                        let is_session_dir = parent
                                            .file_name()
                                            .and_then(|n| n.to_str())
                                            .map(|n| {
                                                n == task.remote_dir
                                                    || n == task.session_folder_id
                                                    || n.starts_with(&format!(
                                                        "{}_",
                                                        task.channel_id
                                                    ))
                                            })
                                            .unwrap_or(false);

                                        if is_session_dir
                                            && let Ok(true) =
                                                crate::engine::cleanup::cleanup_session_dir_if_empty(
                                                    parent,
                                                )
                                                .await
                                        {
                                            let _ = event_tx
                                                .send(AppEvent::Log(LogEntry::clean(format!(
                                                    "[{target}] Cleaned up empty session folder '{}'",
                                                    parent.display()
                                                ))))
                                                .await;
                                        }
                                    }

                                    Ok(reclaimed)
                                }
                                Err(e) => {
                                    let err_msg = e.to_string();
                                    let _ = event_tx
                                        .send(AppEvent::UploadFailed {
                                            channel_id: task.channel_id.clone(),
                                            chunk_name: task.chunk_name.clone(),
                                        })
                                        .await;
                                    let _ = event_tx
                                        .send(AppEvent::Log(LogEntry::error(format!(
                                            "[{target}] Upload failed for {}: {err_msg}",
                                            task.chunk_name
                                        ))))
                                        .await;
                                    Err(err_msg)
                                }
                            }
                        } else {
                            Ok(0)
                        }
                    })
                    .catch_unwind()
                    .await;

                    let res = match unwind_res {
                        Ok(r) => r,
                        Err(_) => {
                            let target = broadcast_identifier(&streamer_panic, &cid_panic);
                            let _ = tx_panic
                                .send(AppEvent::UploadFailed {
                                    channel_id: cid_panic.clone(),
                                    chunk_name: name_panic.clone(),
                                })
                                .await;
                            let _ = tx_panic
                                .send(AppEvent::Log(LogEntry::error(format!(
                                    "[{target}] Upload task panicked unexpectedly during {name_panic}"
                                ))))
                                .await;
                            Err("Panicked unexpectedly".to_string())
                        }
                    };

                    WorkerTaskOutcome {
                        channel_id: cid_panic,
                        task: task_clone,
                        kind: kind_clone,
                        result: res,
                    }
                });
                };

            loop {
                // Reap completed tasks
                while let Some(res) = join_set.try_join_next() {
                    match res {
                        Ok(outcome) => {
                            handle_outcome(
                                outcome,
                                &mut active_channels,
                                &mut channel_queues,
                                &mut ready_channels,
                                &mut dlq_queues,
                                &mut consecutive_failures,
                                &mut circuit_breaker_until,
                                &event_tx,
                                &dlq_config,
                            );
                        }
                        Err(e) => {
                            let _ = event_tx.try_send(AppEvent::Log(LogEntry::error(format!(
                                "Upload worker task join error: {e}"
                            ))));
                        }
                    }
                }

                // Check circuit breaker probe
                if let Some(until) = circuit_breaker_until {
                    if tokio::time::Instant::now() >= until {
                        if let Some(ref backend) = backend_opt {
                            match backend.check_connection().await {
                                Ok(_) => {
                                    let _ = event_tx
                                        .send(AppEvent::Log(LogEntry::info(
                                            "[CIRCUIT BREAKER] Remote health check succeeded. Resuming upload queue.",
                                        )))
                                        .await;
                                    consecutive_failures = 0;
                                    circuit_breaker_until = None;
                                }
                                Err(e) => {
                                    let _ = event_tx
                                        .send(AppEvent::Log(LogEntry::warn(format!(
                                            "[CIRCUIT BREAKER] Remote health check failed: {e}. Retrying in {}s.",
                                            dlq_config.circuit_breaker_cooldown.as_secs()
                                        ))))
                                        .await;
                                    circuit_breaker_until = Some(
                                        tokio::time::Instant::now()
                                            + dlq_config.circuit_breaker_cooldown,
                                    );
                                }
                            }
                        } else {
                            circuit_breaker_until = None;
                            consecutive_failures = 0;
                        }
                    }
                }

                // Enforce DLQ disk-aware eviction strategy under disk pressure
                if !dlq_queues.is_empty() {
                    enforce_dlq_disk_eviction(
                        &mut dlq_queues,
                        &mut channel_queues,
                        &dlq_config,
                        &event_tx,
                    )
                    .await;
                }

                // Dispatch tasks while concurrency allows and circuit breaker is inactive
                if circuit_breaker_until.is_none() {
                    // 1. Dispatch primary tasks (highest priority)
                    let mut i = 0;
                    while join_set.len() < concurrency && i < ready_channels.len() {
                        let ch_id = ready_channels[i].clone();
                        if !active_channels.contains(&ch_id) {
                            ready_channels.remove(i);
                            let task_opt = if let Some(queue) = channel_queues.get_mut(&ch_id) {
                                let task = queue.pop_front();
                                if queue.is_empty() {
                                    channel_queues.remove(&ch_id);
                                }
                                task
                            } else {
                                None
                            };

                            if let Some(task) = task_opt {
                                active_channels.insert(ch_id);
                                spawn_upload_task(
                                    task,
                                    TaskKind::Primary,
                                    backend_opt.clone(),
                                    event_tx.clone(),
                                    &mut join_set,
                                );
                            }
                        } else {
                            i += 1;
                        }
                    }

                    // 2. Dispatch DLQ tasks (opportunistic background priority)
                    if join_set.len() < concurrency {
                        let now = tokio::time::Instant::now();
                        let mut eligible_ch = None;
                        for (cid, q) in &dlq_queues {
                            if !active_channels.contains(cid) {
                                if let Some(front) = q.front() {
                                    if front.next_retry_at <= now {
                                        eligible_ch = Some(cid.clone());
                                        break;
                                    }
                                }
                            }
                        }

                        if let Some(cid) = eligible_ch {
                            if let Some(q) = dlq_queues.get_mut(&cid) {
                                if let Some(dlq_task) = q.pop_front() {
                                    active_channels.insert(cid);
                                    spawn_upload_task(
                                        dlq_task.task,
                                        TaskKind::Dlq {
                                            retry_attempt: dlq_task.retry_attempt,
                                        },
                                        backend_opt.clone(),
                                        event_tx.clone(),
                                        &mut join_set,
                                    );
                                }
                            }
                        }
                    }
                }

                // Exit condition: receiver closed, join_set empty, no pending primary or DLQ tasks
                if rx_closed
                    && join_set.is_empty()
                    && channel_queues.is_empty()
                    && dlq_queues.values().all(|q| q.is_empty())
                {
                    break;
                }

                // Determine next wake-up deadline
                let mut next_deadline = circuit_breaker_until;
                for (cid, q) in &dlq_queues {
                    if !active_channels.contains(cid) {
                        if let Some(front) = q.front() {
                            if next_deadline.is_none_or(|d| front.next_retry_at < d) {
                                next_deadline = Some(front.next_retry_at);
                            }
                        }
                    }
                }

                let sleep_fut = if let Some(deadline) = next_deadline {
                    tokio::time::sleep_until(deadline).boxed()
                } else {
                    futures_util::future::pending().boxed()
                };

                tokio::select! {
                    task_opt = upload_rx.recv(), if !rx_closed => {
                        match task_opt {
                            Some(task) => {
                                let cid = task.channel_id.clone();
                                let queue = channel_queues.entry(cid.clone()).or_default();
                                queue.push_back(task);
                                if !active_channels.contains(&cid) && !ready_channels.contains(&cid) {
                                    ready_channels.push_back(cid);
                                }
                            }
                            None => {
                                rx_closed = true;
                            }
                        }
                    }
                    Some(res) = join_set.join_next(), if !join_set.is_empty() => {
                        match res {
                            Ok(outcome) => {
                                handle_outcome(
                                    outcome,
                                    &mut active_channels,
                                    &mut channel_queues,
                                    &mut ready_channels,
                                    &mut dlq_queues,
                                    &mut consecutive_failures,
                                    &mut circuit_breaker_until,
                                    &event_tx,
                                    &dlq_config,
                                );
                            }
                            Err(e) => {
                                let _ = event_tx.try_send(AppEvent::Log(LogEntry::error(format!(
                                    "Upload worker task join error: {e}"
                                ))));
                            }
                        }
                    }
                    _ = sleep_fut => {
                        // Timer expired: loop re-runs to probe breaker or dispatch ready DLQ tasks
                    }
                    else => {
                        break;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Default)]
    struct MockUploadBackend {
        uploads: Mutex<Vec<(std::path::PathBuf, String)>>,
        should_fail: AtomicBool,
    }

    impl MockUploadBackend {
        fn new() -> Self {
            Self::default()
        }
    }

    impl UploadBackend for MockUploadBackend {
        fn upload_file_and_delete<'a>(
            &'a self,
            local_path: &'a Path,
            remote_dir: &'a str,
            on_progress: ProgressCallback,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<u64>> {
            Box::pin(async move {
                if self.should_fail.load(Ordering::SeqCst) {
                    anyhow::bail!("Simulated upload failure");
                }
                let bytes = if local_path.exists() {
                    let len = std::fs::metadata(local_path).map(|m| m.len()).unwrap_or(0);
                    on_progress(len, len, 10.0);
                    let _ = std::fs::remove_file(local_path);
                    len
                } else {
                    1024
                };
                self.uploads
                    .lock()
                    .unwrap()
                    .push((local_path.to_path_buf(), remote_dir.to_string()));
                Ok(bytes)
            })
        }

        fn upload_text<'a>(
            &'a self,
            _remote_dir: &'a str,
            _file_name: &'a str,
            _content: &'a str,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn check_connection<'a>(
            &'a self,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    #[tokio::test]
    async fn test_upload_worker_processes_and_deletes_chunk() {
        let mock_backend = Arc::new(MockUploadBackend::new());
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

        let temp_dir =
            std::env::temp_dir().join(format!("chzzk_worker_test_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let chunk1_path = temp_dir.join("chunk_0000.ts");
        let mut file = File::create(&chunk1_path).unwrap();
        file.write_all(b"dummy video data").unwrap();
        drop(file);

        let worker_handle = UploadWorker::spawn(Some(mock_backend.clone()), event_tx, upload_rx);

        upload_tx
            .send(UploadTask {
                channel_id: "ch1".to_string(),
                session_folder_id: "session1".to_string(),
                remote_dir: "session1".to_string(),
                chunk_path: chunk1_path.clone(),
                chunk_name: "chunk_0000.ts".to_string(),
                streamer_name: "Streamer1".to_string(),
            })
            .await
            .unwrap();

        drop(upload_tx);
        worker_handle.await.unwrap();

        {
            let uploads = mock_backend.uploads.lock().unwrap();
            assert_eq!(uploads.len(), 1);
            assert_eq!(uploads[0].0, chunk1_path);
            assert_eq!(uploads[0].1, "session1");
        }
        assert!(!chunk1_path.exists());

        let mut saw_progress = false;
        let mut saw_completed = false;
        while let Ok(ev) = event_rx.try_recv() {
            match ev {
                AppEvent::UploadProgress { chunk_name, .. } if chunk_name == "chunk_0000.ts" => {
                    saw_progress = true;
                }
                AppEvent::UploadCompleted { chunk_name, .. } if chunk_name == "chunk_0000.ts" => {
                    saw_completed = true;
                }
                _ => {}
            }
        }
        assert!(saw_progress);
        assert!(saw_completed);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_upload_worker_without_backend_discards_gracefully() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

        let worker_handle = UploadWorker::spawn(None, event_tx, upload_rx);

        upload_tx
            .send(UploadTask {
                channel_id: "ch1".to_string(),
                session_folder_id: "session1".to_string(),
                remote_dir: "session1".to_string(),
                chunk_path: std::path::PathBuf::from("nonexistent.ts"),
                chunk_name: "nonexistent.ts".to_string(),
                streamer_name: "Streamer1".to_string(),
            })
            .await
            .unwrap();

        drop(upload_tx);
        worker_handle.await.unwrap();

        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_upload_worker_cleans_empty_session_dir_with_metadata_jsonl() {
        let mock_backend = Arc::new(MockUploadBackend::new());
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

        let temp_dir =
            std::env::temp_dir().join(format!("chzzk_worker_clean_{}", rand::random::<u32>()));
        let session_dir = temp_dir.join("session_clean_test");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();

        let chunk_path = session_dir.join("chunk_0000.ts");
        tokio::fs::write(&chunk_path, b"chunk data").await.unwrap();

        let meta_path = session_dir.join("metadata.jsonl");
        tokio::fs::write(&meta_path, b"{\"event\":\"INITIAL_STATE\"}\n")
            .await
            .unwrap();

        let worker_handle = UploadWorker::spawn(Some(mock_backend.clone()), event_tx, upload_rx);

        upload_tx
            .send(UploadTask {
                channel_id: "ch_clean".to_string(),
                session_folder_id: "session_clean_test".to_string(),
                remote_dir: "session_clean_test".to_string(),
                chunk_path: chunk_path.clone(),
                chunk_name: "chunk_0000.ts".to_string(),
                streamer_name: "StreamerClean".to_string(),
            })
            .await
            .unwrap();

        drop(upload_tx);
        worker_handle.await.unwrap();

        assert!(!chunk_path.exists());
        assert!(!meta_path.exists(), "metadata.jsonl should be removed");
        assert!(!session_dir.exists(), "session_dir should be removed");

        let mut saw_clean_log = false;
        while let Ok(ev) = event_rx.try_recv() {
            if let AppEvent::Log(entry) = ev
                && entry.message.contains("Cleaned up empty session folder")
            {
                saw_clean_log = true;
            }
        }
        assert!(saw_clean_log);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    struct PanickingBackend;
    impl UploadBackend for PanickingBackend {
        fn upload_file_and_delete<'a>(
            &'a self,
            _local_path: &'a Path,
            _remote_dir: &'a str,
            _on_progress: ProgressCallback,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<u64>> {
            Box::pin(async move {
                panic!("Simulated critical backend failure!");
            })
        }

        fn upload_text<'a>(
            &'a self,
            _remote_dir: &'a str,
            _file_name: &'a str,
            _content: &'a str,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn check_connection<'a>(
            &'a self,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    #[tokio::test]
    async fn test_upload_worker_reports_panic_in_backend() {
        let panicking_backend = Arc::new(PanickingBackend);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

        let fast_dlq = DlqConfig {
            initial_delay: Duration::from_millis(5),
            ..Default::default()
        };

        let worker_handle = UploadWorker::spawn_with_options(
            Some(panicking_backend),
            event_tx,
            upload_rx,
            1,
            fast_dlq,
        );

        upload_tx
            .send(UploadTask {
                channel_id: "ch_panic".to_string(),
                session_folder_id: "session_panic".to_string(),
                remote_dir: "session_panic".to_string(),
                chunk_path: std::path::PathBuf::from("chunk_panic.ts"),
                chunk_name: "chunk_panic.ts".to_string(),
                streamer_name: "StreamerPanic".to_string(),
            })
            .await
            .unwrap();

        drop(upload_tx);

        let mut saw_failed = false;
        let mut saw_panic_log = false;
        while let Some(ev) = event_rx.recv().await {
            match ev {
                AppEvent::UploadFailed { chunk_name, .. } if chunk_name == "chunk_panic.ts" => {
                    saw_failed = true;
                }
                AppEvent::Log(entry) if entry.message.contains("panicked unexpectedly") => {
                    saw_panic_log = true;
                }
                _ => {}
            }
            if saw_failed && saw_panic_log {
                break;
            }
        }
        worker_handle.abort();
        assert!(saw_failed, "UploadFailed event should be emitted on panic");
        assert!(saw_panic_log, "Panic log entry should be emitted");
    }
}
