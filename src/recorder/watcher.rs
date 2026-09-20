use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Scans `session_dir` for `.ts` chunk files and identifies chunks that are
/// safely sealed and ready for upload according to the N+1 chunk completion rule.
///
/// Under the N+1 rule:
/// - If `chunk_{N+1}.ts` exists and has size > 0, `chunk_{N}.ts` is guaranteed sealed.
/// - When `is_stream_finished` is true, the final active chunk is also marked sealed.
pub fn detect_sealed_chunks(
    session_dir: &Path,
    already_enqueued: &mut HashSet<String>,
    is_stream_finished: bool,
) -> Vec<PathBuf> {
    let mut chunks = Vec::new();

    if let Ok(entries) = fs::read_dir(session_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file()
                && path.extension().and_then(|s| s.to_str()).is_some_and(|ext| ext.eq_ignore_ascii_case("ts"))
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
            {
                let meta = fs::metadata(&path).or_else(|_| entry.metadata());
                if let Ok(meta) = meta
                    && meta.len() > 0
                {
                    chunks.push((name.to_string(), path, meta.len()));
                }
            }
        }
    }

    chunks.sort_by(|a, b| a.0.cmp(&b.0));
    let mut sealed = Vec::new();

    if chunks.is_empty() {
        return sealed;
    }

    // N+1 rule: If chunk N+1 exists, chunk N is sealed
    let completed_len = chunks.len().saturating_sub(1);
    for (name, path, _) in &chunks[..completed_len] {
        if !already_enqueued.contains(name) {
            already_enqueued.insert(name.clone());
            sealed.push(path.clone());
        }
    }

    // If stream ended, the final chunk is also sealed
    if is_stream_finished
        && let Some((name, path, _)) = chunks.last()
        && !already_enqueued.contains(name)
    {
        already_enqueued.insert(name.clone());
        sealed.push(path.clone());
    }

    sealed
}

/// A stateful watcher for a session directory that tracks enqueued chunks
/// and provides helper methods to query newly sealed chunks.
#[derive(Debug, Clone)]
pub struct SegmentWatcher {
    session_dir: PathBuf,
    already_enqueued: HashSet<String>,
}

impl SegmentWatcher {
    pub fn new(session_dir: impl Into<PathBuf>) -> Self {
        Self {
            session_dir: session_dir.into(),
            already_enqueued: HashSet::new(),
        }
    }

    pub fn detect_sealed(&mut self, is_stream_finished: bool) -> Vec<PathBuf> {
        detect_sealed_chunks(&self.session_dir, &mut self.already_enqueued, is_stream_finished)
    }

    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    pub fn enqueued_chunks(&self) -> &HashSet<String> {
        &self.already_enqueued
    }
}
