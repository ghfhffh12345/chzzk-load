use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Attempts to parse a numeric chunk index from a chunk filename (e.g. "chunk_0001.ts" -> 1).
pub fn parse_chunk_index(file_name: &str) -> Option<u64> {
    let lower = file_name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".ts")?;
    let index_str = stem.strip_prefix("chunk_")?;
    index_str.parse::<u64>().ok()
}

/// Scans `session_dir` for `.ts` chunk files and identifies chunks that are
/// safely sealed and ready for upload according to explicit numeric N+1 boundary safety:
///
/// Chunk $i$ is sealed if and only if:
/// - There exists some chunk $j > i$ on disk with size > 0, OR
/// - `is_stream_finished` is true (in which case the active final chunk is also sealed).
///
/// Any file already recorded in `already_enqueued` is skipped.
pub fn detect_sealed_chunks(
    session_dir: &Path,
    already_enqueued: &mut HashSet<String>,
    is_stream_finished: bool,
) -> Vec<PathBuf> {
    let mut numeric_chunks: Vec<(u64, String, PathBuf)> = Vec::new();
    let mut other_chunks: Vec<(String, PathBuf)> = Vec::new();

    if let Ok(entries) = fs::read_dir(session_dir) {
        for entry in entries.flatten() {
            let is_file = match entry.file_type() {
                Ok(ft) => ft.is_file(),
                Err(_) => entry.path().is_file(),
            };
            if !is_file {
                continue;
            }

            let file_name = entry.file_name();
            let name_str = match file_name.to_str() {
                Some(s) if s.len() >= 3 && s[s.len() - 3..].eq_ignore_ascii_case(".ts") => s,
                _ => continue,
            };

            let path = entry.path();
            let meta = match entry.metadata() {
                Ok(m) if m.len() > 0 => Ok(m),
                _ => fs::metadata(&path),
            };
            if let Ok(meta) = meta
                && meta.len() > 0
            {
                if let Some(index) = parse_chunk_index(name_str) {
                    numeric_chunks.push((index, name_str.to_string(), path));
                } else {
                    other_chunks.push((name_str.to_string(), path));
                }
            }
        }
    }

    numeric_chunks.sort_by_key(|c| c.0);
    let mut sealed = Vec::new();

    if let Some(&max_index) = numeric_chunks.iter().map(|c| &c.0).max() {
        for (index, name, path) in &numeric_chunks {
            // N+1 invariant: chunk i is sealed if there exists j > i with size > 0
            let is_sealed = *index < max_index || is_stream_finished;
            if is_sealed && !already_enqueued.contains(name) {
                already_enqueued.insert(name.clone());
                sealed.push(path.clone());
            }
        }
    }

    // Fallback for non-numeric .ts files if any exist
    if !other_chunks.is_empty() {
        other_chunks.sort_by(|a, b| a.0.cmp(&b.0));
        let completed_len = if is_stream_finished {
            other_chunks.len()
        } else {
            other_chunks.len().saturating_sub(1)
        };
        for (name, path) in &other_chunks[..completed_len] {
            if !already_enqueued.contains(name) {
                already_enqueued.insert(name.clone());
                sealed.push(path.clone());
            }
        }
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
        detect_sealed_chunks(
            &self.session_dir,
            &mut self.already_enqueued,
            is_stream_finished,
        )
    }

    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    pub fn enqueued_chunks(&self) -> &HashSet<String> {
        &self.already_enqueued
    }
}
