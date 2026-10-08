use anyhow::Context;
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Represents whether the target recording session resides locally or on a remote storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetLocation {
    Local(PathBuf),
    Remote(String),
}

impl TargetLocation {
    /// Parses a raw user-supplied target string into either a `Local` or `Remote` location.
    pub fn parse(raw: &str) -> Self {
        if is_remote_path(raw) {
            TargetLocation::Remote(raw.to_string())
        } else {
            TargetLocation::Local(PathBuf::from(raw))
        }
    }

    /// Returns `true` if the target is a remote rclone path.
    pub fn is_remote(&self) -> bool {
        matches!(self, TargetLocation::Remote(_))
    }

    /// Returns `true` if the target is a local filesystem path.
    pub fn is_local(&self) -> bool {
        matches!(self, TargetLocation::Local(_))
    }

    /// Returns the raw string slice representation of this target.
    pub fn raw(&self) -> &str {
        match self {
            TargetLocation::Local(p) => p.to_str().unwrap_or(""),
            TargetLocation::Remote(s) => s.as_str(),
        }
    }
}

/// Helper function to check if the path starts with a Windows drive letter prefix (e.g. `C:\`, `D:/`, `C:foo`).
fn is_windows_drive_prefix(s: &str) -> bool {
    let mut chars = s.chars();
    if let Some(first) = chars.next() {
        if first.is_ascii_alphabetic() && chars.next() == Some(':') {
            return true;
        }
    }
    false
}

/// Identifies whether `<path>` is an rclone remote path (`remote:bucket/path`) or local directory.
///
/// Windows drive letters (`C:\`, `D:\`, `C:/`) and UNC paths (`\\server\share`) are strictly recognized as local paths.
pub fn is_remote_path(s: &str) -> bool {
    // 1. Windows drive specifiers are always Local
    if is_windows_drive_prefix(s) {
        return false;
    }

    // 2. UNC paths and extended paths are always Local
    if s.starts_with(r"\\") || s.starts_with("//") {
        return false;
    }

    // 3. Look for the first colon
    if let Some(colon_idx) = s.find(':') {
        if colon_idx == 0 {
            // e.g. ":path" is not a valid rclone remote name
            return false;
        }
        let prefix = &s[..colon_idx];
        // In rclone syntax, remote names cannot contain path separators ('/' or '\')
        if !prefix.contains('/') && !prefix.contains('\\') {
            return true;
        }
    }

    false
}

/// Joins a remote base path with a child path or filename without duplicate slashes,
/// respecting rclone root bucket colons (e.g. `remote:` -> `remote:child`).
pub fn join_remote_path(remote_base: &str, child: &str) -> String {
    let trimmed_base = remote_base.trim_end_matches('/');
    let trimmed_child = child.trim_start_matches('/');
    if trimmed_base.ends_with(':') {
        format!("{trimmed_base}{trimmed_child}")
    } else {
        format!("{trimmed_base}/{trimmed_child}")
    }
}

/// Represents a single discovered chunk (video or chat) with numeric index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidationChunk {
    pub index: u32,
    pub name: String,
    pub size: u64,
}

/// Represents a detected gap range in chunk indices (inclusive: `[start, end]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkGap {
    pub start: u32,
    pub end: u32,
}

impl ChunkGap {
    pub fn format(&self) -> String {
        if self.start == self.end {
            format!("{}", self.start)
        } else {
            format!("{}-{}", self.start, self.end)
        }
    }
}

/// Attempts to parse a numeric chunk index from a video chunk filename (`chunk_%04d.ts` -> index).
pub fn parse_video_chunk_index(name: &str) -> Option<u32> {
    let lower = name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".ts")?;
    let index_str = stem.strip_prefix("chunk_")?;
    if index_str.is_empty() || !index_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    index_str.parse::<u32>().ok()
}

/// Attempts to parse a numeric chunk index from a chat chunk filename (`chat_%04d.jsonl` -> index).
pub fn parse_chat_chunk_index(name: &str) -> Option<u32> {
    let lower = name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".jsonl")?;
    let index_str = stem.strip_prefix("chat_")?;
    if index_str.is_empty() || !index_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    index_str.parse::<u32>().ok()
}

/// Detects gaps between consecutive intermediate chunk indices (Issue #22).
///
/// Leading chunks starting at index > 0 are not flagged as gaps.
pub fn detect_index_gaps(indices: &[u32]) -> Vec<ChunkGap> {
    let mut gaps = Vec::new();
    if indices.is_empty() {
        return gaps;
    }
    // Check gaps between consecutive intermediate chunks
    for window in indices.windows(2) {
        let prev = window[0];
        let next = window[1];
        if next > prev + 1 {
            gaps.push(ChunkGap {
                start: prev + 1,
                end: next - 1,
            });
        }
    }
    gaps
}

/// Normalized entry discovered in target directory or remote bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawManifestEntry {
    pub name: String,
    pub size: u64,
    pub is_dir: bool,
}

/// Consolidation manifest detailing all discovered and validated chunks for a recording session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidationManifest {
    pub target: TargetLocation,
    pub video_chunks: Vec<ConsolidationChunk>,
    pub chat_chunks: Vec<ConsolidationChunk>,
    pub has_metadata: bool,
    pub pre_existing_video: bool,
    pub pre_existing_chat: bool,
    pub warnings: Vec<String>,
}

/// Pure manifest constructor and validator operating on discovered file entries.
pub fn build_manifest_from_entries(
    target: TargetLocation,
    entries: Vec<RawManifestEntry>,
    strict: bool,
    overwrite: bool,
) -> anyhow::Result<ConsolidationManifest> {
    let mut video_chunks = Vec::new();
    let mut chat_chunks = Vec::new();
    let mut has_metadata = false;
    let mut pre_existing_video = false;
    let mut pre_existing_chat = false;
    let mut warnings = Vec::new();

    for entry in entries {
        if entry.is_dir {
            continue;
        }

        let name_lower = entry.name.to_ascii_lowercase();
        if name_lower == "consolidated.mp4" {
            pre_existing_video = true;
        } else if name_lower == "consolidated.jsonl" {
            pre_existing_chat = true;
        } else if name_lower == "metadata.jsonl" {
            has_metadata = true;
        } else if let Some(idx) = parse_video_chunk_index(&entry.name) {
            video_chunks.push(ConsolidationChunk {
                index: idx,
                name: entry.name,
                size: entry.size,
            });
        } else if let Some(idx) = parse_chat_chunk_index(&entry.name) {
            chat_chunks.push(ConsolidationChunk {
                index: idx,
                name: entry.name,
                size: entry.size,
            });
        }
    }

    // Sort chunks numerically by chunk index
    video_chunks.sort_by_key(|c| c.index);
    chat_chunks.sort_by_key(|c| c.index);

    // Single-media handling:
    // If zero chunks of both exist, returns an error.
    if video_chunks.is_empty() && chat_chunks.is_empty() {
        return Err(anyhow::anyhow!(
            "No video chunks (chunk_%04d.ts) or chat chunks (chat_%04d.jsonl) found in target: {}",
            target.raw()
        ));
    }

    // Pre-existing file check:
    // If present and !overwrite, returns an error.
    let mut conflicting_files = Vec::new();
    if !video_chunks.is_empty() && pre_existing_video {
        conflicting_files.push("consolidated.mp4");
    }
    if !chat_chunks.is_empty() && pre_existing_chat {
        conflicting_files.push("consolidated.jsonl");
    }
    if !conflicting_files.is_empty() && !overwrite {
        return Err(anyhow::anyhow!(
            "Pre-existing destination file(s) found in target '{}': {}; pass --overwrite to replace",
            target.raw(),
            conflicting_files.join(", ")
        ));
    }

    // Sequence contiguity validation
    validate_chunk_contiguity("Video", &video_chunks, &target, strict, &mut warnings)?;
    validate_chunk_contiguity("Chat", &chat_chunks, &target, strict, &mut warnings)?;

    Ok(ConsolidationManifest {
        target,
        video_chunks,
        chat_chunks,
        has_metadata,
        pre_existing_video,
        pre_existing_chat,
        warnings,
    })
}

/// Helper to validate sequence contiguity for a set of discovered chunks.
fn validate_chunk_contiguity(
    media_name: &str,
    chunks: &[ConsolidationChunk],
    target: &TargetLocation,
    strict: bool,
    warnings: &mut Vec<String>,
) -> anyhow::Result<()> {
    if chunks.is_empty() {
        return Ok(());
    }
    let indices: Vec<u32> = chunks.iter().map(|c| c.index).collect();
    let gaps = detect_index_gaps(&indices);
    if !gaps.is_empty() {
        let gap_str = gaps
            .iter()
            .map(|g| g.format())
            .collect::<Vec<_>>()
            .join(", ");
        let msg = format!("{media_name} chunk sequence has gap(s): missing chunk(s) {gap_str}");
        if strict {
            return Err(anyhow::anyhow!(
                "Contiguity validation failed for {}: {msg}",
                target.raw()
            ));
        } else {
            eprintln!("[WARN] {msg}");
            warnings.push(msg);
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize, Clone)]
struct RcloneLsJsonEntry {
    #[serde(rename = "Path", default)]
    pub path: String,
    #[serde(rename = "Name", default)]
    pub name: String,
    #[serde(rename = "Size", default)]
    pub size: Option<i64>,
    #[serde(rename = "IsDir", default)]
    pub is_dir: bool,
}

/// Parses the JSON stdout of `rclone lsjson` into a vector of normalized entries.
pub fn parse_rclone_lsjson(json_bytes: &[u8]) -> anyhow::Result<Vec<RawManifestEntry>> {
    let items: Vec<RcloneLsJsonEntry> = serde_json::from_slice(json_bytes)
        .context("Failed to parse JSON output from rclone lsjson")?;

    let entries = items
        .into_iter()
        .map(|item| {
            let name = if !item.name.is_empty() {
                item.name
            } else {
                item.path
            };
            let size = item.size.unwrap_or(0).max(0) as u64;
            RawManifestEntry {
                name,
                size,
                is_dir: item.is_dir,
            }
        })
        .collect();

    Ok(entries)
}

/// Resolves the rclone binary to execute:
/// override_bin > `CHZZK_LOAD_RCLONE_BIN` env var > `"rclone"`.
pub fn resolve_rclone_bin(override_bin: Option<&str>) -> String {
    if let Some(bin) = override_bin {
        if !bin.trim().is_empty() {
            return bin.to_string();
        }
    }
    if let Some(bin) = std::env::var("CHZZK_LOAD_RCLONE_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
        return bin;
    }
    "rclone".to_string()
}

/// Discovers the manifest for a remote target via `rclone lsjson`.
pub async fn discover_remote_manifest(
    remote_path: &str,
    strict: bool,
    overwrite: bool,
    rclone_bin: Option<&str>,
) -> anyhow::Result<ConsolidationManifest> {
    let bin = resolve_rclone_bin(rclone_bin);
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.kill_on_drop(true);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.arg("lsjson").arg(remote_path);

    let output = cmd
        .output()
        .await
        .with_context(|| format!("Failed to execute '{bin} lsjson {remote_path}'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let trimmed = stderr.trim();
        return if trimmed.is_empty() {
            Err(anyhow::anyhow!(
                "rclone lsjson failed with status {}",
                output.status
            ))
        } else {
            Err(anyhow::anyhow!(
                "rclone lsjson failed with status {}: {}",
                output.status,
                trimmed
            ))
        };
    }

    let entries = parse_rclone_lsjson(&output.stdout)?;
    build_manifest_from_entries(
        TargetLocation::Remote(remote_path.to_string()),
        entries,
        strict,
        overwrite,
    )
}

/// Discovers the manifest for a local directory target.
pub async fn discover_local_manifest(
    local_path: &Path,
    strict: bool,
    overwrite: bool,
) -> anyhow::Result<ConsolidationManifest> {
    if !local_path.exists() {
        return Err(anyhow::anyhow!(
            "Target directory does not exist: {}",
            local_path.display()
        ));
    }
    if !local_path.is_dir() {
        return Err(anyhow::anyhow!(
            "Target path is not a directory: {}",
            local_path.display()
        ));
    }

    let mut read_dir = tokio::fs::read_dir(local_path)
        .await
        .with_context(|| format!("Failed to read target directory: {}", local_path.display()))?;

    let mut entries = Vec::new();

    while let Some(entry) = read_dir.next_entry().await.with_context(|| {
        format!(
            "Error reading entries in directory: {}",
            local_path.display()
        )
    })? {
        let file_type = entry.file_type().await.ok();
        let is_dir = file_type.map(|ft| ft.is_dir()).unwrap_or(false);
        let name = entry.file_name().to_string_lossy().to_string();
        let size = if !is_dir {
            entry.metadata().await.map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };

        entries.push(RawManifestEntry { name, size, is_dir });
    }

    build_manifest_from_entries(
        TargetLocation::Local(local_path.to_path_buf()),
        entries,
        strict,
        overwrite,
    )
}

/// Discovers and validates chunk manifest for the target location.
pub async fn discover_manifest(
    target: &TargetLocation,
    strict: bool,
    overwrite: bool,
) -> anyhow::Result<ConsolidationManifest> {
    discover_manifest_with_bin(target, strict, overwrite, None).await
}

/// Discovers and validates chunk manifest for the target location with optional rclone binary override.
pub async fn discover_manifest_with_bin(
    target: &TargetLocation,
    strict: bool,
    overwrite: bool,
    rclone_bin: Option<&str>,
) -> anyhow::Result<ConsolidationManifest> {
    match target {
        TargetLocation::Local(p) => discover_local_manifest(p, strict, overwrite).await,
        TargetLocation::Remote(r) => {
            discover_remote_manifest(r, strict, overwrite, rclone_bin).await
        }
    }
}
