# 0015 - Unify CLI Storage Symmetry and Chunk Retention Policy

**Status**: Accepted (Amends ADR 0009, ADR 0011, ADR 0012, ADR 0013, and ADR 0014)

Prior to this architectural pivot, the `chzzk-load` command-line interface suffered from severe operational asymmetry, rigid storage assumptions, and hazardous chunk deletion defaults between the `vod` and `consolidate` subcommands:

1. **Fragmented CLI Option Names and Flags**:
   - `vod` exposed `-o, --output-dir`, `--upload`, `--delete-after-upload`, and `--headless` (alias: `--no-tui`), but lacked an `--overwrite` flag.
   - `consolidate` lacked `-o, --output-dir`, `--upload`, `--delete-after-upload`, and subcommand-level `--headless`, but supported `--overwrite` and an inverted chunk deletion flag `--keep-original`.
2. **Cloud-Biased Storage Assumptions**:
   - `consolidate` forcibly assumed that any remote input path (`remote:path`) must upload its merged output back to cloud storage, preventing users from downloading and consolidating remote recording sessions into a local directory for offline archival.
   - Local consolidation could not upload its consolidated artifacts to cloud storage without manual transfer scripts.
3. **Hazardous, Silent Data Loss Defaults**:
   - `consolidate` silently purged all raw source chunks (`chunk_%04d.ts`, `chat_%04d.jsonl`) by default unless the inverted `--keep-original` flag was explicitly passed.
   - When consolidating remote cloud recordings, this created an acute risk of catastrophic, irreversible data loss if a user omitted `--keep-original`.
4. **Artifact Triad Incompleteness Across Storage Boundaries**:
   - While `vod` always produced and uploaded the complete canonical VOD Artifact Triad (`vod.mp4`, `vod_chat.jsonl`, `vod_metadata.jsonl`), `consolidate` across storage boundaries failed to transfer `live_metadata.jsonl` to the destination target, leaving an incomplete Live Artifact Triad.

We unify subcommand options and storage workflows to provide 100% parity between local and cloud storage across both `vod` and `consolidate`, establish safe-by-default source chunk retention, and preserve canonical artifact triads across all storage boundaries.

---

## Decision

### 1. Universal Opt-In Cloud Upload (`--upload`)
- Cloud storage upload is strictly opt-in across both `vod` and `consolidate`.
- Local disk storage is the safe, consistent default destination for all operations:
  - Running `chzzk-load consolidate <remote:path>` consolidates remote chunks via HTTP loopback streaming and writes `livestream.mp4`, `live_chat.jsonl`, and `live_metadata.jsonl` directly to local disk without uploading back to the cloud.
  - Running `chzzk-load consolidate <local_path> --upload` consolidates local recording sessions and uploads the resulting Live Artifact Triad to remote cloud storage.
  - Running `chzzk-load consolidate <remote:path> --upload` consolidates remote sessions and uploads the consolidated artifacts to the remote cloud destination.

### 2. Clean-Break Safe-by-Default Chunk Retention (`--delete-chunks`)
- Remove the inverted `--keep-original` flag entirely (clean break: no deprecated alias).
- Raw source chunks (`chunk_%04d.ts`, `chat_%04d.jsonl`) on both local disk and remote cloud storage are strictly preserved by default.
- Purging original chunks requires an explicit, opt-in `--delete-chunks` flag.
- When `--delete-chunks` is passed with remote sessions, chunks are purged only after local consolidation and optional cloud uploads succeed with zero errors.

### 3. Symmetrical Destination and Lifecycle Targeting
Both `vod` and `consolidate` expose identical options with uniform semantics:
- `-o, --output-dir <DIR>`: Specifies the local destination directory.
  - For `consolidate`: Defaults to the source directory for local sources, or `<settings.general.recordings_dir>/<session_folder_name>` for remote sources.
  - For `vod`: Defaults to `<settings.general.recordings_dir>/[<liveOpenDate>] [<alias>] <channelName> - <liveTitle>`.
- `--upload`: Enables cloud storage transfer via the unified `UploadBackend`.
- `--remote-path <PATH>` (requires `--upload`): Overrides the cloud destination directory.
  - If omitted on `consolidate`: Defaults to source remote path (for remote source) or `<settings.rclone.remote_path>/<session_folder_name>` (for local source).
  - If omitted on `vod`: Defaults to `<settings.rclone.remote_path>/<session_folder_name>`.
- `--delete-after-upload` (requires `--upload`): Controls whether local target files are unlinked after confirmed cloud upload. Local files are retained locally by default.
- `--overwrite`: Overwrites existing target media files and invalidates stale staging directories.
  - For `vod`: Guards against accidental clobbering of existing `vod.mp4`, `vod_chat.jsonl`, or `vod_metadata.jsonl` unless `--overwrite` is specified.
  - For `consolidate`: Guards against accidental clobbering of existing `livestream.mp4` or `live_chat.jsonl` unless `--overwrite` is specified.
- `--headless` (visible alias: `--no-tui`): Runs in non-interactive console mode with milestone/summary logging, bypassing interactive ANSI progress bars.

### 4. Canonical Preservation of the Live Artifact Triad
- Consolidation guarantees that the full canonical Live Artifact Triad (`livestream.mp4`, `live_chat.jsonl`, `live_metadata.jsonl`) is present at the final destination:
  - In remote-to-local consolidation (without `--upload`), `live_metadata.jsonl` is downloaded from remote storage into the local destination directory alongside the consolidated video and chat files.
  - In cloud upload workflows (with `--upload`), `live_metadata.jsonl` is uploaded to the target remote directory alongside `livestream.mp4` and `live_chat.jsonl`.
  - When `--delete-after-upload` is active, local output files (including local metadata when applicable) are unlinked post-upload, preserving directory cleanliness.
  - When `--delete-chunks` is active, only chunk files (`chunk_%04d.ts`, `chat_%04d.jsonl`) are purged; `live_metadata.jsonl` is strictly preserved in both source and destination locations.

---

## Considered Options

1. **Retain `--keep-original` with Inverted Defaults vs. Clean-Break `--delete-chunks`**:
   - *Considered*: Keeping `--keep-original` but changing its default to true, or providing a deprecated alias.
   - *Rejected*: Inverted flags (e.g. `--no-keep-original` or default-true `--keep-original`) create cognitive dissonance and semantic confusion. A clean-break migration to `--delete-chunks` aligns `chzzk-load` with industry-standard safety practices: non-destructive actions by default, destructive actions requiring explicit affirmative opt-in.
2. **Implicit Destination Inferencing based on Input Prefix**:
   - *Considered*: Continuing to infer that remote input paths imply remote output destinations, and local input paths imply local output destinations.
   - *Rejected*: This prevented offline archival of cloud recordings (remote-to-local) and required manual user scripts for local-to-cloud archiving. Decoupling the input source location from the output storage destination via explicit `--upload` and `-o, --output-dir` delivers complete architectural symmetry.
3. **Omitting Metadata in Consolidation**:
   - *Considered*: Treating `live_metadata.jsonl` as an ephemeral live-recording artifact and omitting it from consolidated targets.
   - *Rejected*: Stream state transitions, tags, category changes, and chronological sync offsets are indispensable for downstream media servers, indexing pipelines, and timeline-accurate player scrubbing. Both live recordings and VOD sessions must adhere strictly to their canonical Artifact Triad invariants.

---

## Consequences

- **Zero Accidental Data Loss**: Users can safely run `chzzk-load consolidate` without fear of unrecoverable chunk deletion. Raw capture files are never deleted unless the user explicitly passes `--delete-chunks`.
- **First-Class Offline & Cloud Archival Parity**: Users can seamlessly consolidate cloud-recorded streams directly to local media drives for offline viewing, or consolidate local streams and push them to cloud storage.
- **Uniform CLI Mental Model**: Flags like `-o, --output-dir`, `--upload`, `--remote-path`, `--delete-after-upload`, `--overwrite`, and `--headless` share identical syntax and semantics across `vod` and `consolidate`.
- **Breaking Change**: Invocations passing `--keep-original` will fail argument parsing. Users wishing to reclaim disk space must update scripts to pass `--delete-chunks`.
- **Artifact Triad Invariant Preserved**: Downstream consumers and player tools can rely on finding complete artifact triads at both local and remote destinations.
