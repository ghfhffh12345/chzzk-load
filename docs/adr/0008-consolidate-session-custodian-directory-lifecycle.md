# 0008 - Consolidate Session Custodian Directory Lifecycle

**Status**: Accepted

Session directory lifecycle management was previously fragmented across two separate locations: the `SessionCustodian` coordinator (`src/engine/custodian.rs`) tracking active and draining sessions, and a separate helper module `src/engine/cleanup.rs` implementing low-level 0-entry directory unlinking and coarse sweeps. This split architecture resulted in several maintainability and correctness issues:

1. **Split Ownership & Shallow Modules**: Directory unlinking and sweep routines lived in `src/engine/cleanup.rs`, while session state tracking and quiescence coordination lived in `SessionCustodian`. This split created an artificial boundary and required circular coordination between the modules.
2. **String-Prefix Matching Fragility**: The unmanaged directory sweep routine (`cleanup_empty_session_dirs_excluding`) relied on fragile string prefix heuristics (`dir_name_str == item || dir_name_str.starts_with(&format!("{item}_"))`) to identify active sessions. This assumption failed when channel configurations used custom directory naming patterns, timestamps, or streamer aliases (`[{timestamp}] [{alias}] {streamer} - {title}`).
3. **Test-Convenience Middle-Man Shims**: `EngineOrchestrator` exposed four static forwarding shims (`cleanup_empty_session_dirs_excluding`, `cleanup_empty_session_dirs`, `cleanup_empty_session_dirs_bounded`, and `cleanup_session_dir_if_empty`) purely to facilitate test calls, violating the repository coding standard against pass-through middle men (`CODING_STANDARDS.md` Section 1).
4. **Redundant Live Sweeps**: In `EngineOrchestrator::poll_channels_once`, every `StreamClosed` transition initiated an unmanaged directory sweep over the entire recordings directory, executing unnecessary filesystem I/O during routine polling cycles.

We consolidate the complete session directory lifecycle into a single cohesive, deep module: `SessionCustodian`.

## Decision

1. **Absorb 0-Entry Directory Unlinking into `SessionCustodian`**:
   - Implement `SessionCustodian::purge_dir_if_empty` directly within `src/engine/custodian.rs`, absorbing the low-level directory inspection and unlinking logic from `cleanup.rs`.
   - Update `SessionCustodian::try_purge` to delegate to `Self::purge_dir_if_empty`.

2. **Exact Canonical Path Protection in `sweep_unmanaged`**:
   - Implement `SessionCustodian::sweep_unmanaged(&self, recordings_dir: &Path)` replacing `cleanup_empty_session_dirs_excluding`.
   - Protect active recording sessions via exact canonical path matching against `self.active_paths()` using `std::fs::canonicalize`, completely eliminating string-prefix heuristics.
   - Guard against concurrent in-flight purges via `in_flight_purges` tracking.
   - Remove successfully purged sessions from internal tracking if they were in `SessionTrackState::Draining`.

3. **Bounded Sweeps on `SessionCustodian`**:
   - Provide `SessionCustodian::sweep_empty_dirs_bounded(recordings_dir: &Path, timeout: Duration) -> std::io::Result<usize>` for time-critical shutdown and escalation teardown, wrapping `SessionCustodian::without_events().sweep_unmanaged(recordings_dir)` in a Tokio timeout.

4. **Eliminate Orchestrator Forwarding Shims & Purge Legacy Cleanup Module**:
   - Remove the four static forwarding shims from `EngineOrchestrator`.
   - Expose typed accessor `pub fn custodian(&self) -> &SessionCustodian` on `EngineOrchestrator`.
   - Wire `run()` periodic ticks and shutdown barriers, as well as `main.rs` graceful and forced shutdown routines, directly through `SessionCustodian`.
   - Delete `src/engine/cleanup.rs` (`pub mod cleanup;` and its re-exports).

5. **Migrate Test Suites to Direct Seams**:
   - Migrate integration and unit test suites (`tests/test_engine_events.rs`, `tests/test_engine_orchestrator_registry.rs`) to test against `SessionCustodian` methods directly instead of routing through orchestrator middle men.

## Invariants

1. **Strict 0-Entry Directory Emptiness**: A directory is candidate for deletion if and only if `tokio::fs::read_dir` yields exactly 0 entries (`next_entry().await?.is_none()`). Heuristic file-name sniffing (`has_metadata`) is strictly prohibited.
2. **Windows File-Lock Retry Resilience**: Directory deletion retries transient Windows filesystem lock errors (sharing violation `32`, access denied `5`, asynchronous pending unlink latency `145`, or `ErrorKind::PermissionDenied`) up to 5 attempts with bounded exponential backoff (`20ms * attempt`), treating `ErrorKind::NotFound` or non-existence as immediate success.
3. **Exact Canonical Path Protection**: Active recording session directories are shielded from sweeps by canonical filesystem path comparison against `self.active_paths()`. Unmanaged empty directories or drained sessions are safely swept.
4. **Centralized Attributed & Summary Logging**:
   - Single-session purges (`try_purge`) emit streamer/channel-attributed clean logs: `[{target}] Cleaned up empty session folder '{path}'`.
   - Bulk sweeps (`sweep_unmanaged`) emit summary clean logs when one or more folders are swept: `Cleaned up {count} empty session folder(s) in '{recordings_dir}'`.

## Considered Options

- **Retaining `src/engine/cleanup.rs` as a separate low-level helper**: Rejected because splitting directory inspection from directory lifecycle tracking created two shallow abstractions where one deep module is required, and encouraged forwarding shims on `EngineOrchestrator`.
- **Retaining forwarding shims on `EngineOrchestrator` for backward compatibility**: Rejected because `chzzk-load` is a standalone application where internal API compatibility is unnecessary, and middle-man forwarding shims violate `CODING_STANDARDS.md` Section 1.
- **Relying on string-prefix exclusion in unmanaged sweeps**: Rejected because channel directories can be configured with diverse timestamp, streamer, or alias prefixes that break simple string-prefix matching.

## Consequences

- `SessionCustodian` serves as the single source of truth and execution coordinator for all session directory states, quiescence verification, 0-entry unlinking, and unmanaged directory sweeps.
- `EngineOrchestrator` is relieved of directory unlinking logic, presenting a clean typed interface without pass-through middle men.
- Fragile string-prefix matching is replaced with robust, canonical path containment.
- Codebase surface is simplified with the removal of `src/engine/cleanup.rs`.
- All test suites verify behavior directly against the public `SessionCustodian` seam.
