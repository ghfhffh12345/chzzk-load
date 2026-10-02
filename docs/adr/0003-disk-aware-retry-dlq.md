# 0003 - Disk-Aware Retry DLQ

We implemented a global, Disk-Aware Retry Policy for the Dead-Letter Queue (DLQ) to handle extreme disk pressure during network outages on constrained environments (like SBCs running RAM disks).

Under normal conditions, failed uploads (`.ts` video and `.jsonl` chat chunks) retry infinitely to guarantee eventual consistency. However, if the free disk space falls below `min_free_disk_gb`, the DLQ permanently deletes the oldest pending `chunk_XXXX.ts` and its corresponding `chat_XXXX.jsonl` from the global pool to prevent the system from crashing due to disk exhaustion.

### Considered Options

*   **Fixed Retries (e.g., 3 retries, then drop):** Rejected because home internet outages often exceed the ~14-second backoff window, causing unacceptable data loss. An archiver's primary goal is data retention.
*   **Per-Channel Eviction Quota:** Rejected because a single channel with a massive backlog shouldn't artificially limit its own quota while space exists globally. Global eviction is simpler and maximizes overall retention.

### Consequences

*   The startup reconciliation phase must scan for all sealed chunks (`chunk_XXXX.ts` not actively written to) and re-inject them into the DLQ on boot to persist state across crashes.
*   Data loss is explicitly accepted as a lesser evil compared to a hard process crash when the disk physically fills up.
