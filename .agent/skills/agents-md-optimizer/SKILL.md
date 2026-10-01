---
name: agents-md-optimizer
description: Guides condensing, maintaining, and updating AGENTS.md or agent specification guides to stay within strict line budgets (target <= 150 lines) without losing technical fidelity, architectural invariants, or operational commands.
---

# AGENTS.md Optimizer

Use this skill when editing, compressing, or updating `AGENTS.md` (or other developer/AI agent specification guides) to maintain high information density while strictly adhering to a line budget (<= 150 lines).

## When to use
- Condensing bloated `AGENTS.md` or system prompt guides that exceed 150 lines.
- Adding new features, architectural invariants, or lifecycle updates to `AGENTS.md` without increasing overall file bloat.
- Auditing documentation for filler prose, redundant narratives, and oversized directory trees.

## When NOT to use
- Drafting long-form architecture blueprints or readiness plans (use `plans/YYYY-MM-DD-<topic>.md`).
- Writing user-facing documentation (use `README.md` or `README.ko.md`).

---

## Section Budget Target (Total: ~130–150 lines)

| Section | Target Lines | Key Content & Compression Strategy |
| :--- | :---: | :--- |
| **Header & Intro** | 3–5 | 1 summary sentence covering core tech stack and system mission. No welcome greetings or filler. |
| **1. Key Characteristics** | 12–16 | Dense bullet list of technical traits (zero transcoding, P2P bypass, flash I/O, bounded disk, etc.). |
| **2. Repository Structure** | 14–18 | Compact shallow tree (15–18 lines max); use inline comments (`# ...`) instead of deeply nested individual files. |
| **3. Architecture & Lifecycle** | 25–35 | Subsections (Engine, FFmpeg/Watcher, Chat, Upload/DLQ, TUI) summarized as 3–4 bullet points each with exact technical parameters. |
| **4. CI/CD & Distribution** | 14–18 | Workflows, Zig musl targets, npm optionalDependencies pattern, and OIDC auth in bullet points. |
| **5. Testing & Workflow** | 20–25 | Implementation gate callout, 4-step TDD loop, and compact multi-command code block. |
| **6. Architectural Invariants** | 20–30 | Strictly numbered, 1-line or 2-line punchy invariant rules. Never drop or soften an invariant. |

---

## Compression Heuristics

1. **Eliminate Narrative Padding**:
   - ❌ *"Welcome to chzzk-load. This document serves as the primary technical specification and operational guide..."*
   - ✅ *"`chzzk-load` is a standalone Rust application with a Ratatui TUI that monitors..."*
2. **Compact Directory Trees**:
   - ❌ Expanding 4 levels of nested files over 70+ lines.
   - ✅ Group module directories with inline role annotations:
     ```text
     ├── src/
     │   ├── chzzk/      # client.rs (API/CDN extract), chat.rs (WebSocket), models.rs
     │   ├── recorder/   # ffmpeg.rs (process/flags), watcher.rs (numeric N+1 sealing)
     ```
3. **Preserve Concrete Specifics**:
   - Always retain concrete parameters, environment variables, commands, endpoints, and flags (e.g. `-c copy`, `min_free_disk_gb`, `CHZZK_LOAD_FFMPEG_BIN`, `chunk_%04d.ts`, `2s, 4s, 8s`).
   - Remove redundant prose explaining *why* a technology exists if the rule or invariant already enforces the constraint.
4. **Dense Invariant Phrasing**:
   - Format invariants as **Bold Key Concept**: direct actionable constraint.

---

## Verification & Execution Steps

1. **Count Lines Pre- and Post-Edit**:
   ```bash
   # In PowerShell / Windows terminal
   (Get-Content AGENTS.md).Length
   ```
2. **Audit Invariant Retention**:
   - Verify every invariant (terminal isolation, panic hooks, `-c copy`, test ports, path resolution, flash longevity, telemetry backpressure, binary resolution, EOF exit, mutex scoping, TOML sanitization, metadata sync, disk circuit breaker, numeric N+1 safety, user approval gate, plans directory) is preserved.
3. **Verify Line Budget**:
   - Confirm total line count is strictly $\le 150$ lines.
