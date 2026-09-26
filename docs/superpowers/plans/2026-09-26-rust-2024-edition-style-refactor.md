# Rust 2024 Edition Style Refactor Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `chzzk-load`'s Rust module layout and coding style to match modern Rust 2024 conventions, eliminating `mod.rs` in favor of `foo.rs` + `foo/`, and adhering to modern idiomatic standards.

**Architecture:** Replace the legacy 2015-style `foo/mod.rs` module layout with Rust 2018/2024 `foo.rs` and `foo/` subdirectories. Modernize format string arguments to inlined captures (`format!("{x}")`), simplify module re-exports (removing `self::`), and ensure all codebase modules and documentation conform to Rust 2024 idiomatic patterns without any compiler or Clippy warnings.

**Tech Stack:** Rust 2024 Edition, Tokio, Ratatui, Crossterm, Reqwest.

## Global Constraints
- Strictly preserve all existing functionality, CLI flags, TUI rendering behavior, and test guarantees.
- Zero warnings under `cargo clippy --all-targets -- -D warnings`.
- Zero formatting issues under `cargo fmt --check`.
- All tests in `tests/` must pass 100%.
- No `mod.rs` files may remain in `src/`.

---

### Task 1: Migrate Module Structure from `mod.rs` to `foo.rs`

**Files:**
- Move: `src/chzzk/mod.rs` -> `src/chzzk.rs`
- Move: `src/drive/mod.rs` -> `src/drive.rs`
- Move: `src/engine/mod.rs` -> `src/engine.rs` (and delete empty `src/engine/` dir)
- Move: `src/recorder/mod.rs` -> `src/recorder.rs`
- Move: `src/tui/mod.rs` -> `src/tui.rs`
- Move: `src/uploader/mod.rs` -> `src/uploader.rs` (and delete empty `src/uploader/` dir)

- [ ] **Step 1: Move module files and remove empty directories**
- [ ] **Step 2: Clean up redundant `self::` in re-exports (e.g. in `src/tui.rs`)**
- [ ] **Step 3: Verify module compilation with `cargo check --all-targets`**
- [ ] **Step 4: Verify that no `mod.rs` files remain in `src/`**

---

### Task 2: Modern Idiomatic Rust 2024 Code Modernization

**Files:**
- Modify: `src/main.rs`
- Modify: `src/uploader.rs`
- Modify: `src/tui/ui.rs`
- Modify: `src/tui.rs`
- Modify: `src/engine.rs`
- Modify: other `src/` modules as needed for inlined format args and modern idioms

- [ ] **Step 1: Modernize `src/main.rs` (inlined format args, cleaner control flow)**
- [ ] **Step 2: Modernize `src/tui/ui.rs` and `src/tui.rs` (inlined format args, simplified exports)**
- [ ] **Step 3: Modernize `src/uploader.rs` and `src/engine.rs` (inlined format args, doc comments, clean error patterns)**
- [ ] **Step 4: Run `cargo clippy --all-targets -- -D warnings` and fix any issues**

---

### Task 3: Update Architectural Documentation

**Files:**
- Modify: `AGENTS.md`
- Check: `README.md`, `README.ko.md`

- [ ] **Step 1: Update `AGENTS.md` file tree and module paths from `foo/mod.rs` to `foo.rs`**
- [ ] **Step 2: Verify consistency across all repository documentation**

---

### Task 4: Complete Verification Suite

- [ ] **Step 1: Run `cargo fmt --check`**
- [ ] **Step 2: Run `cargo clippy --all-targets -- -D warnings`**
- [ ] **Step 3: Run `cargo test` across all targets**
- [ ] **Step 4: Run npm package tests `node scripts/test-npm-packages.js`**
