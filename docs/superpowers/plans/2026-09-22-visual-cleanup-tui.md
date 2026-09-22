# Visual Cleanup Process in TUI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Visually display the ongoing cleanup process (stopping recordings, flushing FFmpeg, and finishing uploads) in the Ratatui TUI dashboard upon quitting instead of closing the terminal window immediately.

**Architecture:** Extend `App` state with an `is_shutting_down` flag. When the user requests a quit via `q` or `Ctrl+C`, the TUI transitions to a shutdown view (amber/yellow header banner and action-oriented footer notice) while keeping the render loop, channels list, active upload progress gauges, and activity logs fully live until `orch_handle.is_finished()` (or a safety timeout / second `q` or `Ctrl+C` force-exit).

**Tech Stack:** Rust, Ratatui, Crossterm, Tokio, Tokio-util (CancellationToken).

**Spec:** In-chat bounded design approved in conversation:
- Header: amber/yellow banner indicating active shutdown and cleanup.
- Footer: force-exit instructions (`[q / Ctrl+C] Force Exit Immediately`).
- Body & Logs: channels active badges clear as FFmpeg closes, uploads update until done, activity logs stream shutdown logs.
- Main loop: keeps rendering and draining `event_rx` until `orch_handle.is_finished()`, force quit, or 10-second timeout.

## Global Constraints

- **No Direct Terminal Pollution**: Never use `println!`, `eprintln!`, or unredirected subprocess outputs while the TUI is active.
- **Terminal Panic Recovery**: Preserve panic hook in `src/main.rs`.
- **MPEG-TS Stream Copy**: Preserve lossless FFmpeg stream copy flags (`-c copy`).
- **Path Portability**: Use `app_path::resolve_path(...)` for relative file paths.
- **Test Invariants**: Filesystem mutations in tests must operate strictly within `std::env::temp_dir()`.

---

### Task 1: Add Shutdown State and Key Transition to App

**Files:**
- Modify: `src/tui/app.rs:26-66`, `src/tui/app.rs:215-236`
- Test: `tests/test_tui_state.rs`

**Interfaces:**
- Produces: `app.is_shutting_down: bool` on `App`.
- Consumes: `AppEvent::Key(KeyCode::Char('q'))`.

- [ ] **Step 1: Write the failing unit tests**

Add `test_app_quit_two_stage_shutdown` to `tests/test_tui_state.rs`:

```rust
#[test]
fn test_app_quit_two_stage_shutdown() {
    let mut app = App::new();
    assert!(!app.is_shutting_down);
    assert!(!app.should_quit);

    // First 'q' press triggers shutdown mode, not immediate quit
    let q_key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(q_key));
    assert!(app.is_shutting_down);
    assert!(!app.should_quit);

    // Second 'q' press while in shutdown mode forces immediate quit
    app.handle_event(AppEvent::Key(q_key));
    assert!(app.is_shutting_down);
    assert!(app.should_quit);
}
```

Update `test_app_keyboard_navigation_and_quit` in `tests/test_tui_state.rs` to reflect the two-stage quit (first `q` sets `is_shutting_down`, second `q` sets `should_quit`).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_tui_state test_app_quit_two_stage_shutdown`
Expected: FAIL (`no field is_shutting_down on type App`)

- [ ] **Step 3: Implement minimal code in `src/tui/app.rs`**

In `src/tui/app.rs`:
1. Add `pub is_shutting_down: bool` to `struct App`.
2. Initialize `is_shutting_down: false` in `App::new()`.
3. In `handle_event` under `AppEvent::Key`:
   ```rust
   KeyCode::Char('q') => {
       if self.is_shutting_down {
           self.should_quit = true;
       } else {
           self.is_shutting_down = true;
       }
   }
   ```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_tui_state`
Expected: PASS (all 13 tests pass)

- [ ] **Step 5: Commit**

```bash
git add src/tui/app.rs tests/test_tui_state.rs
git commit -m "feat(tui): add two-stage shutdown state to App"
```

---

### Task 2: Render Shutdown Status in Header and Footer

**Files:**
- Modify: `src/tui/ui.rs:22-38`, `src/tui/ui.rs:278-284`
- Test: `tests/test_tui_state.rs`

**Interfaces:**
- Consumes: `app.is_shutting_down: bool`.
- Produces: Updated Ratatui header paragraph and footer widget rendering.

- [ ] **Step 1: Write the failing unit tests**

Add `test_draw_ui_shutdown_banner_rendering` to `tests/test_tui_state.rs`:

```rust
#[test]
fn test_draw_ui_shutdown_banner_rendering() {
    let backend = TestBackend::new(120, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    app.reclaimed_mb = 42.5;
    app.uploaded_count = 5;

    // 1. Normal state rendering
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_normal: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(content_normal.contains("chzzk-load v0.1.0"));
    assert!(content_normal.contains("[q] Quit"));
    assert!(!content_normal.contains("[ SHUTTING DOWN ]"));

    // 2. Shutdown state rendering
    app.is_shutting_down = true;
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_shutdown: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(content_shutdown.contains("[ SHUTTING DOWN ]"));
    assert!(content_shutdown.contains("Stopping recordings & finishing uploads..."));
    assert!(content_shutdown.contains("Reclaimed: 42.5 MB"));
    assert!(content_shutdown.contains("[q / Ctrl+C] Force Exit Immediately"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_tui_state test_draw_ui_shutdown_banner_rendering`
Expected: FAIL (shutdown text not found)

- [ ] **Step 3: Implement minimal code in `src/tui/ui.rs`**

In `src/tui/ui.rs`:
Update Header block:
```rust
    // Header
    let (header_text, header_style, border_style) = if app.is_shutting_down {
        (
            format!(
                " [ SHUTTING DOWN ] Stopping recordings & finishing uploads... │ Reclaimed: {:.1} MB │ Uploads: {} ",
                app.reclaimed_mb, app.uploaded_count
            ),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
            Style::default().fg(Color::Yellow),
        )
    } else {
        (
            format!(
                " chzzk-load v0.1.0 │ Reclaimed Space: {:.1} MB │ Chunks Uploaded: {}",
                app.reclaimed_mb, app.uploaded_count
            ),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            Style::default(),
        )
    };

    let header = Paragraph::new(header_text)
        .style(header_style)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(border_style),
        );
    f.render_widget(header, chunks[0]);
```

Update Footer block:
```rust
    // Footer
    let (footer_text, footer_style) = if app.is_shutting_down {
        (
            " [q / Ctrl+C] Force Exit Immediately │ Cleaning up: stopping FFmpeg & flushing uploads... ",
            Style::default()
                .fg(Color::LightRed)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        (
            " [q] Quit   [↑/↓] Channel   [PgUp/PgDn] Scroll Logs   [Home/End] Top/Latest   [r] Refresh ",
            Style::default().fg(Color::Yellow),
        )
    };
    let footer = Paragraph::new(footer_text).style(footer_style);
    f.render_widget(footer, chunks[3]);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_tui_state`
Expected: PASS (all tests pass)

- [ ] **Step 5: Commit**

```bash
git add src/tui/ui.rs tests/test_tui_state.rs
git commit -m "feat(tui): render visual cleanup banner and footer during shutdown"
```

---

### Task 3: Integrate TUI Render Loop with Background Engine Cleanup

**Files:**
- Modify: `src/main.rs:130-183`
- Test: `cargo test`

**Interfaces:**
- Consumes: `orch_handle.is_finished()`, `cancel_token`, `app.is_shutting_down`, `app.should_quit`.
- Produces: Non-blocking interactive render loop until background engine shutdown finishes.

- [ ] **Step 1: Inspect current render loop in `src/main.rs`**

Currently, `main.rs` exits the loop as soon as `cancel_token.is_cancelled() || app.should_quit`. We want it to stay in the loop, continuing to render frames and handle `event_rx`, until:
1. `orch_handle.is_finished()` is true (cleanup complete!), OR
2. `app.should_quit` is true (second `q` press or force-exit), OR
3. A 10-second timeout after initiating shutdown expires.

- [ ] **Step 2: Update `main.rs` loop logic**

In `src/main.rs`:
Track `let mut shutdown_start: Option<std::time::Instant> = None;` before the loop.

Inside the loop:
1. Check if shutdown was requested via Ctrl+C:
   ```rust
   if cancel_token.is_cancelled() && !app.is_shutting_down {
       app.is_shutting_down = true;
       shutdown_start = Some(std::time::Instant::now());
   }
   ```
2. Check termination conditions:
   ```rust
   if app.should_quit {
       break;
   }
   if app.is_shutting_down {
       if orch_handle.is_finished() {
           break;
       }
       if let Some(start) = shutdown_start {
           if start.elapsed() >= Duration::from_secs(10) {
               break;
           }
       }
   }
   ```
3. When `KeyCode::Char('q')` is pressed:
   ```rust
   if key.code == KeyCode::Char('q') {
       if app.is_shutting_down {
           break;
       } else {
           app.is_shutting_down = true;
           cancel_token.cancel();
           shutdown_start = Some(std::time::Instant::now());
       }
   } else {
       app.handle_event(AppEvent::Key(key));
   }
   ```
4. After loop ends:
   ```rust
   // Restore terminal
   let _ = disable_raw_mode();
   let _ = crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen, Show);

   // If orch_handle is still running (e.g. forced exit or safety timeout), await with short grace period
   if !orch_handle.is_finished() {
       let _ = tokio::time::timeout(Duration::from_secs(2), orch_handle).await;
   }
   ```

- [ ] **Step 3: Run the full test suite**

Run: `cargo test`
Expected: All 20 tests pass.

- [ ] **Step 4: Verify formatting and linter**

Run: `cargo fmt --check`
Run: `cargo clippy --all-targets -- -D warnings`
Run: `node scripts/test-npm-packages.js`
Expected: All checks pass with 0 errors / warnings.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat(tui): maintain live dashboard and logs rendering during engine cleanup"
```

---

### Task 4: Release Build and Smoke Test

**Files:**
- Target: `target/release/chzzk-load.exe`

- [ ] **Step 1: Compile release binary**

Run: `cargo build --release`
Expected: Build finishes with exit code 0.

- [ ] **Step 2: Smoke test CLI**

Run: `target/release/chzzk-load.exe --help`
Expected: Help output displayed.
