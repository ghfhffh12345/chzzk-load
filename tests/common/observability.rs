#![allow(dead_code)]

use chzzk_load::tui::event::AppEvent;
use tokio::sync::mpsc;

/// Non-blockingly drains all pending log messages from an `AppEvent` receiver.
pub fn drain_buffered_logs(rx: &mut mpsc::Receiver<AppEvent>) -> Vec<String> {
    let mut logs = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if let AppEvent::Log(entry) = event {
            logs.push(entry.to_string());
        }
    }
    logs
}

/// Formats a slice of captured logs with item count for diagnostic output in assertions.
pub fn format_captured_logs(logs: &[String]) -> String {
    if logs.is_empty() {
        "Captured logs: <none>".to_string()
    } else {
        format!("Captured logs ({}):\n{}", logs.len(), logs.join("\n"))
    }
}

/// A lightweight recorder that tracks log messages across async test event loops.
#[derive(Debug, Default, Clone)]
pub struct TestLogRecorder {
    logs: Vec<String>,
}

impl TestLogRecorder {
    pub fn new() -> Self {
        Self { logs: Vec::new() }
    }

    /// Records an event if it contains log entries or telemetry.
    pub fn record(&mut self, event: &AppEvent) {
        if let AppEvent::Log(entry) = event {
            self.logs.push(entry.to_string());
        }
    }

    /// Drains any remaining buffered logs from `rx` into this recorder.
    pub fn drain_buffered(&mut self, rx: &mut mpsc::Receiver<AppEvent>) {
        self.logs.extend(drain_buffered_logs(rx));
    }

    /// Returns all captured logs.
    pub fn logs(&self) -> &[String] {
        &self.logs
    }

    /// Formats all captured logs for diagnostic output.
    pub fn summary(&self) -> String {
        format_captured_logs(&self.logs)
    }
}

/// Asserts that an Option is Some, or panics with an informative message
/// containing all buffered logs from the event receiver (and optional recorder).
pub fn expect_with_logs<T>(
    opt: Option<T>,
    msg: &str,
    rx: &mut mpsc::Receiver<AppEvent>,
    recorder: Option<&TestLogRecorder>,
) -> T {
    match opt {
        Some(val) => val,
        None => {
            let mut logs = recorder.map(|r| r.logs().to_vec()).unwrap_or_default();
            logs.extend(drain_buffered_logs(rx));
            panic!("{}. {}", msg, format_captured_logs(&logs));
        }
    }
}

/// Asserts that a condition is true, or panics with an informative message
/// containing all buffered logs from the event receiver (and optional recorder).
pub fn assert_with_logs(
    cond: bool,
    msg: &str,
    rx: &mut mpsc::Receiver<AppEvent>,
    recorder: Option<&TestLogRecorder>,
) {
    if !cond {
        let mut logs = recorder.map(|r| r.logs().to_vec()).unwrap_or_default();
        logs.extend(drain_buffered_logs(rx));
        panic!("{}. {}", msg, format_captured_logs(&logs));
    }
}

/// Asserts that a Result is Ok, or panics with an informative message
/// containing all buffered logs from the event receiver (and optional recorder).
pub fn unwrap_with_logs<T, E: std::fmt::Debug>(
    res: Result<T, E>,
    msg: &str,
    rx: &mut mpsc::Receiver<AppEvent>,
    recorder: Option<&TestLogRecorder>,
) -> T {
    match res {
        Ok(val) => val,
        Err(err) => {
            let mut logs = recorder.map(|r| r.logs().to_vec()).unwrap_or_default();
            logs.extend(drain_buffered_logs(rx));
            panic!("{}: {:?}. {}", msg, err, format_captured_logs(&logs));
        }
    }
}

fn process_event_for_match(
    event: &AppEvent,
    expected_substr: &str,
    captured: &mut Vec<String>,
) -> bool {
    match event {
        AppEvent::Log(entry) => {
            captured.push(entry.to_string());
            entry.contains(expected_substr)
        }
        other => {
            captured.push(format!("<non-log event: {:?}>", other));
            false
        }
    }
}

/// Non-blockingly drains events from `rx` and asserts that at least one log entry
/// contains `expected_substr`. If not found, panics with an informative message
/// formatting all drained logs.
pub fn assert_log_emitted(rx: &mut mpsc::Receiver<AppEvent>, expected_substr: &str) {
    let mut captured = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if process_event_for_match(&event, expected_substr, &mut captured) {
            return;
        }
    }
    panic!(
        "Expected log containing '{}', but not found. {}",
        expected_substr,
        format_captured_logs(&captured)
    );
}

/// Awaits events from `rx` until a log entry matching `expected_substr` is received,
/// or `timeout` expires. If not found, panics with an informative message formatting all captured logs.
pub async fn assert_log_emitted_timeout(
    rx: &mut mpsc::Receiver<AppEvent>,
    expected_substr: &str,
    timeout: std::time::Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut captured = Vec::new();
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline - now;
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(event)) => {
                if process_event_for_match(&event, expected_substr, &mut captured) {
                    return;
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    panic!(
        "Timed out waiting for log containing '{}' (timeout: {:?}). {}",
        expected_substr,
        timeout,
        format_captured_logs(&captured)
    );
}
