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
