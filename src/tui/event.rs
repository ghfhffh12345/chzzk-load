use crossterm::event::KeyEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogKind {
    Info,
    Warn,
    Error,
    Clean,
    Rec,
    Ffmpeg,
    Drive,
    Poll,
}

impl LogKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogKind::Info => "INFO",
            LogKind::Warn => "WARN",
            LogKind::Error => "ERROR",
            LogKind::Clean => "CLEAN",
            LogKind::Rec => "REC",
            LogKind::Ffmpeg => "FFMPEG",
            LogKind::Drive => "DRIVE",
            LogKind::Poll => "POLL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub kind: LogKind,
    pub message: String,
}

impl LogEntry {
    pub fn new(kind: LogKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn info(message: impl Into<String>) -> Self {
        Self::new(LogKind::Info, message)
    }

    pub fn warn(message: impl Into<String>) -> Self {
        Self::new(LogKind::Warn, message)
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::new(LogKind::Error, message)
    }

    pub fn clean(message: impl Into<String>) -> Self {
        Self::new(LogKind::Clean, message)
    }

    pub fn rec(message: impl Into<String>) -> Self {
        Self::new(LogKind::Rec, message)
    }

    pub fn ffmpeg(message: impl Into<String>) -> Self {
        Self::new(LogKind::Ffmpeg, message)
    }

    pub fn drive(message: impl Into<String>) -> Self {
        Self::new(LogKind::Drive, message)
    }

    pub fn poll(message: impl Into<String>) -> Self {
        Self::new(LogKind::Poll, message)
    }

    pub fn contains(&self, pat: &str) -> bool {
        self.to_string().contains(pat)
    }

    pub fn starts_with(&self, pat: &str) -> bool {
        self.to_string().starts_with(pat)
    }

    fn matches_str(&self, s: &str) -> bool {
        if self.message == s {
            return true;
        }
        if let Some(rest) = s.strip_prefix('[')
            && let Some((tag, msg)) = rest.split_once(']')
        {
            return tag.trim() == self.kind.as_str() && msg.trim_start() == self.message;
        }
        false
    }
}

impl std::fmt::Display for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind.as_str(), self.message)
    }
}

impl<T: Into<String>> From<T> for LogEntry {
    fn from(s: T) -> Self {
        let text = s.into();
        if let Some(rest) = text.strip_prefix('[')
            && let Some((tag, msg)) = rest.split_once(']')
        {
            let kind = match tag.trim() {
                "ERROR" => LogKind::Error,
                "WARN" => LogKind::Warn,
                "CLEAN" => LogKind::Clean,
                "REC" => LogKind::Rec,
                "FFMPEG" => LogKind::Ffmpeg,
                "DRIVE" => LogKind::Drive,
                "POLL" => LogKind::Poll,
                _ => LogKind::Info,
            };
            return Self {
                kind,
                message: msg.trim_start().to_string(),
            };
        }
        Self {
            kind: LogKind::Info,
            message: text,
        }
    }
}

impl PartialEq<str> for LogEntry {
    fn eq(&self, other: &str) -> bool {
        self.matches_str(other)
    }
}

impl PartialEq<&str> for LogEntry {
    fn eq(&self, other: &&str) -> bool {
        self.matches_str(other)
    }
}

impl PartialEq<String> for LogEntry {
    fn eq(&self, other: &String) -> bool {
        self.matches_str(other)
    }
}

impl PartialEq<LogEntry> for str {
    fn eq(&self, other: &LogEntry) -> bool {
        other == self
    }
}

impl PartialEq<LogEntry> for &str {
    fn eq(&self, other: &LogEntry) -> bool {
        other == self
    }
}

impl PartialEq<LogEntry> for String {
    fn eq(&self, other: &LogEntry) -> bool {
        other == self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AppEvent {
    Tick,
    Key(KeyEvent),
    ChannelUpdate {
        channel_id: String,
        channel_name: String,
        is_live: bool,
        title: String,
    },
    RecordingStarted {
        channel_id: String,
        session_title: String,
    },
    RecordingEnded {
        channel_id: String,
    },
    ChunkSealed {
        chunk_name: String,
        size_bytes: u64,
    },
    UploadProgress {
        channel_id: String,
        chunk_name: String,
        streamer_name: String,
        uploaded_bytes: u64,
        total_bytes: u64,
        speed_mb_s: f64,
    },
    UploadCompleted {
        channel_id: String,
        chunk_name: String,
        reclaimed_bytes: u64,
    },
    UploadFailed {
        channel_id: String,
        chunk_name: String,
    },
    Log(LogEntry),
}

impl AppEvent {
    pub fn log(kind: LogKind, message: impl Into<String>) -> Self {
        AppEvent::Log(LogEntry::new(kind, message))
    }
}

impl From<LogEntry> for AppEvent {
    fn from(entry: LogEntry) -> Self {
        AppEvent::Log(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_kind_as_str() {
        assert_eq!(LogKind::Info.as_str(), "INFO");
        assert_eq!(LogKind::Warn.as_str(), "WARN");
        assert_eq!(LogKind::Error.as_str(), "ERROR");
        assert_eq!(LogKind::Clean.as_str(), "CLEAN");
        assert_eq!(LogKind::Rec.as_str(), "REC");
        assert_eq!(LogKind::Ffmpeg.as_str(), "FFMPEG");
        assert_eq!(LogKind::Drive.as_str(), "DRIVE");
        assert_eq!(LogKind::Poll.as_str(), "POLL");
    }

    #[test]
    fn test_log_entry_constructors() {
        assert_eq!(LogEntry::info("msg"), LogEntry::new(LogKind::Info, "msg"));
        assert_eq!(LogEntry::warn("msg"), LogEntry::new(LogKind::Warn, "msg"));
        assert_eq!(LogEntry::error("msg"), LogEntry::new(LogKind::Error, "msg"));
        assert_eq!(LogEntry::clean("msg"), LogEntry::new(LogKind::Clean, "msg"));
        assert_eq!(LogEntry::rec("msg"), LogEntry::new(LogKind::Rec, "msg"));
        assert_eq!(
            LogEntry::ffmpeg("msg"),
            LogEntry::new(LogKind::Ffmpeg, "msg")
        );
        assert_eq!(LogEntry::drive("msg"), LogEntry::new(LogKind::Drive, "msg"));
        assert_eq!(LogEntry::poll("msg"), LogEntry::new(LogKind::Poll, "msg"));
    }

    #[test]
    fn test_log_entry_display_and_parsing() {
        let entry = LogEntry::clean("Uploaded & deleted chunk_0000.ts");
        assert_eq!(entry.kind, LogKind::Clean);
        assert_eq!(
            entry.to_string(),
            "[CLEAN] Uploaded & deleted chunk_0000.ts"
        );

        let from_str: LogEntry = "[FFMPEG] frame= 100 fps=30".into();
        assert_eq!(from_str.kind, LogKind::Ffmpeg);
        assert_eq!(from_str.message, "frame= 100 fps=30");

        let unformatted: LogEntry = "Generic message".into();
        assert_eq!(unformatted.kind, LogKind::Info);
        assert_eq!(unformatted.message, "Generic message");
    }

    #[test]
    fn test_log_entry_from_all_tags() {
        let cases = [
            ("[INFO] hello", LogKind::Info, "hello"),
            ("[WARN] warning message", LogKind::Warn, "warning message"),
            ("[ERROR] failed", LogKind::Error, "failed"),
            ("[CLEAN] deleted", LogKind::Clean, "deleted"),
            ("[REC] chunk sealed", LogKind::Rec, "chunk sealed"),
            ("[FFMPEG] stderr output", LogKind::Ffmpeg, "stderr output"),
            ("[DRIVE] uploading", LogKind::Drive, "uploading"),
            ("[POLL] checking status", LogKind::Poll, "checking status"),
            (
                "[UNKNOWN] fallback to info",
                LogKind::Info,
                "fallback to info",
            ),
        ];

        for (input, expected_kind, expected_msg) in cases {
            let entry: LogEntry = input.into();
            assert_eq!(entry.kind, expected_kind);
            assert_eq!(entry.message, expected_msg);
        }
    }

    #[test]
    fn test_log_entry_string_comparisons_and_helpers() {
        let entry = LogEntry::clean("Uploaded & deleted chunk_0000.ts");
        assert_eq!(entry, "[CLEAN] Uploaded & deleted chunk_0000.ts");
        assert_eq!(entry, "Uploaded & deleted chunk_0000.ts");
        assert_eq!("[CLEAN] Uploaded & deleted chunk_0000.ts", entry);
        assert_eq!("Uploaded & deleted chunk_0000.ts", entry);
        assert!(entry.contains("chunk_0000.ts"));
        assert!(entry.starts_with("[CLEAN]"));
    }

    #[test]
    fn test_app_event_log_helper() {
        let event = AppEvent::log(LogKind::Rec, "recording started");
        match event {
            AppEvent::Log(entry) => {
                assert_eq!(entry.kind, LogKind::Rec);
                assert_eq!(entry.message, "recording started");
            }
            _ => panic!("Expected AppEvent::Log"),
        }
    }
}
