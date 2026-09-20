use crossterm::event::KeyEvent;

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
    ChunkSealed {
        chunk_name: String,
        size_bytes: u64,
    },
    UploadProgress {
        chunk_name: String,
        uploaded_bytes: u64,
        total_bytes: u64,
        speed_mb_s: f64,
    },
    UploadCompleted {
        chunk_name: String,
        reclaimed_bytes: u64,
    },
    Log(String),
}
