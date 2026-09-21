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
    Log(String),
}
