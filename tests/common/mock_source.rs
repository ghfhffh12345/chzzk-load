#![allow(dead_code)]

use chzzk_load::chzzk::models::{LiveDetail, LiveStreamInfo};
use chzzk_load::chzzk::models_metadata::StreamMetadataState;

pub fn make_open_detail(
    channel_id: &str,
    channel_name: &str,
    title: &str,
    live_id: u64,
    hls_url: &str,
) -> LiveDetail {
    LiveDetail::Open(LiveStreamInfo {
        channel_id: channel_id.to_string(),
        live_id: Some(live_id),
        streamer_name: channel_name.to_string(),
        title: title.to_string(),
        hls_url: hls_url.to_string(),
        chat_channel_id: Some(format!("chat_{channel_id}")),
        metadata: StreamMetadataState {
            live_id: Some(live_id),
            channel_id: channel_id.to_string(),
            channel_name: channel_name.to_string(),
            live_title: title.to_string(),
            ..Default::default()
        },
    })
}

pub fn make_close_detail(channel_name: Option<&str>) -> LiveDetail {
    LiveDetail::Close {
        streamer_name: channel_name.map(|s| s.to_string()),
    }
}

pub fn make_restricted_detail(
    channel_id: &str,
    channel_name: &str,
    title: &str,
    live_id: Option<u64>,
    adult: bool,
) -> LiveDetail {
    LiveDetail::Restricted {
        channel_id: channel_id.to_string(),
        live_id,
        streamer_name: channel_name.to_string(),
        title: title.to_string(),
        chat_channel_id: Some(format!("chat_{channel_id}")),
        adult,
    }
}
