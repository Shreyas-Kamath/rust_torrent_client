use crate::{parser::commands::Torrent, peers::Peer, trackers::commands::TrackerCommand};

// on success, report back a vector of parsed peers, and an optional warning
// on failure, report back an error
pub enum SessionEvent {
    AnnounceSuccess {
        peers: Vec<Peer>,
        warning: Option<String>,
    },

    AnnounceFailure {
        error: String,
    },
}

// Structural representation of a Session holding the torrent, trackers, pieces, disk, and a lightweight HTTP client
// for HTTP(S) trackers to use.
pub struct Session {
    pub torrent: Torrent,
    pub trackers: Vec<tokio::sync::mpsc::Sender<TrackerCommand>>,
    pub http_client: reqwest::Client,
}
