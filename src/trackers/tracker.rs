use crate::session::commands::SessionEvent;
use crate::trackers::{Tracker, TrackerCommand};
use crate::trackers::{http::HTTPTracker, udp::UDPTracker};
use tokio::sync::mpsc;

pub const DEFAULT_ANNOUNCE_TIMER: u64 = 180;

impl Tracker {
    pub fn new(
        url: String,
        http_client: reqwest::Client,
        info_hash: [u8; 20],
        sender: mpsc::Sender<SessionEvent>,
    ) -> Self {
        if url.starts_with("udp://") {
            Self::UDP(UDPTracker::new(url, info_hash, sender))
        } else {
            Self::HTTP(HTTPTracker::new(url, http_client, info_hash, sender))
        }
    }

    pub async fn run(self, rx: mpsc::Receiver<TrackerCommand>) {
        match self {
            Tracker::UDP(tracker) => tracker.run(rx).await.expect("UDP tracker went wrong"),
            Tracker::HTTP(tracker) => tracker.run(rx).await.expect("HTTP tracker went wrong"),
        }
    }
}
