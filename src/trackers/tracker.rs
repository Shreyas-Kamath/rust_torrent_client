use crate::session::commands::SessionEvent;
use crate::trackers::commands::{Tracker, TrackerCommand};
use crate::trackers::http::HTTPTracker;
use tokio::sync::mpsc;
// use crate::trackers::udp::UDPTracker;

pub const DEFAULT_ANNOUNCE_TIMER: u64 = 180;

impl Tracker {
    pub fn new(
        url: String,
        http_client: reqwest::Client,
        info_hash: [u8; 20],
        sender: mpsc::Sender<SessionEvent>,
    ) -> Self {
        // if url.starts_with("udp://") {

        //     Self::UDP(UDPTracker::new(url, info_hash))
        // }
        // else {
        Self::HTTP(HTTPTracker::new(url, http_client, info_hash, sender))
        // }
    }

    pub async fn run(self, rx: mpsc::Receiver<TrackerCommand>) {
        match self {
            // Tracker::UDP(tracker) => tracker.run(rx).await,
            Tracker::HTTP(tracker) => tracker.run(rx).await.expect("Something went wrong"),
        }
    }
}
