// use tokio::{sync::mpsc};
// use crate::trackers::{commands::{Tracker, TrackerCommand, TrackerResponse}, tracker::DEFAULT_ANNOUNCE_TIMER};

// pub struct UDPTracker {
//     url: String,
//     connection_id: u64,
//     transaction_id: u32,
//     info_hash: [u8; 20]
// }

// impl UDPTracker {
//     pub fn new(url: String, info_hash: [u8; 20]) -> UDPTracker {
//         UDPTracker {
//             url,
//             connection_id: 0,
//             transaction_id: 0,
//             info_hash
//         }
//     }

//     async fn announce(&mut self) -> tokio::io::Result<TrackerResponse> {

//     }

//     pub async fn run(self, mut rx: mpsc::Receiver<TrackerCommand>) -> tokio::io::Result<()> {
//         let mut timer = tokio::time::interval(tokio::time::Duration::from_secs(DEFAULT_ANNOUNCE_TIMER));

//         match self.announce().await {
//             Ok(result) => {
//                 let peers = parse_peers(result.peers);

//             }
//             Err(e) =>
//         }

//     loop {
//         tokio::select! {
//             Some(cmd) = rx.recv() => {
//                 match cmd {
//                     TrackerCommand::Shutdown => break,

//                     TrackerCommand::ForceReannounce => {
//                         self.announce().await?;
//                     }
//                 }
//             }

//             _ = timer.tick() => {
//                 self.announce().await?;
//             }
//         }
//     }
//         Ok(())
//     }
// }
