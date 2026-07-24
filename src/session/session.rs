use crate::client::SessionCommand;
use crate::disk::DiskEvent;
use crate::parser::Torrent;
use crate::scheduler::{Scheduler, SchedulerEvent};
use crate::session::{Session, SessionEvent};
use crate::trackers::{Tracker, TrackerCommand};

use std::collections::HashSet;
use tokio::sync::mpsc;

impl Session {
    pub fn new(torrent: Torrent, http_client: reqwest::Client) -> Self {
        Self {
            torrent,
            trackers: Vec::new(),
            http_client,
        }
    }

    // launch fire and forget tasks for trackers, with channels sent in for communication between tasks
    async fn spawn_trackers(&mut self, sender: mpsc::Sender<SessionEvent>) {
        for url in get_tracker_urls(
            &self.torrent.announce,
            self.torrent.announce_list.as_deref(),
        ) {
            let (tx, rx) = tokio::sync::mpsc::channel::<TrackerCommand>(32);

            let tracker = Tracker::new(
                url.clone(),
                self.http_client.clone(),
                self.torrent.info_hash,
                sender.clone(),
            );
            self.trackers.push(tx);
            tokio::spawn(tracker.run(rx));
        }
    }

    async fn handle_command(&mut self, cmd: SessionCommand) {}

    pub async fn run(
        mut self,
        mut rx: tokio::sync::mpsc::Receiver<SessionCommand>,
    ) -> tokio::io::Result<()> {
        // channels
        let (tracker_resp_sender, mut tracker_resp_receiver) = mpsc::channel(32);
        let (scheduler_event_sender, scheduler_event_receiver) =
            mpsc::channel::<SchedulerEvent>(32);
        let (disk_tx, disk_rx) = mpsc::channel::<DiskEvent>(1000);

        // spawn the scheduler
        let piece_hashes: Vec<[u8; 20]> = self
            .torrent
            .info
            .pieces
            .chunks_exact(20)
            .map(|chunk| chunk.try_into().unwrap())
            .collect();
        let total_pieces = piece_hashes.len();
        let scheduler = Scheduler::new(
            total_pieces,
            self.torrent.total_length,
            self.torrent.info.piece_length,
            disk_tx,
        );

        let file_list = std::mem::take(&mut self.torrent.info.files);
        tokio::spawn(scheduler.run(
            self.torrent.info.name.clone(),
            scheduler_event_receiver,
            self.torrent.info_hash,
            piece_hashes,
            file_list,
            disk_rx,
        ));

        self.spawn_trackers(tracker_resp_sender.clone()).await;

        loop {
            tokio::select! {
                Some(cmd) = tracker_resp_receiver.recv() => {
                    match cmd {
                        SessionEvent::AnnounceSuccess { peers, warning } => {
                            if let Some(warning) = warning {
                                println!("Warning: {warning}");
                            }
                            scheduler_event_sender.send(SchedulerEvent::LaunchPeers { peers }).await.ok();
                        },

                        SessionEvent::AnnounceFailure { error } => {
                            println!("Error: {error}");
                        }
                    }
                }
                Some(cmd) = rx.recv() => {
                    match cmd {
                        SessionCommand::IncomingPeer { stream, peer } => {
                            let _ = scheduler_event_sender.send(SchedulerEvent::IncomingPeer { stream, peer }).await;
                        }
                        _ => {
                            println!("Not implemented yet");
                        }
                    }
                }
            }
        }
    }
}

// get tracker URLs from the TorrentResponse struct and deduplicate them using a HashSet, and return it.
fn get_tracker_urls(announce: &str, list: Option<&[Vec<String>]>) -> HashSet<String> {
    let mut set: HashSet<String> = HashSet::new();
    set.insert(announce.to_string());

    if let Some(list) = list {
        for tier in list {
            for url in tier {
                set.insert(url.to_string());
            }
        }
    }

    set
}
