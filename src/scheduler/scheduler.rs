use std::{
    collections::HashSet,
    net::SocketAddr,
};

use bitvec::vec::BitVec;
use slab::Slab;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    peers::{
        Peer,
        commands::{PeerCommand, PeerResponse},
    },
    scheduler::{PeerHandle, Scheduler, SchedulerEvent},
};

impl Scheduler {
    pub fn new(piece_hashes: Vec<[u8; 20]>, total_pieces: usize) -> Self {
        Self {
            existing_peers: HashSet::new(),
            slots: Slab::with_capacity(1000),
            num_pieces: total_pieces,
            pieces: Vec::with_capacity(total_pieces),
            piece_hashes,
        }
    }

    pub async fn run(mut self, mut rx: Receiver<SchedulerEvent>) -> tokio::io::Result<()> {
        let (peer_tx, mut peer_rx) = mpsc::channel::<PeerResponse>(1000);

        tokio::select! {
            Some(cmd) = rx.recv() => {
                match cmd {
                    SchedulerEvent::LaunchPeers { peers } => {
                        self.dedup_and_start_peers(peers, peer_tx.clone()).await;
                    }

                    // fetch stats
                    SchedulerEvent::FetchInfo => { todo!(); }

                    // kill all peers and remove (prepare for disconnect flood)
                    SchedulerEvent::ShutdownPeers => { todo!(); }
                }
            }

            Some(cmd) = peer_rx.recv() => {
                match cmd {
                    PeerResponse::Data { data } => { todo!(); }
                    PeerResponse::Disconnect { id } => {
                        self.remove_peer(id);
                    }
                    PeerResponse::RequestingBlocks { num } => { todo!(); }
                    PeerResponse::Bitfield => { todo!(); }
                    PeerResponse::Have { piece } => { todo!(); }
                }
            }
        }

        Ok(())
    }

    async fn dedup_and_start_peers(&mut self, list: Vec<Peer>, tx: Sender<PeerResponse>) {
        for peer in list {
            if self.existing_peers.contains(&peer.addr) {
                continue;
            }

            println!("{:?}", peer);
            let (tx, rx) = mpsc::channel::<PeerCommand>(5);
            let peer_handle = PeerHandle::new(tx, self.num_pieces, peer.addr);
            self.slots.insert(peer_handle);
        }
    }

    fn remove_peer(&mut self, unique_id: usize) {
        let removed = self
            .slots
            .try_remove(unique_id)
            .expect("Received removal event for unknown peer");

        self.existing_peers.remove(&removed.addr);
    }
}

impl PeerHandle {
    pub fn new(tx: Sender<PeerCommand>, total_pieces: usize, peer_addr: SocketAddr) -> Self {
        Self {
            bitfield: BitVec::with_capacity(total_pieces),
            sender: tx,
            addr: peer_addr,
        }
    }
}
