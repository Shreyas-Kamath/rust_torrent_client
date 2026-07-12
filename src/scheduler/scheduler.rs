use std::{collections::HashSet, net::SocketAddr};

use bitvec::vec::BitVec;
use slab::Slab;
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::{
    peers::{
        Peer, PeerConnection,
        commands::{PeerCommand, PeerEvent},
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

    pub async fn run(
        mut self,
        mut rx: Receiver<SchedulerEvent>,
        info_hash: [u8; 20],
    ) -> tokio::io::Result<()> {
        let (peer_tx, mut peer_rx) = mpsc::channel::<PeerEvent>(1000);

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
                    PeerEvent::Connect { stream, peer } => {
                        self.existing_peers.insert(peer.addr);

                        let (tx, rx) = mpsc::channel::<PeerCommand>(5);
                        let peer_handle = PeerHandle::new(tx, self.num_pieces, peer.addr);
                        let unique_id = self.slots.insert(peer_handle);
                        let conn =
                            PeerConnection::new(stream, peer_tx.clone(), unique_id, peer.id, info_hash);
                        tokio::spawn(conn.run(rx));
                    },
                    PeerEvent::Data { data } => { todo!(); }
                    PeerEvent::Disconnect { id } => {
                        self.remove_peer(id);
                    }
                    PeerEvent::RequestingBlocks { num } => { todo!(); }
                    PeerEvent::Bitfield => { todo!(); }
                    PeerEvent::Have { piece } => { todo!(); }
                }
            }
        }

        Ok(())
    }

    async fn dedup_and_start_peers(&mut self, list: Vec<Peer>, peer_tx: Sender<PeerEvent>) {
        for peer in list {
            if self.existing_peers.contains(&peer.addr) {
                continue;
            }

            let peer_tx_cloned = peer_tx.clone();

            tokio::spawn(async move {
                match TcpStream::connect(peer.addr).await {
                    Ok(stream) => {
                        peer_tx_cloned
                            .send(PeerEvent::Connect { stream, peer })
                            .await
                            .ok();
                    }
                    Err(_) => {
                        return;
                    }
                }
            });
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
