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
    scheduler::{BlockRequest, BlockState, PeerHandle, PieceBuffer, Scheduler, SchedulerEvent},
};

const BLOCK_SIZE: u32 = 16384;

impl Default for PieceBuffer {
    fn default() -> Self {
        Self { data: Vec::new(), block_status: Vec::new(), blocks_received: 0, complete: false }
    }
}

impl PieceBuffer {
    fn lazy_init(&mut self, piece_len: u64) {
        if self.block_status.is_empty() {
            self.complete = false;
            self.data.resize(piece_len as usize, 0u8);
            let num_blocks = ((piece_len + (BLOCK_SIZE - 1) as u64) / BLOCK_SIZE as u64) as usize;
            self.block_status
                .resize_with(num_blocks, || BlockState::NotRequested);
        }
    }
}

impl Scheduler {
    pub fn new(
        piece_hashes: Vec<[u8; 20]>,
        total_pieces: usize,
        total_len: u64,
        piece_len: u64,
    ) -> Self {
        Self {
            existing_peers: HashSet::new(),
            slots: Slab::with_capacity(1000),
            num_pieces: total_pieces,
            pieces: vec![PieceBuffer::default(); total_pieces],
            piece_hashes,
            total_len,
            piece_len,
        }
    }

    pub async fn run(
        mut self,
        mut rx: Receiver<SchedulerEvent>,
        info_hash: [u8; 20],
    ) -> tokio::io::Result<()> {
        let (peer_tx, mut peer_rx) = mpsc::channel::<PeerEvent>(1000);

        loop {
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
                            let peer_handle = PeerHandle::new(tx, peer.addr);
                            let unique_id = self.slots.insert(peer_handle);
                            let conn =
                                PeerConnection::new(stream, peer_tx.clone(), unique_id, peer.id, info_hash, self.num_pieces);
                            tokio::spawn(conn.run(rx));
                        }

                        PeerEvent::ConnectFailed { peer } => {
                            self.existing_peers.remove(&peer.addr);
                        }

                        PeerEvent::Data { slot_id, data } => { todo!(); }

                        PeerEvent::Disconnect { slot_id } => {
                            self.remove_peer(slot_id);
                        }

                        PeerEvent::RequestingBlocks { slot_id, num } => {
                            let blocks = self.fetch_blocks_for_peer(slot_id, num);
                            self.slots.get(slot_id).unwrap().sender.send(PeerCommand::BlocksToDownload { blocks }).await.ok();
                        }

                        PeerEvent::Bitfield { slot_id, bitfield } => {
                            self.slots.get_mut(slot_id).unwrap().bitfield = bitfield;
                        }

                        PeerEvent::Have { slot_id, piece } => {
                            self.slots.get_mut(slot_id).unwrap().bitfield.set(piece as usize, true);
                        }
                    }
                }
            }
        }
    }

    async fn dedup_and_start_peers(&mut self, list: Vec<Peer>, peer_tx: Sender<PeerEvent>) {
        for peer in list {
            if self.existing_peers.contains(&peer.addr) {
                continue;
            }

            self.existing_peers.insert(peer.addr);
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
                        peer_tx_cloned
                            .send(PeerEvent::ConnectFailed { peer })
                            .await
                            .ok();
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

    // very naive
    fn fetch_blocks_for_peer(&mut self, slot_id: usize, mut num: u32) -> Option<Vec<BlockRequest>> {
        let mut blocks: Vec<BlockRequest> = Vec::with_capacity(num as usize);

        let peer_bitfield = &self.slots.get(slot_id).unwrap().bitfield;

        for piece_index in 0..self.pieces.len() {
            if num == 0 {
                break;
            }

            let len = self.piece_len_for_index(piece_index);
            let piece_buff = &mut self.pieces[piece_index];

            if piece_buff.complete {
                continue;
            }

            if *peer_bitfield.get(piece_index).unwrap() {
                piece_buff.lazy_init(len);
            }

            for block_index in 0..piece_buff.block_status.len() {
                if num == 0 {
                    break;
                }

                if piece_buff.block_status[block_index] == BlockState::NotRequested {
                    piece_buff.block_status[block_index] = BlockState::Requested;
                    blocks.push(BlockRequest {
                        piece_index: piece_index as u32,
                        offset: block_index as u32 * BLOCK_SIZE,
                        len: BLOCK_SIZE.min(len as u32 - (block_index) as u32 * BLOCK_SIZE),
                    });
                    println!("Handed block {block_index} to {slot_id}");
                    num -= 1;
                }
            }
        }

        if blocks.is_empty() {
            None
        } else {
            Some(blocks)
        }
    }

    fn piece_len_for_index(&self, index: usize) -> u64 {
        if index < self.num_pieces - 1 {
            return self.piece_len;
        } else {
            return self.total_len - self.piece_len * (self.num_pieces as u64 - 1);
        }
    }
}

impl PeerHandle {
    pub fn new(tx: Sender<PeerCommand>, peer_addr: SocketAddr) -> Self {
        Self {
            // see whats up with this? do we need to preallocate this?
            bitfield: BitVec::new(),
            sender: tx,
            addr: peer_addr,
        }
    }
}
