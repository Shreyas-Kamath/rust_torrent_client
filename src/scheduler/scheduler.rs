use std::{collections::HashSet, net::SocketAddr};

use bitvec::vec::BitVec;
use slab::Slab;
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::{
    disk::{Disk, DiskEvent, DiskResponse}, parser::File, peers::{
        Peer, commands::{PeerCommand, PeerEvent}, run_peer,
    }, scheduler::{BlockRequest, BlockState, PeerHandle, PieceBuffer, Scheduler, SchedulerEvent},
};

const BLOCK_SIZE: u32 = 16384;
const MAX_IN_FLIGHT: u32 = 32;

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

    fn complete_and_cleanup(&mut self) {
        // only reset memory, not state (perhaps blocks received too)
        self.block_status.clear();
        self.block_status.shrink_to_fit();

        // profile this, it should already be empty because data has been moved
        self.data.clear();
        self.data.shrink_to_fit();
    }

    fn reset(&mut self) {
        // full reset
        self.data.clear();
        self.block_status.clear();
        self.blocks_received = 0;
        self.complete = false;
    }
}

impl Scheduler {
    pub fn new(
        total_pieces: usize,
        total_len: u64,
        piece_len: u64,
        disk_event_sender: Sender<DiskEvent>,
    ) -> Self {
        Self {
            existing_peers: HashSet::new(),
            slots: Slab::with_capacity(1000),
            num_pieces: total_pieces,
            pieces: vec![PieceBuffer::default(); total_pieces],
            total_len,
            piece_len,
            disk_event_sender,
        }
    }

    pub async fn run(
        mut self,
        torrent_name: String,
        mut rx: Receiver<SchedulerEvent>,
        info_hash: [u8; 20],
        piece_hashes: Vec<[u8; 20]>,
        file_list: Option<Vec<File>>,
        disk_event_receiver: Receiver<DiskEvent>,
    ) -> tokio::io::Result<()> {

        let (peer_tx, mut peer_rx) = mpsc::channel::<PeerEvent>(2000);
        let (disk_response_tx, mut disk_response_rx) = mpsc::channel::<DiskResponse>(1000);

        let disk = Disk::new(
            torrent_name,
            self.total_len,
            self.piece_len,
            piece_hashes,
            disk_response_tx,
        );
        tokio::spawn(disk.run(file_list, disk_event_receiver));

        loop {
            tokio::select! {
                Some(cmd) = disk_response_rx.recv() => {
                    match cmd {
                        DiskResponse::CompletedPieces { bitfield } => { todo!(); }
                        DiskResponse::HashFailed { piece } => { self.handle_failure(piece); }
                        DiskResponse::WriteFailed { piece } => { self.handle_failure(piece); }
                        DiskResponse::PieceHashedAndWritten { piece } => { self.on_complete_piece(piece).await; }
                    }
                }

                // Session -> Scheduler
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

                //
                Some(cmd) = peer_rx.recv() => {
                    match cmd {
                        PeerEvent::Connect { stream, peer } => {
                            self.existing_peers.insert(peer.addr);

                            let (tx, rx) = mpsc::channel::<PeerCommand>(32);
                            let peer_handle = PeerHandle::new(tx, peer.addr);
                            let slot_id = self.slots.insert(peer_handle);
                            tokio::spawn(run_peer(slot_id, info_hash, stream, peer_tx.clone(), rx, self.num_pieces));
                        }

                        PeerEvent::ConnectFailed { peer } => {
                            self.existing_peers.remove(&peer.addr);
                        }

                        PeerEvent::Data { slot_id, piece, begin, data } => {
                            self.handle_data(slot_id, piece, begin, data).await;
                        }

                        PeerEvent::Disconnect { slot_id } => {
                            self.remove_peer(slot_id);
                        }

                        PeerEvent::Bitfield { slot_id, bitfield } => {
                            self.handle_bitfield(slot_id, bitfield).await;
                        }

                        PeerEvent::Have { slot_id, piece } => {
                            self.handle_have(slot_id, piece).await;
                        }

                        PeerEvent::Choke { slot_id } => {
                            self.handle_choke(slot_id);
                        }
                        PeerEvent::Unchoke { slot_id } => {
                            self.handle_unchoke(slot_id).await;
                        }
                        PeerEvent::Interested { slot_id } => {
                            self.handle_interested(slot_id);
                        }
                        PeerEvent::NotInterested { slot_id } => {
                            self.handle_not_interested(slot_id);
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
            let peer_event_tx = peer_tx.clone();

            tokio::spawn(async move {
                match TcpStream::connect(peer.addr).await {
                    Ok(stream) => {
                        peer_event_tx
                            .send(PeerEvent::Connect { stream, peer })
                            .await
                            .ok();
                    }
                    Err(_) => {
                        peer_event_tx
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

    fn piece_len_for_index(&self, index: usize) -> u64 {
        if index < self.num_pieces - 1 {
            self.piece_len
        } else {
            self.total_len - self.piece_len * (self.num_pieces as u64 - 1)
        }
    }

    async fn handle_have(&mut self, slot_id: usize, piece: u32) {
        let peer_handle = self.slots.get_mut(slot_id).unwrap();
        peer_handle.bitfield.set(piece as usize, true);

        let am_interested = &peer_handle.am_interested;
        
        if !am_interested {
            peer_handle.sender.send(PeerCommand::SendInterested).await;
        }

        self.maybe_push_blocks(slot_id).await;
    }

    fn handle_choke(&mut self, slot_id: usize) {
        let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");

        if !peer_handle.am_choked {
            peer_handle.am_choked = true;
        }
    }

    async fn handle_unchoke(&mut self, slot_id: usize)  {
        let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");

        if peer_handle.am_choked {
            peer_handle.am_choked = false;
            self.maybe_push_blocks(slot_id).await;
        }
    }

    fn handle_interested(&mut self, slot_id: usize) {
        let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");

        if !peer_handle.peer_interested {
            peer_handle.peer_interested = true;
        }
    }

    fn handle_not_interested(&mut self, slot_id: usize) {
        let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");

        if peer_handle.peer_interested {
            peer_handle.peer_interested = false;
        }
    }

    async fn handle_bitfield(&mut self, slot_id: usize, bitfield: BitVec) {
        let peer_handle = self.slots.get_mut(slot_id).unwrap();
        peer_handle.bitfield = bitfield;

        let am_interested = &peer_handle.am_interested;
        
        if !am_interested {
            peer_handle.sender.send(PeerCommand::SendInterested).await;
            peer_handle.am_interested = true;
        }

        // maybe request blocks here too?
    }

    async fn maybe_push_blocks(&mut self, slot_id: usize) {
        let (should_request, in_flight) = {
            let peer_handle = self.slots.get(slot_id).expect("Peer doesnt exist");
            (peer_handle.am_interested && !peer_handle.am_choked, peer_handle.in_flight)
        };
        if !should_request || in_flight > MAX_IN_FLIGHT / 2 { return; }

        let blocks = self.push_blocks(slot_id, MAX_IN_FLIGHT - in_flight);
        if !blocks.is_empty() {
            let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");
            let len = blocks.len() as u32;
            peer_handle.sender.send(PeerCommand::BlocksToDownload { blocks }).await;
            peer_handle.in_flight += len;
        }
    }

    fn push_blocks(&mut self, slot_id: usize, mut request: u32) -> Vec<BlockRequest> {
        let peer_bitfield = &self.slots.get(slot_id).expect("Peer doesnt exist").bitfield;
        let mut blocks: Vec<BlockRequest> = Vec::with_capacity(request as usize);

            for piece_index in 0..self.pieces.len() {
                if request == 0 {
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
                    if request == 0 {
                        break;
                    }

                    if piece_buff.block_status[block_index] == BlockState::NotRequested {
                        piece_buff.block_status[block_index] = BlockState::Requested;
                        blocks.push(BlockRequest {
                            piece_index: piece_index as u32,
                            offset: block_index as u32 * BLOCK_SIZE,
                            len: BLOCK_SIZE.min(len as u32 - (block_index) as u32 * BLOCK_SIZE),
                        });
                        request -= 1;
                    }
                }
            }
            blocks
    }

    async fn handle_data(&mut self, slot_id: usize, piece: u32, begin: u32, data: Vec<u8>) {
        let begin = begin as usize;
        let block = begin / BLOCK_SIZE as usize;

        let piece_buffer = &mut self.pieces[piece as usize];
        let block_status = &mut piece_buffer.block_status;

        if piece_buffer.complete {
            return;
        }
        if block_status[block] == BlockState::Received {
            return;
        }

        block_status[block] = BlockState::Received;
        piece_buffer.blocks_received += 1;

        piece_buffer.data[begin..begin + data.len() - 8].copy_from_slice(&data[8..]);

        if piece_buffer.blocks_received as usize == block_status.len() {
            piece_buffer.complete = true;

            // now hash and write
            let data = std::mem::take(&mut piece_buffer.data);
            self.disk_event_sender
                .send(DiskEvent::TryHashAndWrite { piece, data })
                .await
                .ok();
        }

        self.slots.get_mut(slot_id).expect("Peer doesnt exist").in_flight -= 1;
        self.maybe_push_blocks(slot_id).await;
    }

    async fn on_complete_piece(&mut self, piece: u32) {
        let piece_buffer = self.pieces.get_mut(piece as usize).unwrap();
        piece_buffer.complete_and_cleanup();
        println!("Piece {piece} is complete");

        for (_, peer_handle) in &self.slots {
            peer_handle
                .sender
                .send(PeerCommand::SendHave { piece })
                .await
                .ok();
        }
    }

    fn handle_failure(&mut self, piece: u32) {
        println!("Piece {piece} failed");
        let piece_buffer = self.pieces.get_mut(piece as usize).unwrap();
        piece_buffer.reset();
    }
}

impl PeerHandle {
    pub fn new(tx: Sender<PeerCommand>, peer_addr: SocketAddr) -> Self {
        Self {
            bitfield: BitVec::new(),
            sender: tx,
            addr: peer_addr,
            am_choked: true,
            am_interested: false,
            peer_choked: true,
            peer_interested: false,
            in_flight: 0
        }
    }
}
