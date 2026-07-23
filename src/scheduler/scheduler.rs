use std::{collections::HashSet, iter::zip, net::SocketAddr, time::Duration};

use bitvec::vec::BitVec;
use slab::Slab;
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, Sender},
    time,
};

use crate::{
    disk::{Disk, DiskEvent, DiskResponse},
    parser::File,
    peers::{
        Peer,
        commands::{PeerCommand, PeerEvent},
        run_peer,
    },
    scheduler::{
        BlockRequest, BlockState, InFlightBlock, PeerHandle, PieceBuffer, Scheduler, SchedulerEvent,
    },
};

const BLOCK_SIZE: u32 = 16384;
const MAX_IN_FLIGHT: u32 = 40;
const TIMEOUT_SCAN_INTERVAL: u32 = 5;
const REQUEST_TIMEOUT: u32 = 15;

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
            completed_pieces: 0,
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

        let mut watchdog: time::Interval = time::interval_at(
            time::Instant::now() + Duration::from_secs(20),
            Duration::from_secs(TIMEOUT_SCAN_INTERVAL.into()),
        );

        loop {
            tokio::select! {
                Some(cmd) = disk_response_rx.recv() => {
                    match cmd {
                        DiskResponse::CompletedPieces { bitfield } => { self.parse_completed_pieces(bitfield); }
                        DiskResponse::HashFailed { piece } => { self.handle_failure(piece); }
                        DiskResponse::WriteFailed { piece } => { self.handle_failure(piece); }
                        DiskResponse::PieceHashedAndWritten { piece } => { self.on_complete_piece(piece).await; }
                    }
                }

                // Session -> Scheduler
                Some(cmd) = rx.recv() => {
                    match cmd {
                        SchedulerEvent::LaunchPeers { peers } => {
                            // println!("Received {} peers", peers.len());
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

                            let (tx, rx) = mpsc::channel::<PeerCommand>(100);
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
                            // one last cleanup
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
                _ = watchdog.tick() => {
                    self.scan_timeouts();
                }
            }
        }
    }

    fn parse_completed_pieces(&mut self, bitfield: BitVec) {
        assert_eq!(bitfield.len(), self.pieces.len());

        for (bit, piece_buff) in zip(bitfield, &mut self.pieces) {
            if bit { 
                self.completed_pieces += 1;
                piece_buff.complete = true; 
            }
        }

        println!("Found {} completed pieces", self.completed_pieces);
    }

    fn scan_timeouts(&mut self) {
        let now = time::Instant::now();
        let timeout = Duration::from_secs(REQUEST_TIMEOUT as u64);

        for (_, peer_handle) in &mut self.slots {
            let inflight = &mut peer_handle.in_flight;
            let mut i = 0;

            while i < inflight.len() {
                if now.duration_since(inflight[i].sent_at)
                    >= timeout
                {
                    let BlockRequest {
                        piece_index,
                        offset,
                        ..
                    } = &inflight[i].request;

                    // make this a function later
                    let piece = &mut self.pieces[*piece_index as usize];
                    let block_index = (offset / BLOCK_SIZE) as usize;

                    if !piece.complete && piece.block_status[block_index] == BlockState::Requested {
                        piece.block_status[block_index] = BlockState::NotRequested;
                    }

                    inflight.swap_remove(i);
                } else {
                    i += 1;
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
        let inflightblocks = &self.slots.get_mut(unique_id).unwrap().in_flight;

        for InFlightBlock { request, .. } in inflightblocks {
            let BlockRequest {
                piece_index,
                offset,
                ..
            } = request;
            let piece = &mut self.pieces[*piece_index as usize];
            let block_index = (offset / BLOCK_SIZE) as usize;

            if !piece.complete && piece.block_status[block_index] == BlockState::Requested {
                piece.block_status[block_index] = BlockState::NotRequested;
            }
        }

        let removed = self
            .slots
            .try_remove(unique_id)
            .expect("Received removal event for unknown peer");

        self.existing_peers.remove(&removed.addr);
    }

    // precompute this shit for god's sake
    fn piece_len_for_index(num_pieces: usize, total_len: u64, piece_len: u64, index: usize) -> u64 {
        if index < num_pieces - 1 {
            piece_len
        } else {
            total_len - piece_len * (num_pieces as u64 - 1)
        }
    }

    async fn handle_have(&mut self, slot_id: usize, piece: u32) {
        let peer_handle = self.slots.get_mut(slot_id).unwrap();
        peer_handle.bitfield.set(piece as usize, true);

        let am_interested = &peer_handle.am_interested;

        if !am_interested {
            let _ = peer_handle.sender.send(PeerCommand::SendInterested).await;
            peer_handle.am_interested = true;
        }

        // println!("Handle have: Peer {}, interested: {}, choked: {}, in_flight: {}", slot_id, peer_handle.am_interested, peer_handle.am_choked, peer_handle.in_flight.len());
        self.maybe_push_blocks(slot_id).await;
    }

    // these 4 can probably be compressed into one function with a type param
    fn handle_choke(&mut self, slot_id: usize) {
        let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");

        if !peer_handle.am_choked {
            peer_handle.am_choked = true;
        }
    }

    async fn handle_unchoke(&mut self, slot_id: usize) {
        let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");

        if peer_handle.am_choked {
            peer_handle.am_choked = false;
            // println!("handle unchoke: Peer {}, interested: {}, choked: {}, in_flight: {}", slot_id, peer_handle.am_interested, peer_handle.am_choked, peer_handle.in_flight.len());
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

    // only set and indicate interest
    // perhaps an actual check to see whether there are any interesting pieces would be nice
    // but I am too lazy
    async fn handle_bitfield(&mut self, slot_id: usize, bitfield: BitVec) {
        let peer_handle = self.slots.get_mut(slot_id).unwrap();
        peer_handle.bitfield = bitfield;

        let am_interested = &peer_handle.am_interested;

        if !am_interested {
            let _ = peer_handle.sender.send(PeerCommand::SendInterested).await;
            peer_handle.am_interested = true;
        }

        // maybe request blocks here too?
    }

    // request iff I am interested, not choked, and I have <= max requests in flight / 2
    async fn maybe_push_blocks(&mut self, slot_id: usize) {
        let (should_request, in_flight) = {
            let peer_handle = self.slots.get(slot_id).expect("Peer doesnt exist");
            (
                peer_handle.am_interested && !peer_handle.am_choked,
                peer_handle.in_flight.len(),
            )
        };
        if !should_request || in_flight as u32 > MAX_IN_FLIGHT / 2 {
            return;
        }

        let blocks = self.push_blocks(slot_id, MAX_IN_FLIGHT - in_flight as u32);

        if !blocks.is_empty() {
            let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");
            let _ = peer_handle
                .sender
                .send(PeerCommand::BlocksToDownload { blocks })
                .await;
        }
    }

    // FCFS, this is very naive and I should probably switch to rarest first, or something heap based
    // ordered by the number of peers who have it
    // but how tf are you supposed to modify it on the fly?
    fn push_blocks(&mut self, slot_id: usize, mut request: u32) -> Vec<BlockRequest> {
        let peer_handle = self.slots.get_mut(slot_id).expect("Peer doesnt exist");
        let mut blocks: Vec<BlockRequest> = Vec::with_capacity(request as usize);

        for piece_index in 0..self.pieces.len() {
            if request == 0 {
                break;
            }

            let len = Self::piece_len_for_index(
                self.num_pieces,
                self.total_len,
                self.piece_len,
                piece_index,
            );
            let piece_buff = &mut self.pieces[piece_index];

            if piece_buff.complete {
                continue;
            }

            if *peer_handle.bitfield.get(piece_index).unwrap() {
                piece_buff.lazy_init(len);
            }

            for block_index in 0..piece_buff.block_status.len() {
                if request == 0 {
                    break;
                }

                if piece_buff.block_status[block_index] == BlockState::NotRequested {
                    piece_buff.block_status[block_index] = BlockState::Requested;

                    let block_request = BlockRequest {
                        piece_index: piece_index as u32,
                        offset: block_index as u32 * BLOCK_SIZE,
                        len: BLOCK_SIZE.min(len as u32 - (block_index) as u32 * BLOCK_SIZE),
                    };

                    blocks.push(block_request.clone());
                    peer_handle.in_flight.push(InFlightBlock {
                        request: block_request,
                        sent_at: time::Instant::now(),
                    });
                    request -= 1;
                }
            }
        }
        blocks
    }

    // Only work this does is append the data to an existing vector, manage completion and send the piece
    // into the disk writer task for verification
    async fn handle_data(&mut self, slot_id: usize, piece: u32, begin: u32, data: Vec<u8>) {
        let peer_handle = self.slots.get_mut(slot_id).unwrap();
        
        // this is a bad hack, -8 because it includes the piece header, which should be removed
        // but the borrow checker is too strict and does not allow &[u8] slices over async boundaries
        if let Some(pos) = peer_handle.in_flight.iter().position(|block| {
            block.request
                == BlockRequest {
                    piece_index: piece,
                    offset: begin,
                    len: (data.len() - 8) as u32,
                }
        }) {
            peer_handle.in_flight.swap_remove(pos);
        }
        // println!("Handle data after supposed removal from inflight: Peer {}, interested: {}, choked: {}, in_flight: {}", slot_id, peer_handle.am_interested, peer_handle.am_choked, peer_handle.in_flight.len());

        let begin = begin as usize;
        let block = begin / BLOCK_SIZE as usize;

        let piece_buffer = &mut self.pieces[piece as usize];
        let block_status = &mut piece_buffer.block_status;

        if piece_buffer.complete || block_status[block] == BlockState::Received {
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

        // println!("Handle data before pushing blocks: Peer {}, interested: {}, choked: {}, in_flight: {}", slot_id, peer_handle.am_interested, peer_handle.am_choked, peer_handle.in_flight.len());
        self.maybe_push_blocks(slot_id).await;
    }

    // cleanup or download more RAM
    // broadcast to all peers that I have the piece
    async fn on_complete_piece(&mut self, piece: u32) {
        let piece_buffer = self.pieces.get_mut(piece as usize).unwrap();
        piece_buffer.complete_and_cleanup();
        self.completed_pieces += 1;

        println!("Completed {} pieces", self.completed_pieces);

        // a naive scan over a slab is deemed bad because it scans tombstone slots too
        // this is the only linear scan over the slab
        for (_, peer_handle) in &self.slots {
            peer_handle
                .sender
                .send(PeerCommand::SendHave { piece })
                .await
                .ok();
        }
    }

    // who cares how the piece failed?
    // Retry anyway, either a hash or write failure
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
            in_flight: Vec::with_capacity(MAX_IN_FLIGHT as usize),
        }
    }
}
