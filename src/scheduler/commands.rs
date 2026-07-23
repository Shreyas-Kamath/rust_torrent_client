use std::{collections::HashSet, net::SocketAddr};

use bitvec::vec::BitVec;
use slab::Slab;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    disk::DiskEvent,
    peers::{Peer, PeerCommand},
};

pub enum SchedulerEvent {
    LaunchPeers { peers: Vec<Peer> },

    ShutdownPeers,
    FetchInfo,
}

#[derive(Clone, Default)]
pub struct PieceBuffer {
    pub data: Vec<u8>,
    pub block_status: Vec<BlockState>,
    pub blocks_received: u32,
    pub complete: bool,
}

#[derive(PartialEq, Eq, Clone)]
pub enum BlockState {
    NotRequested,
    Requested, // track with slot id later
    Received,
}

#[derive(Clone, PartialEq, Debug)]
pub struct BlockRequest {
    pub piece_index: u32,
    pub offset: u32,
    pub len: u32,
}

pub struct Scheduler {
    pub existing_peers: HashSet<SocketAddr>,
    pub slots: Slab<PeerHandle>,
    pub num_pieces: usize,
    pub pieces: Vec<PieceBuffer>,
    pub total_len: u64,
    pub piece_len: u64,
    pub disk_event_sender: Sender<DiskEvent>,
    pub completed_pieces: u32,
}

pub struct InFlightBlock {
    pub request: BlockRequest,
    pub sent_at: Instant,
}

pub struct PeerHandle {
    pub bitfield: BitVec,
    pub sender: Sender<PeerCommand>,
    pub addr: SocketAddr,

    pub peer_choked: bool,
    pub am_choked: bool,

    pub peer_interested: bool,
    pub am_interested: bool,

    pub in_flight: Vec<InFlightBlock>,
}
