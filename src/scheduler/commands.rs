use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use bitvec::vec::BitVec;
use slab::Slab;
use tokio::sync::mpsc::Sender;

use crate::peers::{
    Peer,
    commands::{PeerCommand, PeerResponse},
};

type PeerID = u32;

pub enum SchedulerEvent {
    LaunchPeers { peers: Vec<Peer> },

    ShutdownPeers,
    FetchInfo,
}

pub struct PieceBuffer {
    data: Vec<u8>,
    block_status: Vec<BlockState>,
    blocks_received: u32,
    complete: bool,
}

enum BlockState {
    NotRequested,
    Requested,
    Received,
}

pub struct Scheduler {
    pub existing_peers: HashSet<SocketAddr>,
    pub slots: Slab<PeerHandle>,
    pub num_pieces: usize,
    pub pieces: Vec<PieceBuffer>,
    pub piece_hashes: Vec<[u8; 20]>,
}

pub struct PeerHandle {
    pub bitfield: BitVec,
    pub sender: Sender<PeerCommand>,
    pub addr: SocketAddr,
}
