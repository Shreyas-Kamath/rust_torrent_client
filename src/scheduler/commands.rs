use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use bitvec::vec::BitVec;
use slab::Slab;
use tokio::sync::mpsc::Sender;

use crate::peers::{
    Peer,
    commands::{PeerCommand, PeerEvent},
};

type PeerID = u32;

pub enum SchedulerEvent {
    LaunchPeers { peers: Vec<Peer> },

    ShutdownPeers,
    FetchInfo,
}

pub struct PieceBuffer {
    pub data: Vec<u8>,
    pub block_status: Vec<BlockState>,
    pub blocks_received: u32,
    pub complete: bool,
}

#[derive(PartialEq, Eq)]
pub enum BlockState {
    NotRequested,
    Requested, // track with slot id later
    Received,
}

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
    pub piece_hashes: Vec<[u8; 20]>,
    pub total_len: u64,
    pub piece_len: u64,
}

pub struct PeerHandle {
    pub bitfield: BitVec,
    pub sender: Sender<PeerCommand>,
    pub addr: SocketAddr,
}
