use std::net::SocketAddr;
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};

pub enum PeerEvent {
    Connect { stream: TcpStream, peer: Peer },

    // data from socket
    Data { data: Vec<u8> },

    Disconnect { id: usize },

    RequestingBlocks { num: u32 },

    Bitfield,
    Have { piece: u32 },
}

#[derive(Debug)]
pub struct Peer {
    pub addr: SocketAddr,
    pub id: Option<[u8; 20]>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageID {
    Choke = 0,
    Unchoke = 1,
    Interested = 2,
    NotInterested = 3,
    Have = 4,
    Bitfield = 5,
    Request = 6,
    Piece = 7,
    Cancel = 8,
    Port = 9,
}

pub enum PeerCommand {
    Shutdown,
    Resume,
}

pub struct PeerConnection {
    pub socket: TcpStream,

    pub slot_id: usize,
    pub id: Option<[u8; 20]>,
    pub info_hash: [u8; 20],

    pub peer_choked: bool,
    pub am_choked: bool,

    pub peer_interested: bool,
    pub am_interested: bool,

    pub in_flight: u32,

    // send stuff to the scheduler
    pub peer_response_tx: Sender<PeerEvent>,

    // send requests to the scheduler (maybe a oneshot later)

    // buffers
    pub message_buffer: Vec<u8>,
}
