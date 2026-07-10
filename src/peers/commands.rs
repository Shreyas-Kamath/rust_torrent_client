use std::net::SocketAddr;
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};

pub enum PeerResponse {
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

pub enum PeerCommand {
    Shutdown,
    Resume,
}

pub struct PeerConnection {
    socket: TcpStream,

    pub addr: SocketAddr,
    pub id: Option<[u8; 20]>,

    // bitfield:
    peer_choked: bool,
    am_choked: bool,

    peer_interested: bool,
    am_interested: bool,

    in_flight: u32,

    scheduler_rx: Receiver<PeerCommand>,
    // sender to scheduler later
}
