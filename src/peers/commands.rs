use bitvec::vec::BitVec;
use std::net::SocketAddr;
use tokio::{
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc::{Receiver, Sender},
};

use crate::scheduler::BlockRequest;

pub enum PeerEvent {
    Connect {
        stream: TcpStream,
        peer: Peer,
        incoming: bool,
    },
    ConnectFailed {
        peer: Peer,
    },

    // data from socket
    Data {
        slot_id: usize,
        piece: u32,
        begin: u32,
        data: Vec<u8>,
    },

    Disconnect {
        slot_id: usize,
    },

    Bitfield {
        slot_id: usize,
        bitfield: BitVec,
    },

    Have {
        slot_id: usize,
        piece: u32,
    },

    Choke {
        slot_id: usize,
    },

    Unchoke {
        slot_id: usize,
    },

    Interested {
        slot_id: usize,
    },

    NotInterested {
        slot_id: usize,
    },
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
    SendInterested,
    SendUnchoke,
    SendChoke,
    SendCancel,
    SendPiece,
    BlocksToDownload { blocks: Vec<BlockRequest> },
    SendHave { piece: u32 },
}

pub struct PeerReader {
    pub slot_id: usize,
    pub reader: OwnedReadHalf,
    pub peer_event_tx: Sender<PeerEvent>,
    pub message_buffer: Vec<u8>,
    pub num_pieces: usize,
}

pub struct PeerWriter {
    pub writer: OwnedWriteHalf,
    pub peer_command_rx: Receiver<PeerCommand>,
}
