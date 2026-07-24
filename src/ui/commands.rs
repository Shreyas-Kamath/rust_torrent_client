use std::path::PathBuf;

use rfd::FileHandle;
use tokio::{net::TcpStream, sync::mpsc};

use crate::peers::Peer;

#[derive(Debug, Clone)]
pub enum Message {
    AddTorrent,
    TorrentLoaded(Option<FileHandle>),
    Nothing,
}

pub struct App {
    pub torrent_count: usize,
    pub client_tx: mpsc::Sender<ClientCommand>,
}

pub enum ClientCommand {
    AddTorrent {
        path: PathBuf,
    },
    RemoveTorrent {
        id: String,
        remove_files: bool,
    },
    PauseTorrent {
        id: String,
    },
    ResumeTorrent {
        id: String,
    },
    IncomingPeer {
        stream: TcpStream,
        info_hash: [u8; 20],
        peer: Peer,
    },
}
