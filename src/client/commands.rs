use std::collections::HashMap;

use tokio::{net::TcpStream, sync::mpsc};

use crate::peers::Peer;

type TorrentID = [u8; 20];

pub enum SessionCommand {
    ForceReannounce,
    FetchTrackerInfo,
    FetchPeersInfo,
    FetchTorrentInfo,
    IncomingPeer { stream: TcpStream, peer: Peer },
}

struct SessionHandle {
    tx: tokio::sync::mpsc::Sender<SessionCommand>,
    join: tokio::task::JoinHandle<()>,
}

pub struct Client {
    pub session_map: HashMap<TorrentID, mpsc::Sender<SessionCommand>>,
    pub http_client: reqwest::Client,
}
