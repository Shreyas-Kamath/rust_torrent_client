pub enum ClientCommand {
    ForceReannounce,
    FetchTrackerInfo,
    FetchPeersInfo,
    FetchTorrentInfo,
}

struct SessionHandle {
    tx: tokio::sync::mpsc::Sender<ClientCommand>,
    join: tokio::task::JoinHandle<()>,
}
