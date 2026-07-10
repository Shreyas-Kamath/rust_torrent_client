use std::path::PathBuf;

use rfd::FileHandle;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum Message {
    AddTorrent,
    TorrentLoaded(Option<FileHandle>),
    Nothing,
}

pub struct App {
    pub torrent_count: usize,
    pub client_tx: mpsc::Sender<UIToClientCommand>,
}

pub enum UIToClientCommand {
    AddTorrent { path: PathBuf },
    RemoveTorrent { id: String, remove_files: bool },
    PauseTorrent { id: String },
    ResumeTorrent { id: String },
}
