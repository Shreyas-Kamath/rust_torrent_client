use crate::client::commands::ClientCommand;
use crate::parser::commands::Torrent;
use crate::trackers::commands::TrackerCommand;
use std::{collections::HashMap, path::PathBuf};
use tokio::{io::Result, sync::mpsc};

use crate::session;

use crate::session::commands::Session;
use crate::{
    client::commands,
    parser::parse_torrent,
    ui::{self, commands::UIToClientCommand},
};

type TorrentID = [u8; 20];

async fn try_parse_torrent(path: PathBuf) -> std::result::Result<Torrent, String> {
    let data = tokio::fs::read(path).await.map_err(|e| e.to_string())?;
    let torrent = parse_torrent(&data)?;
    Ok(torrent)
}

pub async fn run(mut rx: mpsc::Receiver<ui::commands::UIToClientCommand>) -> Result<()> {
    let mut session_map: HashMap<TorrentID, mpsc::Sender<commands::ClientCommand>> = HashMap::new();
    let http_client = reqwest::Client::new();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            UIToClientCommand::AddTorrent { path } => match try_parse_torrent(path).await {
                Ok(result) => {
                    let (session_tx, session_rx) = tokio::sync::mpsc::channel::<ClientCommand>(32);
                    let id = result.info_hash;

                    let session = Session::new(result, http_client.clone());
                    tokio::spawn(session.run(session_rx));
                    session_map.insert(id, session_tx);
                }
                Err(e) => {
                    eprintln!("Some error: {e}");
                }
            },
            _ => {
                println!("Not implemented yet");
            }
        }
    }
    Ok(())
}
