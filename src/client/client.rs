use crate::client::SessionCommand;
use crate::parser::commands::Torrent;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::{io::Result, sync::mpsc};

use crate::session::commands::Session;
use crate::{
    client::Client,
    parser::parse_torrent,
    ui::{self, commands::ClientCommand},
};

impl Client {
    pub fn new() -> Self {
        Self {
            session_map: HashMap::new(),
            http_client: reqwest::Client::new(),
        }
    }

    async fn try_parse_torrent(&self, path: PathBuf) -> std::result::Result<Torrent, String> {
        let data = tokio::fs::read(path).await.map_err(|e| e.to_string())?;
        let torrent = parse_torrent(&data)?;
        Ok(torrent)
    }

    pub async fn run(&mut self, mut rx: mpsc::Receiver<ui::ClientCommand>) -> Result<()> {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                ClientCommand::AddTorrent { path } => match self.try_parse_torrent(path).await {
                    Ok(result) => {
                        let (session_tx, session_rx) =
                            tokio::sync::mpsc::channel::<SessionCommand>(32);
                        let id = result.info_hash;

                        let session = Session::new(result, self.http_client.clone());
                        tokio::spawn(session.run(session_rx));
                        self.session_map.insert(id, session_tx);
                    }
                    // do something with this shit
                    Err(_) => {}
                },
                ClientCommand::IncomingPeer {
                    stream,
                    info_hash,
                    peer,
                } => {
                    if let Some(sender) = self.session_map.get(&info_hash) {
                        let _ = sender
                            .send(SessionCommand::IncomingPeer { stream, peer })
                            .await;
                    }
                }
                _ => {
                    println!("Not implemented yet");
                }
            }
        }
        Ok(())
    }
}
