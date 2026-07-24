mod client;
mod disk;
mod parser;
mod peers;
mod scheduler;
mod session;
mod trackers;
mod ui;

use crate::{
    client::Client,
    parser::parse_incoming_handshake,
    peers::Peer,
    ui::{App, ClientCommand, Message},
};
use iced::Task;
use tokio::{
    net::TcpListener,
    sync::mpsc::{self, Sender},
};

fn boot() -> (App, Task<Message>) {
    let (tx, rx) = mpsc::channel(32);

    let t1 = tx.clone();
    let t2 = tx.clone();

    tokio::spawn(async move {
        if let Ok(v4) = TcpListener::bind("0.0.0.0:6881").await {
            tokio::spawn(accept_loop(v4, t1));
        }

        if let Ok(v6) = TcpListener::bind("[::]:6881").await {
            tokio::spawn(accept_loop(v6, t2));
        }

        let mut client = Client::new();
        let _ = client.run(rx).await;
    });

    (App::new(tx), Task::none())
}

async fn accept_loop(listener: TcpListener, sender: Sender<ClientCommand>) {
    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => match parse_incoming_handshake(&mut stream).await {
                Ok((info_hash, id)) => {
                    let _ = sender
                        .send(ClientCommand::IncomingPeer {
                            stream,
                            info_hash,
                            peer: Peer { addr, id },
                        })
                        .await;
                }
                Err(e) => {
                    eprintln!("{e}");
                }
            },
            Err(e) => {
                eprintln!("Incoming peer dropped: {e}");
            }
        }
    }
}

fn main() -> iced::Result {
    iced::application(boot, App::update, App::view)
        .title("Rust Torrent")
        .run()
}
