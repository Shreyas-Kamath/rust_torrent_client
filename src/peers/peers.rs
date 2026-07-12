use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};

use crate::peers::{MessageID, PeerCommand, PeerConnection, PeerEvent};

const P_STR_PROTOCOL: &[u8] = b"BitTorrent protocol";
const PEER_ID: &[u8] = b"-TR2940-1234567890ab";

impl TryFrom<u8> for MessageID {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Choke),
            1 => Ok(Self::Unchoke),
            2 => Ok(Self::Interested),
            3 => Ok(Self::NotInterested),
            4 => Ok(Self::Have),
            5 => Ok(Self::Bitfield),
            6 => Ok(Self::Request),
            7 => Ok(Self::Piece),
            8 => Ok(Self::Cancel),
            9 => Ok(Self::Port),
            _ => Err(io::Error::other("Unknown message id")),
        }
    }
}

impl PeerConnection {
    pub fn new(
        stream: TcpStream,
        tx: Sender<PeerEvent>,
        slot_id: usize,
        id: Option<[u8; 20]>,
        info_hash: [u8; 20],
    ) -> Self {
        Self {
            socket: stream,
            peer_response_tx: tx,
            slot_id,
            info_hash,
            id,
            peer_choked: true,
            am_choked: true,
            peer_interested: true,
            am_interested: false,
            in_flight: 0,
            message_buffer: Vec::new(),
        }
    }

    async fn do_handshake(&mut self) -> io::Result<()> {
        let mut handshake_buf = [0u8; 68];
        handshake_buf[0] = 19;
        handshake_buf[1..1 + P_STR_PROTOCOL.len()].copy_from_slice(P_STR_PROTOCOL);
        handshake_buf[28..28 + self.info_hash.len()].copy_from_slice(&self.info_hash);
        handshake_buf[48..48 + PEER_ID.len()].copy_from_slice(PEER_ID);

        self.socket.write_all(&handshake_buf).await?;

        let mut incoming_handshake_buff = [0u8; 68];
        self.socket.read_exact(&mut incoming_handshake_buff).await?;

        self.validate_handshake(&incoming_handshake_buff)?;

        Ok(())
    }

    fn validate_handshake(&self, peer_handshake: &[u8; 68]) -> io::Result<()> {
        if peer_handshake[0] != 19 {
            return Err(io::Error::other("Bad pstrlen"));
        }

        if peer_handshake[1..20] != *P_STR_PROTOCOL {
            return Err(io::Error::other("Bad protocol"));
        }

        if peer_handshake[28..48] != self.info_hash {
            return Err(io::Error::other("Info hash mismatch"));
        }

        Ok(())
    }

    pub async fn run(mut self, mut scheduler_rx: Receiver<PeerCommand>) {
        let res = self.run_inner(scheduler_rx).await;

        self.peer_response_tx
            .send(PeerEvent::Disconnect { id: self.slot_id })
            .await
            .ok();

        if let Err(e) = res {
            eprintln!("Peer {} exiting: {}", self.slot_id, e);
        }
    }

    async fn read_message(&mut self) -> io::Result<()> {
        let len = self.socket.read_u32().await?;
        // add keep alive timer later
        if len == 0 {
            return Ok(());
        }

        let message_id = MessageID::try_from(self.socket.read_u8().await?);

        self.message_buffer.resize((len - 1) as usize, 0u8);
        self.socket.read_exact(&mut self.message_buffer).await?;

        match message_id {
            // flags
            Ok(MessageID::Choke) => {
                self.am_choked = true;
            }
            Ok(MessageID::Unchoke) => {
                self.am_choked = false;
            }
            Ok(MessageID::Interested) => {
                self.peer_interested = true;
            }
            Ok(MessageID::NotInterested) => {
                self.peer_interested = false;
            }

            Ok(MessageID::Have) => {
                let piece = u32::from_be_bytes(self.message_buffer[0..4].try_into().unwrap());
                self.peer_response_tx.send(PeerEvent::Have { piece }).await;
            }

            Ok(MessageID::Bitfield) => {
                todo!("Parse this shit later");
            }

            Ok(MessageID::Request) => {
                todo!("Peer is requesting");
            }

            Ok(MessageID::Piece) => {
                todo!("Peer sent a block");
            }

            Ok(MessageID::Cancel) => {
                todo!("Peer sent cancel");
            }

            Ok(MessageID::Port) => {
                todo!("Peer sent port");
            }

            Err(e) => {
                eprintln!("{e}");
            }
        }

        Ok(())
    }

    async fn run_inner(&mut self, mut scheduler_rx: Receiver<PeerCommand>) -> io::Result<()> {
        self.do_handshake().await?;
        println!("Good handshake!");
        // self.send_bitfield().await?;

        loop {
            tokio::select! {
                res = self.read_message() => {
                    res?;
                }

                Some(cmd) = scheduler_rx.recv() => {
                    match cmd {
                        PeerCommand::Shutdown => {
                            return Ok(());
                        }
                        PeerCommand::Resume => {
                            todo!();
                        }
                    }
                }
            }
        }
    }
}
