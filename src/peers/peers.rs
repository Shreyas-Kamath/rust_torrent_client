use bitvec::vec::BitVec;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};

use crate::{
    peers::{MessageID, PeerCommand, PeerConnection, PeerEvent},
    scheduler::BlockRequest,
};

const P_STR_PROTOCOL: &[u8] = b"BitTorrent protocol";
const PEER_ID: &[u8] = b"-TR2940-1234567890ab";
const MAX_IN_FLIGHT: u32 = 20;

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
        num_pieces: usize,
    ) -> Self {
        Self {
            socket: stream,
            peer_response_tx: tx,
            slot_id,
            info_hash,
            id,
            peer_choked: true,
            am_choked: true,
            peer_interested: false,
            am_interested: false,
            in_flight: 0,
            message_buffer: Vec::new(),
            num_pieces,
        }
    }

    async fn do_handshake(&mut self) -> tokio::io::Result<()> {
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

    fn validate_handshake(&self, peer_handshake: &[u8; 68]) -> tokio::io::Result<()> {
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

    fn parse_bitfield(&self) -> tokio::io::Result<BitVec> {
        let expected = self.num_pieces.div_ceil(8);

        if self.message_buffer.len() != expected {
            return Err(io::Error::other("Invalid bitfield length"));
        }
        let mut bitfield = BitVec::with_capacity(self.num_pieces);

        for &byte in &self.message_buffer {
            for i in (0..8).rev() {
                if bitfield.len() == self.num_pieces {
                    break;
                }

                bitfield.push((byte >> i) & 1 != 0);
            }
        }

        Ok(bitfield)
    }

    pub async fn run(mut self, mut scheduler_rx: Receiver<PeerCommand>) {
        let res = self.run_inner(scheduler_rx).await;

        self.peer_response_tx
            .send(PeerEvent::Disconnect {
                slot_id: self.slot_id,
            })
            .await
            .ok();

        if let Err(_) = res {
            // eprintln!("Peer {} exiting: {}", self.slot_id, e);
        }
    }

    async fn send_interested(&mut self) -> tokio::io::Result<()> {
        let mut interested_buffer = [0u8; 5];
        interested_buffer[..4].copy_from_slice(&1u32.to_be_bytes());
        interested_buffer[4] = MessageID::Interested as u8;

        self.socket.write_all(&interested_buffer).await?;

        self.am_interested = true;
        Ok(())
    }

    async fn request_blocks(&mut self) {
        println!("{} is about to request {} blocks from the scheduler", self.slot_id, MAX_IN_FLIGHT - self.in_flight);
        self.peer_response_tx
            .send(PeerEvent::RequestingBlocks {
                slot_id: self.slot_id,
                num: MAX_IN_FLIGHT - self.in_flight,
            })
            .await
            .ok();
    }

    async fn maybe_request_blocks(&mut self) {
        if self.am_interested && !self.am_choked && self.in_flight <= MAX_IN_FLIGHT / 2 {
            self.request_blocks().await;
        }
    }

    async fn download(&mut self, blocks: Vec<BlockRequest>) -> tokio::io::Result<()> {
        let mut counter = 0;
        let mut blocks_buf: Vec<u8> = Vec::new();
        blocks_buf.resize(17 * blocks.len(), 0u8);

        for BlockRequest {
            piece_index,
            offset,
            len,
        } in &blocks
        {
            blocks_buf[counter..counter + 4].copy_from_slice(&13u32.to_be_bytes());
            blocks_buf[counter + 4] = MessageID::Request as u8;
            blocks_buf[counter + 5..counter + 9].copy_from_slice(&piece_index.to_be_bytes());
            blocks_buf[counter + 9..counter + 13].copy_from_slice(&offset.to_be_bytes());
            blocks_buf[counter + 13..counter + 17].copy_from_slice(&len.to_be_bytes());

            counter += 17;
        }

        self.socket.write_all(&blocks_buf).await?;
        self.in_flight += blocks.len() as u32;

        println!("Sent requests to {}", self.slot_id);
        Ok(())
    }

    async fn read_message(&mut self) -> tokio::io::Result<()> {
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
                if self.am_choked {
                    self.am_choked = false;
                    self.maybe_request_blocks().await;
                }
            }
            Ok(MessageID::Interested) => {
                self.peer_interested = true;
            }
            Ok(MessageID::NotInterested) => {
                self.peer_interested = false;
            }

            Ok(MessageID::Have) => {
                let piece = u32::from_be_bytes(self.message_buffer[0..4].try_into().unwrap());
                self.peer_response_tx
                    .send(PeerEvent::Have {
                        slot_id: self.slot_id,
                        piece,
                    })
                    .await
                    .ok();
                if !self.am_interested {
                    self.send_interested().await?;
                    self.maybe_request_blocks().await;
                }
            }

            Ok(MessageID::Bitfield) => {
                // we usually wait and the scan the bitfield if we need a piece
                // but for testing send interested anyway
                let bitfield = self.parse_bitfield()?;
                self.peer_response_tx
                    .send(PeerEvent::Bitfield {
                        slot_id: self.slot_id,
                        bitfield,
                    })
                    .await
                    .ok();
                if !self.am_interested {
                    self.send_interested().await?;
                }
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

    async fn run_inner(
        &mut self,
        mut scheduler_rx: Receiver<PeerCommand>,
    ) -> tokio::io::Result<()> {
        self.do_handshake().await?;
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
                        PeerCommand::BlocksToDownload { blocks } => {
                            if let Some(blocks) = blocks {
                                self.download(blocks).await?;
                            }
                            else {
                                println!("{} requested but got None", self.slot_id);
                            }
                        }
                    }
                }
            }
        }
    }
}
