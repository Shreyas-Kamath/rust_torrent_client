use bitvec::vec::BitVec;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc::{Receiver, Sender},
};

use crate::{
    peers::{MessageID, PeerCommand, PeerEvent, PeerReader, PeerWriter},
    scheduler::BlockRequest,
};

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

impl PeerWriter {
    fn new(writer: OwnedWriteHalf, peer_command_rx: Receiver<PeerCommand>) -> Self {
        Self {
            writer,
            peer_command_rx,
        }
    }

    async fn run(mut self) -> io::Result<()> {
        while let Some(cmd) = self.peer_command_rx.recv().await {
            match cmd {
                PeerCommand::Shutdown => {
                    return Ok(());
                }
                PeerCommand::Resume => {
                    todo!();
                }
                PeerCommand::BlocksToDownload { blocks } => {
                    self.download(blocks).await?;
                }
                PeerCommand::SendHave { piece } => {
                    self.send_have(piece).await?;
                }
                PeerCommand::SendInterested => {
                    self.send_interested().await?;
                }
                PeerCommand::SendChoke => {
                    self.send_choke().await?;
                }
                PeerCommand::SendUnchoke => {
                    self.send_unchoke().await?;
                }
                PeerCommand::SendPiece => {
                    todo!();
                }
                PeerCommand::SendCancel => {
                    todo!();
                }
            }
        }
        Ok(())
    }

    async fn download(&mut self, blocks: Vec<BlockRequest>) -> tokio::io::Result<()> {
        let mut counter = 0;
        let mut blocks_buf = vec![0u8; 17 * blocks.len()];

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

        self.writer.write_all(&blocks_buf).await?;
        Ok(())
    }

    async fn send_have(&mut self, piece: u32) -> io::Result<()> {
        let mut have_buffer = [0u8; 9];

        have_buffer[0..4].copy_from_slice(&5u32.to_be_bytes());
        have_buffer[4] = MessageID::Have as u8;
        have_buffer[5..].copy_from_slice(&piece.to_be_bytes());

        self.writer.write_all(&have_buffer).await
    }

    async fn send_interested(&mut self) -> io::Result<()> {
        let mut interested_buffer = [0u8; 5];

        interested_buffer[..4].copy_from_slice(&1u32.to_be_bytes());
        interested_buffer[4] = MessageID::Interested as u8;

        self.writer.write_all(&interested_buffer).await
    }

    async fn send_unchoke(&mut self) -> io::Result<()> {
        let mut unchoke_buffer = [0u8; 5];

        unchoke_buffer[..4].copy_from_slice(&1u32.to_be_bytes());
        unchoke_buffer[4] = MessageID::Unchoke as u8;

        self.writer.write_all(&unchoke_buffer).await
    }

    async fn send_choke(&mut self) -> io::Result<()> {
        let mut choke_buffer = [0u8; 5];

        choke_buffer[..4].copy_from_slice(&1u32.to_be_bytes());
        choke_buffer[4] = MessageID::Choke as u8;

        self.writer.write_all(&choke_buffer).await
    }
}

impl PeerReader {
    fn new(
        slot_id: usize,
        reader: OwnedReadHalf,
        peer_event_tx: Sender<PeerEvent>,
        num_pieces: usize,
    ) -> Self {
        Self {
            slot_id,
            reader,
            peer_event_tx,
            message_buffer: Vec::new(),
            num_pieces,
        }
    }

    async fn run(mut self) -> io::Result<()> {
        loop {
            self.read_message().await?
        }
    }

    async fn read_message(&mut self) -> io::Result<()> {
        let len = self.reader.read_u32().await?;
        // println!("Message length: {len}");
        // add keep alive timer later
        if len == 0 {
            return Ok(());
        }

        let id_byte = self.reader.read_u8().await?;
        let message_id = MessageID::try_from(id_byte);
        // println!("Message id: {id_byte}");

        self.message_buffer.resize((len - 1) as usize, 0u8);
        self.reader.read_exact(&mut self.message_buffer).await?;

        match message_id {
            Ok(MessageID::Choke) => {
                if self
                    .peer_event_tx
                    .send(PeerEvent::Choke {
                        slot_id: self.slot_id,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                };
            }
            Ok(MessageID::Unchoke) => {
                if self
                    .peer_event_tx
                    .send(PeerEvent::Unchoke {
                        slot_id: self.slot_id,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                };
            }
            Ok(MessageID::Interested) => {
                if self
                    .peer_event_tx
                    .send(PeerEvent::Interested {
                        slot_id: self.slot_id,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                };
            }
            Ok(MessageID::NotInterested) => {
                if self
                    .peer_event_tx
                    .send(PeerEvent::NotInterested {
                        slot_id: self.slot_id,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                };
            }

            Ok(MessageID::Have) => {
                let piece = u32::from_be_bytes(self.message_buffer[0..4].try_into().unwrap());
                if self
                    .peer_event_tx
                    .send(PeerEvent::Have {
                        slot_id: self.slot_id,
                        piece,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                };
            }

            Ok(MessageID::Bitfield) => {
                // we usually wait and the scan the bitfield if we need a piece
                // but for testing send interested anyway
                let bitfield = self.parse_bitfield()?;
                if self
                    .peer_event_tx
                    .send(PeerEvent::Bitfield {
                        slot_id: self.slot_id,
                        bitfield,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                };
            }

            Ok(MessageID::Request) => {
                todo!("Peer is requesting");
            }

            Ok(MessageID::Piece) => {
                if self.message_buffer.len() >= 8 {
                    let piece = u32::from_be_bytes(self.message_buffer[0..4].try_into().unwrap());
                    let begin = u32::from_be_bytes(self.message_buffer[4..8].try_into().unwrap());
                    let data = std::mem::take(&mut self.message_buffer);

                    if self
                        .peer_event_tx
                        .send(PeerEvent::Data {
                            slot_id: self.slot_id,
                            piece,
                            begin,
                            data,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
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

    fn parse_bitfield(&self) -> io::Result<BitVec> {
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
}

pub async fn run_peer(
    slot_id: usize,
    info_hash: [u8; 20],
    mut stream: TcpStream,
    peer_event_tx: Sender<PeerEvent>,
    peer_command_rx: Receiver<PeerCommand>,
    num_pieces: usize,
    incoming: bool,
) {
    let tx_clone = peer_event_tx.clone();

    if do_handshake(&mut stream, info_hash, incoming)
        .await
        .is_err()
    {
        let _ = tx_clone.send(PeerEvent::Disconnect { slot_id }).await;
        return;
    }

    let (reader, writer) = stream.into_split();

    let reader = PeerReader::new(slot_id, reader, peer_event_tx, num_pieces);
    let writer = PeerWriter::new(writer, peer_command_rx);

    let mut reader = tokio::spawn(reader.run());
    let mut writer = tokio::spawn(writer.run());

    tokio::select! {
        _ = &mut reader => {
            writer.abort();
        }

        _ = &mut writer => {
            reader.abort();
        }
    }
    let _ = tx_clone.send(PeerEvent::Disconnect { slot_id }).await;
}

async fn do_handshake(
    stream: &mut TcpStream,
    info_hash: [u8; 20],
    incoming: bool,
) -> tokio::io::Result<()> {
    let mut handshake_buf = [0u8; 68];
    handshake_buf[0] = 19;
    handshake_buf[1..1 + P_STR_PROTOCOL.len()].copy_from_slice(P_STR_PROTOCOL);
    handshake_buf[28..28 + info_hash.len()].copy_from_slice(&info_hash);
    handshake_buf[48..48 + PEER_ID.len()].copy_from_slice(PEER_ID);

    stream.write_all(&handshake_buf).await?;

    if !incoming {
        let mut incoming_handshake_buff = [0u8; 68];
        stream.read_exact(&mut incoming_handshake_buff).await?;

        validate_handshake(&incoming_handshake_buff, info_hash)?;
    }

    Ok(())
}

fn validate_handshake(peer_handshake: &[u8; 68], info_hash: [u8; 20]) -> tokio::io::Result<()> {
    if peer_handshake[0] != 19 {
        return Err(io::Error::other("Bad pstrlen"));
    }

    if peer_handshake[1..20] != *P_STR_PROTOCOL {
        return Err(io::Error::other("Bad protocol"));
    }

    if peer_handshake[28..48] != info_hash {
        return Err(io::Error::other("Info hash mismatch"));
    }

    Ok(())
}
