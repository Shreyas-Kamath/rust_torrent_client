use std::net::{Ipv4Addr, Ipv6Addr};

use crate::{parser::Torrent, trackers::TrackerResponse};
use serde_bencode;
use serde_bencode::value::Value;
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use tokio::{
    io::{self, AsyncReadExt},
    net::TcpStream,
};

use crate::peers::Peer;

// duplicate
const P_STR_PROTOCOL: &[u8] = b"BitTorrent protocol";

pub fn parse_torrent(raw: &[u8]) -> Result<Torrent, String> {
    let mut result: Torrent = serde_bencode::from_bytes(raw).map_err(|e| e.to_string())?;
    let total_length = if let Some(files) = &result.info.files {
        // multi-file torrent → sum all file lengths
        files.iter().map(|f| f.length).sum()
    } else {
        result.info.length.unwrap_or_default()
    };

    result.total_length = total_length;
    result.info_hash = Sha1::digest(serde_bencode::to_bytes(&result.info).unwrap()).into();

    Ok(result)
}

fn parse_peers_v4(value: &Value) -> Vec<Peer> {
    match value {
        Value::Bytes(bytes) => bytes
            .chunks_exact(6)
            .map(|chunk| {
                let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                Peer {
                    addr: SocketAddr::from((ip, port)),
                    id: None,
                }
            })
            .collect(),
        Value::List(list) => {
            let mut peers: Vec<Peer> = Vec::new();
            for item in list {
                let Value::Dict(dict) = item else {
                    continue;
                };

                let port = match dict.get(b"port".as_slice()) {
                    Some(Value::Int(i)) => *i as u16,
                    _ => continue,
                };

                let ip = match dict.get(b"ip".as_slice()) {
                    Some(Value::Bytes(bytes)) => bytes,
                    _ => continue,
                };

                let ip = match std::str::from_utf8(ip) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                let ip = match ip.parse::<Ipv4Addr>() {
                    Ok(ip) => ip,
                    Err(_) => continue,
                };

                let id = dict.get(b"peer id".as_slice()).and_then(|v| match v {
                    Value::Bytes(bytes) if bytes.len() == 20 => {
                        let mut id = [0u8; 20];
                        id.copy_from_slice(bytes);
                        Some(id)
                    }
                    _ => None,
                });

                peers.push(Peer {
                    addr: SocketAddr::from((ip, port)),
                    id,
                });
            }
            peers
        }
        _ => panic!("Should be unreachable"),
    }
}

fn parse_peers_v6(value: &Value) -> Vec<Peer> {
    match value {
        Value::Bytes(bytes) => bytes
            .chunks_exact(18)
            .map(|chunk| {
                let ip = Ipv6Addr::from([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                    chunk[8], chunk[9], chunk[10], chunk[11], chunk[12], chunk[13], chunk[14],
                    chunk[15],
                ]);
                let port = u16::from_be_bytes([chunk[16], chunk[17]]);
                Peer {
                    addr: SocketAddr::from((ip, port)),
                    id: None,
                }
            })
            .collect(),
        Value::List(list) => {
            let mut peers: Vec<Peer> = Vec::new();
            for item in list {
                let Value::Dict(dict) = item else {
                    continue;
                };

                let port = match dict.get(b"port".as_slice()) {
                    Some(Value::Int(i)) => *i as u16,
                    _ => continue,
                };

                let ip = match dict.get(b"ip".as_slice()) {
                    Some(Value::Bytes(bytes)) => bytes,
                    _ => continue,
                };

                let ip = match std::str::from_utf8(ip) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                let ip = match ip.parse::<Ipv6Addr>() {
                    Ok(ip) => ip,
                    Err(_) => continue,
                };

                let id = dict.get(b"peer id".as_slice()).and_then(|v| match v {
                    Value::Bytes(bytes) if bytes.len() == 20 => {
                        let mut id = [0u8; 20];
                        id.copy_from_slice(bytes);
                        Some(id)
                    }
                    _ => None,
                });

                peers.push(Peer {
                    addr: SocketAddr::from((ip, port)),
                    id,
                });
            }
            peers
        }
        _ => panic!("Should be unreachable"),
    }
}

pub fn parse_response(bytes: &[u8]) -> Result<TrackerResponse, serde_bencode::Error> {
    serde_bencode::from_bytes(bytes)
}

pub fn parse_peers(v4: &Value, v6: &Option<Value>) -> Vec<Peer> {
    let mut peers = parse_peers_v4(v4);

    if let Some(v6) = v6 {
        peers.extend(parse_peers_v6(v6));
    }

    peers
}

pub async fn parse_incoming_handshake(
    stream: &mut TcpStream,
) -> io::Result<([u8; 20], Option<[u8; 20]>)> {
    let mut buffer = [0u8; 68];
    stream.read_exact(&mut buffer).await?;

    if buffer[0] != 19 {
        return Err(io::Error::other("Pstrlen is not 19"));
    }

    if buffer[1..20] != *P_STR_PROTOCOL {
        return Err(io::Error::other("Protocol type does not match"));
    }

    // 8 bytes are feature flags (dht, pex etc..., very hard for me at this stage)

    let mut info_hash = [0u8; 20];
    info_hash.copy_from_slice(&buffer[28..48]);

    // ignore peer id for now
    Ok((info_hash, None))
}
