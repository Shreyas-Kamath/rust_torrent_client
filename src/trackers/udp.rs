use std::{net::{Ipv4Addr, Ipv6Addr, SocketAddr}, time::Duration};
use iced::futures::io;
use rand::random;
use reqwest::Url;
use tokio::{net::{UdpSocket, lookup_host}, sync::mpsc::{self, Sender}, time::{self, Instant}};
use crate::{peers::Peer, session::SessionEvent, trackers::{DEFAULT_ANNOUNCE_TIMER, TrackerCommand}};

const PEER_ID: &[u8] = b"-TR2940-1234567890ab";
const PORT: u16 = 6881;

pub struct UDPTracker {
    url: String,
    info_hash: [u8; 20],

    ipv4: Option<UDPContext>,
    ipv6: Option<UDPContext>,

    leechers: u32,
    seeders: u32,

    sender: Sender<SessionEvent>,
}

pub struct UDPResponse {
    pub peers: Vec<Peer>,
    pub leechers: u32,
    pub seeders: u32,
    pub interval: u32,
}

pub struct UDPContext {
    pub socket: UdpSocket,
    pub remote: SocketAddr,
    pub connection: Option<Connection>,
}

impl UDPContext {
    async fn new(remote: SocketAddr) -> tokio::io::Result<Self> {
        let bind_addr = if remote.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };

        let socket = UdpSocket::bind(bind_addr).await?;
        socket.connect(remote).await?;

        Ok(Self { socket, remote, connection: None })
    }

    pub async fn send_connect(&mut self) -> io::Result<()> {
        let mut buffer = [0u8; 16];

        let transaction_id: u32 = random();

        buffer[0..8].copy_from_slice(&0x41727101980u64.to_be_bytes());
        buffer[8..12].copy_from_slice(&0u32.to_be_bytes());
        buffer[12..16].copy_from_slice(&transaction_id.to_be_bytes());

        self.socket.send(&buffer).await?;

        let mut response = [0u8; 16];
        let len = self.socket.recv(&mut response).await?;

        if len != 16 {
            return Err(io::Error::other("Response is malformed"));
        }

        let action: u32 = u32::from_be_bytes(response[0..4].try_into().unwrap());
        let return_transaction_id: u32 = u32::from_be_bytes(response[4..8].try_into().unwrap());
        let conn: u64 = u64::from_be_bytes(response[8..16].try_into().unwrap());

        if action != 0 || transaction_id != return_transaction_id {
            return Err(io::Error::other("Bad Response"));
        }
        
        self.connection = Some(Connection { id: conn, expires: Instant::now() + Duration::from_secs(60) });

        Ok(())
    }

    async fn announce(&mut self, info_hash: &[u8; 20]) -> io::Result<UDPResponse> {
        let reconnect = match &self.connection {
            Some(conn) => Instant::now() >= conn.expires,
            None => true,
        };

        if reconnect {
            self.send_connect().await?;
        }

        self.send_announce(info_hash).await
    }

    // add stats later
    async fn send_announce(&mut self, info_hash: &[u8; 20]) -> io::Result<UDPResponse> {
        let mut buffer = [0u8; 98];

        let transaction_id: u32 = random();
        buffer[0..8].copy_from_slice(&self.connection.as_ref().unwrap().id.to_be_bytes());
        buffer[8..12].copy_from_slice(&1u32.to_be_bytes());
        buffer[12..16].copy_from_slice(&transaction_id.to_be_bytes());
        buffer[16..36].copy_from_slice(info_hash);
        buffer[36..56].copy_from_slice(&PEER_ID);

        // add downloaded, left, uploaded
        buffer[56..64].copy_from_slice(&0u64.to_be_bytes());
        buffer[64..72].copy_from_slice(&0u64.to_be_bytes());
        buffer[72..80].copy_from_slice(&0u64.to_be_bytes());

        // if (downloaded == 0) event = 2;  started
        // later when completed:
        // else if (downloaded == total) event = 1;
        // on shutdown:
        // event = 3;

        // event
        let event: u32 = 0;
        buffer[80..84].copy_from_slice(&event.to_be_bytes());
        buffer[84..88].copy_from_slice(&0u32.to_be_bytes());

        // let the tracker infer our ip address
        let rand: u32 = random();
        buffer[88..92].copy_from_slice(&rand.to_be_bytes());
        buffer[92..96].copy_from_slice(&0xFFFFFFFFu32.to_be_bytes());
        buffer[96..98].copy_from_slice(&PORT.to_be_bytes());

        self.socket.send(&buffer).await?;

        let mut response = [0u8; 1500];
        let bytes_read = self.socket.recv(&mut response).await?;

        if bytes_read < 20 {
            return Err(io::Error::other("Response is malformed"));
        }

        let response = &response[..bytes_read];

        let action = u32::from_be_bytes(response[0..4].try_into().unwrap());
        if action != 1 {
            return Err(io::Error::other("Action is not 1"));
        }

        let transaction_id_ret = u32::from_be_bytes(response[4..8].try_into().unwrap());
        if transaction_id != transaction_id_ret {
            return Err(io::Error::other("Transaction id mismatch"));
        }

        let interval = u32::from_be_bytes(response[8..12].try_into().unwrap());

        // what do we do with these?
        let leechers = u32::from_be_bytes(response[12..16].try_into().unwrap());
        let seeders = u32::from_be_bytes(response[16..20].try_into().unwrap());
        
        // we have now parsed 5 u32s = 20 bytes

        // peers from 20 onwards
        match self.remote {
            SocketAddr::V4(_) => {
                let peers = self.parse_v4_peers(&response[20..])?;
                return Ok(UDPResponse { peers, leechers, seeders, interval })
            }
            SocketAddr::V6(_) => {
                let peers = self.parse_peers_v6(&response[20..])?;
                return Ok(UDPResponse { peers, leechers, seeders, interval })
            }
        }
    }

    fn parse_v4_peers(&self, buffer: &[u8]) -> io::Result<Vec<Peer>> {
        let (chunks, remainder) = buffer.as_chunks::<6>();
        if !remainder.is_empty() {
            return Err(io::Error::other("Malformed ipv4 peer list"));
        }

        let peers = chunks
            .iter()
            .map(|&[a, b, c, d, p1, p2]| {
                let ip = Ipv4Addr::new(a, b, c, d);
                let port = u16::from_be_bytes([p1, p2]);

                Peer { addr: SocketAddr::from((ip, port)), id: None }
            })
            .collect();

        Ok(peers)
    }

    fn parse_peers_v6(&self, buffer: &[u8]) -> io::Result<Vec<Peer>> {
        let (chunks, remainder) = buffer.as_chunks::<18>();
        if !remainder.is_empty() {
            return Err(io::Error::other("Malformed ipv6 peer list"));
        } 

        let peers = chunks
            .iter()
            .map(|&[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, p1, p2]| {
                let ip = Ipv6Addr::from([a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p]);
                let port = u16::from_be_bytes([p1, p2]);

                Peer { addr: SocketAddr::from((ip, port)), id: None }
            })
            .collect();

        Ok(peers)
    }
}

pub struct Connection {
    pub id: u64,
    pub expires: Instant,
}

impl UDPTracker {
    pub fn new(url: String, info_hash: [u8; 20], sender: Sender<SessionEvent>) -> UDPTracker {
        UDPTracker {
            url,
            info_hash,
            ipv4: None,
            ipv6: None,
            leechers: 0,
            seeders: 0,
            sender
        }
    }

    async fn initialize(&mut self) -> tokio::io::Result<()> {
        let url = Url::parse(&self.url).unwrap();

        let host = url.host_str().unwrap();
        let port = url.port_or_known_default().unwrap();

        let results = lookup_host(format!("{host}:{port}")).await.unwrap();

        for addr in results {
            match addr {
                SocketAddr::V4(_) if self.ipv4.is_none() => {
                    self.ipv4 = Some(UDPContext::new(addr).await?);
                }
                SocketAddr::V6(_) if self.ipv6.is_none() => {
                    self.ipv6 = Some(UDPContext::new(addr).await?);
                }
                _ => { println!("What?"); }
            }
        }

        Ok(())
    }

    async fn perform_announce(&mut self) -> time::Duration {
        let (v4, v6) = tokio::join!(
            async {
                match self.ipv4.as_mut() {
                    Some(context) => {
                        context.announce(&self.info_hash).await
                    }
                    None => Err(io::Error::other("No IPV4 context")),
                }
            },

            async {
                match self.ipv6.as_mut() {
                    Some(context) => {
                        context.announce(&self.info_hash).await
                    }
                    None => Err(io::Error::other("No IPV6 context"))
                }
            }
        );

        let mut peers = Vec::new();
        let mut interval = DEFAULT_ANNOUNCE_TIMER;

        match (v4, v6) {
            (Err(e1), Err(e2)) => {
                let error = format!(
                    "IPv4: {}; IPv6: {}",
                    e1, e2
                );

                self.sender
                    .send(SessionEvent::AnnounceFailure { error })
                    .await
                    .ok();

                return Duration::from_secs(DEFAULT_ANNOUNCE_TIMER);
            }

            (v4, v6) => {
                if let Ok(response) = v4 {
                    peers.extend(response.peers);
                    interval = interval.min(response.interval as u64);
                    self.leechers = self.leechers.max(response.leechers);
                    self.seeders = self.seeders.max(response.seeders);
                }

                if let Ok(response) = v6 {
                    peers.extend(response.peers);
                    interval = interval.min(response.interval as u64);
                    self.leechers = self.leechers.max(response.leechers);
                    self.seeders = self.seeders.max(response.seeders);  
                }

                self.sender.send(SessionEvent::AnnounceSuccess { peers, warning: None }).await.ok();
            }
        }

        time::Duration::from_secs(interval)
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<TrackerCommand>) -> tokio::io::Result<()> {
        self.initialize().await?;
        let sleep = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                Some(cmd) = rx.recv() => {
                    match cmd {
                        TrackerCommand::Shutdown => break,
                        TrackerCommand::ForceReannounce => {
                            self.perform_announce().await;
                        }
                    }
                }

                _ = &mut sleep => {
                    let next = self.perform_announce().await;
                    sleep.as_mut().reset(tokio::time::Instant::now() + next);
                }
            }
        }
        Ok(())
    }
}
