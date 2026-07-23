use crate::parser::parse_response;
use crate::session::commands::SessionEvent;
use crate::{
    parser::parse_peers,
    trackers::{commands::TrackerCommand, tracker::DEFAULT_ANNOUNCE_TIMER},
};
use std::fmt::Write;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct HTTPTracker {
    url: String,
    http_client: reqwest::Client,
    pct_encoded_info_hash: String,
    sender: mpsc::Sender<SessionEvent>,
}

impl HTTPTracker {
    pub fn new(
        url: String,
        http_client: reqwest::Client,
        info_hash: [u8; 20],
        sender: mpsc::Sender<SessionEvent>,
    ) -> HTTPTracker {
        HTTPTracker {
            url,
            http_client,
            pct_encoded_info_hash: percent_encode(info_hash),
            sender,
        }
    }

    async fn perform_announce(&mut self) -> tokio::time::Duration {
        match self.announce().await {
            Ok(result) => match parse_response(&result) {
                Ok(parsed) => {
                    let peers = parse_peers(&parsed.peers, &parsed.peers6);

                    if let Some(error) = parsed.error {
                        self.sender
                            .send(SessionEvent::AnnounceFailure { error })
                            .await
                            .ok();
                        return tokio::time::Duration::from_secs(DEFAULT_ANNOUNCE_TIMER);
                    }

                    self.sender
                        .send(SessionEvent::AnnounceSuccess {
                            peers,
                            warning: parsed.warning_message,
                        })
                        .await
                        .ok();
                    tokio::time::Duration::from_secs(parsed.interval as u64)
                }

                Err(e) => {
                    self.sender
                        .send(SessionEvent::AnnounceFailure {
                            error: e.to_string(),
                        })
                        .await
                        .ok();
                    tokio::time::Duration::from_secs(DEFAULT_ANNOUNCE_TIMER)
                }
            },

            Err(e) => {
                self.sender
                    .send(SessionEvent::AnnounceFailure {
                        error: e.to_string(),
                    })
                    .await
                    .ok();

                tokio::time::Duration::from_secs(DEFAULT_ANNOUNCE_TIMER)
            }
        }
    }

    async fn announce(&mut self) -> Result<Vec<u8>, reqwest::Error> {
        let url = format!(
            "{}?info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1",
            self.url, self.pct_encoded_info_hash, "-TR2940-1234567890ab", 6881, 0, 0, 0,
        );

        let resp = self.http_client.get(url).send().await?;
        let bytes = resp.bytes().await?;

        Ok(bytes.into())
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<TrackerCommand>) -> tokio::io::Result<()> {
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

fn percent_encode(info_hash: [u8; 20]) -> String {
    let mut pct_encoded = String::with_capacity(info_hash.len() * 3);

    for b in info_hash {
        write!(&mut pct_encoded, "%{:02X}", b).unwrap();
    }

    pct_encoded
}
