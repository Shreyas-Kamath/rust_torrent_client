use serde::Deserialize;

// use crate::trackers::udp::UDPTracker;
use crate::trackers::http::HTTPTracker;

pub enum TrackerCommand {
    ForceReannounce,
    Shutdown,
}

#[derive(Debug, Deserialize)]
pub struct TrackerResponse {
    pub interval: u32,

    pub peers: serde_bencode::value::Value,

    pub peers6: Option<serde_bencode::value::Value>,

    #[serde(rename = "failure reason")]
    pub error: Option<String>,

    #[serde(rename = "warning message")]
    pub warning_message: Option<String>,
}

pub enum Tracker {
    // UDP(UDPTracker),
    HTTP(HTTPTracker),
}
