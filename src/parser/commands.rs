use serde::{Deserialize, Serialize};
use serde_bytes;

#[derive(Deserialize, Debug)]
pub struct Torrent {
    pub announce: String,
    #[serde(rename = "announce-list")]
    pub announce_list: Option<Vec<Vec<String>>>,
    pub info: Info,

    #[serde(skip)]
    pub total_length: u64,
    #[serde(skip)]
    pub info_hash: [u8; 20],
}

#[derive(Deserialize, Debug, Serialize)]
pub struct Info {
    pub name: String,
    #[serde(rename = "piece length")]
    pub piece_length: u64,

    #[serde(with = "serde_bytes")]
    pub pieces: Vec<u8>,

    // optionals
    pub length: Option<u64>,
    pub files: Option<Vec<File>>,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct File {
    pub length: u64,
    pub path: Vec<String>,
}
