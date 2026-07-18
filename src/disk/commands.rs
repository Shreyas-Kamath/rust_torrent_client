use bitvec::vec::BitVec;
use tokio::{fs, sync::mpsc::Sender};

pub struct OutputFile {
    pub len: u64,
    pub offset: u64,
    pub handle: fs::File,
}

pub struct Disk {
    pub output_files: Vec<OutputFile>,
    pub torrent_name: String,
    pub expected_piece_len: u64,
    pub total_len: u64,
    pub piece_hashes: Vec<[u8; 20]>,
    pub disk_response_tx: Sender<DiskResponse>,
}

pub enum DiskEvent {
    TryHashAndWrite { piece: u32, data: Vec<u8> },
    Shutdown,
    GetAlreadyCompleted,
}

pub enum DiskResponse {
    CompletedPieces { bitfield: BitVec },
    HashFailed { piece: u32 },
    WriteFailed { piece: u32 },
    PieceHashedAndWritten { piece: u32 },
}
