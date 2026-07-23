use std::{io::SeekFrom, path::PathBuf};

use bitvec::bitvec;
use sha1::{Digest, Sha1};
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc::{Receiver, Sender},
};

use crate::{
    disk::{Disk, DiskEvent, DiskResponse, OutputFile},
    parser::File,
};

impl Disk {
    pub fn new(
        torrent_name: String,
        total_len: u64,
        expected_piece_len: u64,
        piece_hashes: Vec<[u8; 20]>,
        disk_response_tx: Sender<DiskResponse>,
    ) -> Self {
        Self {
            output_files: Vec::new(),
            torrent_name,
            expected_piece_len,
            total_len,
            piece_hashes,
            disk_response_tx,
            savefile: None,
        }
    }

    pub async fn run(mut self, file_list: Option<Vec<File>>, mut rx: Receiver<DiskEvent>) {
        self.build_output_files(file_list).await;
        self.read_savefile().await;

        while let Some(cmd) = rx.recv().await {
            match cmd {
                DiskEvent::TryHashAndWrite { piece, data } => {
                    self.try_hash_and_write(piece, data).await;
                }
                DiskEvent::Shutdown => {
                    todo!();
                }
            }
        }
    }

    async fn read_savefile(&mut self) {
        let mut bitfield = bitvec!(0; self.piece_hashes.len());

        if let Some(savefile) = &mut self.savefile {
            loop {
                match savefile.read_u32_le().await {
                    Ok(index) => bitfield.set(index as usize, true),
                    Err(_) => break,
                }
            }
        }
        self.disk_response_tx
            .send(DiskResponse::CompletedPieces { bitfield })
            .await
            .ok();
    }

    async fn try_hash_and_write(&mut self, piece: u32, data: Vec<u8>) {
        if !self.do_hash(piece, &data) {
            self.disk_response_tx
                .send(DiskResponse::HashFailed { piece })
                .await
                .ok();
            return;
        }

        if !self.write(piece, data).await {
            self.disk_response_tx
                .send(DiskResponse::WriteFailed { piece })
                .await
                .ok();
            return;
        }

        self.disk_response_tx
            .send(DiskResponse::PieceHashedAndWritten { piece })
            .await
            .ok();

        if let Some(savefile) = &mut self.savefile {
            savefile.write_u32_le(piece).await.expect("Could not write to savefile");
        }
    }

    fn do_hash(&self, piece: u32, data: &[u8]) -> bool {
        let hashed: [u8; 20] = Sha1::digest(data).into();
        hashed == *self.piece_hashes.get(piece as usize).unwrap()
    }

    async fn write(&mut self, piece: u32, data: Vec<u8>) -> bool {
        let mut piece_offset: u64 = piece as u64 * self.expected_piece_len;
        let mut remaining: u64 = data.len() as u64;
        let mut data_offset: u64 = 0;

        // find the first file which is greater than the offset, then sub 1 because
        // we write from the previous file
        // basically the last true in an array of falsey values
        let mut idx = self
            .output_files
            .partition_point(|file| file.offset <= piece_offset)
            .saturating_sub(1);

        while remaining > 0 {
            let file = self.output_files.get_mut(idx).unwrap();
            let file_handle = &mut file.handle;

            let file_offset = piece_offset.saturating_sub(file.offset);

            let write_len = remaining.min(file.len - file_offset);

            if file_handle
                .seek(SeekFrom::Start(file_offset))
                .await
                .is_err()
            {
                return false;
            }
            if file_handle
                .write_all(&data[data_offset as usize..(data_offset + write_len) as usize])
                .await
                .is_err()
            {
                return false;
            }

            remaining -= write_len;
            data_offset += write_len;
            piece_offset += write_len;

            if remaining == 0 {
                break;
            }

            idx += 1;
        }

        true
    }

    async fn build_output_files(&mut self, file_list: Option<Vec<File>>) {
        let base = PathBuf::from(&self.torrent_name);

        if let Some(ref file_list) = file_list {
            let mut offset: u64 = 0;

            for file in file_list {
                let mut path = base.clone();
                for comp in &file.path {
                    path.push(comp);
                }

                if let Some(path) = path.parent() {
                    tokio::fs::create_dir_all(path)
                        .await
                        .expect("Failed to create directory2");
                }

                let handle = tokio::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .await
                    .expect("Failed to open multi file handles");
                handle
                    .set_len(file.length)
                    .await
                    .expect("Could not preallocate");

                self.output_files.push(OutputFile {
                    len: file.length,
                    offset,
                    handle,
                });

                offset += file.length;
            }
        } else {
            let handle = tokio::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&base)
                .await
                .expect("Failed to create single file");
            handle
                .set_len(self.total_len)
                .await
                .expect("Could not preallocate");

            self.output_files.push(OutputFile {
                len: self.total_len,
                offset: 0,
                handle,
            });
        }

        let savefile_path = if file_list.is_some() {
            base.join(format!("{}.fastresume", self.torrent_name))
        } else {
            PathBuf::from(format!("{}.fastresume", self.torrent_name))
        };

        self.savefile = Some(
            fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(savefile_path)
                .await
                .expect("Failed to create savefile"),
        );

        self.output_files.sort_by_key(|a| a.offset);
    }
}
