use iced::{
    Element, Task,
    widget::{button, column, text},
};
use tokio::sync::mpsc;

use crate::ui::commands::{App, Message, UIToClientCommand};
use rfd::{self, AsyncFileDialog};

impl App {
    fn add_torrent(&self) -> Task<Message> {
        Task::perform(
            async {
                let file_handle = AsyncFileDialog::new()
                    .set_title("Select a .torrent file")
                    .add_filter("Torrent files", &["torrent"])
                    .pick_file()
                    .await;
                file_handle
            },
            Message::TorrentLoaded,
        )
    }
    fn on_torrent_add(&self, result: Option<rfd::FileHandle>) -> Task<Message> {
        match result {
            Some(handle) => {
                let tx = self.client_tx.clone();

                Task::perform(
                    async move {
                        tx.send(UIToClientCommand::AddTorrent {
                            path: handle.path().to_path_buf(),
                        })
                        .await
                        .unwrap();
                    },
                    |_| Message::Nothing,
                )
            }

            None => Task::none(),
        }
    }
    pub fn new(client_tx: mpsc::Sender<UIToClientCommand>) -> Self {
        Self {
            torrent_count: 0,
            client_tx,
        }
    }
    pub fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::AddTorrent => self.add_torrent(),
            Message::TorrentLoaded(result) => self.on_torrent_add(result),
            Message::Nothing => Task::none(),
        }
    }
    pub fn view(&self) -> Element<'_, Message> {
        column![
            button("Add Torrent").on_press(Message::AddTorrent),
            text(format!("You have {} torrents.", self.torrent_count)),
        ]
        .spacing(20)
        .padding(20)
        .into()
    }
}
