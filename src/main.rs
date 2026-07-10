use iced;

mod client;
mod parser;
mod peers;
mod scheduler;
mod session;
mod trackers;
mod ui;

use crate::ui::commands::{App, Message};
use iced::Task;
use tokio::sync::mpsc;

fn boot() -> (App, Task<Message>) {
    let (tx, rx) = mpsc::channel(32);

    tokio::spawn(client::run(rx));
    (App::new(tx), Task::none())
}

fn main() -> iced::Result {
    iced::application(boot, App::update, App::view)
        .title("Rust Torrent")
        .run()
}
