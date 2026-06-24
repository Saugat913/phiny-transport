use std::sync::Arc;

use tokio::sync::mpsc;

use crate::{handle::TransportHandle, message::TransportMessage};

pub struct TransportActor {}

impl TransportActor {
    pub fn spawn() -> TransportHandle {
        let (tx, mut rx) = mpsc::channel(100);
        let actor = Self {};
        let worker_handle = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let _ = actor.handle_message(msg).await;
            }
        });
        TransportHandle {
            message_channel: tx,
            worker_handle: Arc::new(worker_handle),
        }
    }

    async fn handle_message(&self, message: TransportMessage) {
        match message {
            TransportMessage::Connect(peer_id) => {
                // Handle connect
                println!("Connecting to peer: {}", peer_id);
            }
            TransportMessage::Disconnect(peer_id) => {
                // Handle disconnect
                println!("Disconnecting from peer: {}", peer_id);
            }
            TransportMessage::Send(peer_id, data) => {
                // Handle send
                println!("Sending data to peer: {}", peer_id);
            }
        }
    }
}
