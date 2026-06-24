use std::sync::Arc;

use tokio::sync::mpsc;
use crate::message::TransportMessage;


#[derive(Debug,Clone)]
pub struct TransportHandle {
    pub message_channel: mpsc::Sender<TransportMessage>,
    pub worker_handle: Arc<tokio::task::JoinHandle<()>>,
}

impl TransportHandle {
    pub async fn shutdown(&self) {
        self.worker_handle.abort();
    }
    pub async fn send_to(&self, data: Vec<u8>, peer_id: String) {
        self.message_channel.send(TransportMessage::Send(peer_id, data)).await.unwrap();
    }
    pub async fn connect(&self, peer_id: String) {
        self.message_channel.send(TransportMessage::Connect(peer_id)).await.unwrap();
    }
    pub async fn disconnect(&self, peer_id: String) {
        self.message_channel.send(TransportMessage::Disconnect(peer_id)).await.unwrap();
    }
}
