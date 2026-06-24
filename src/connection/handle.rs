use bytes::Bytes;
use tokio::sync::mpsc;

use crate::connection::message::ConnectionMessage;

#[derive(Clone)]
pub struct ConnectionHandle {
    tx: mpsc::Sender<ConnectionMessage>,
}

impl ConnectionHandle {
    pub fn new(tx: mpsc::Sender<ConnectionMessage>) -> Self {
        Self { tx }
    }

    pub async fn send(&self, data: Bytes) {
        let _ = self.tx.send(ConnectionMessage::Send(data)).await;
    }

    pub async fn shutdown(&self) {
        let _ = self.tx.send(ConnectionMessage::Shutdown).await;
    }
}