use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};
use crate::transport::{event::TransportEvent, message::TransportMessage};

#[derive(Debug, Clone)]
pub struct TransportHandle {
    pub message_channel: mpsc::Sender<TransportMessage>,
    pub broadcast_tx: broadcast::Sender<TransportEvent>,
}

impl TransportHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<TransportEvent> {
        self.broadcast_tx.subscribe()
    }

    pub async fn shutdown(self) {
        self.message_channel
            .send(TransportMessage::Shutdown)
            .await
            .unwrap();
    }
    pub async fn send_to(&self, data: Bytes, peer_id: String) {
        self.message_channel
            .send(TransportMessage::Send(peer_id, data))
            .await
            .unwrap();
    }
    pub async fn connect(&self, peer_id: String) {
        self.message_channel
            .send(TransportMessage::Connect(peer_id))
            .await
            .unwrap();
    }
    pub async fn disconnect(&self, peer_id: String) {
        self.message_channel
            .send(TransportMessage::Disconnect(peer_id))
            .await
            .unwrap();
    }
    pub async fn get_node_id(&self) -> String {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.message_channel
            .send(TransportMessage::GetNodeId(tx))
            .await
            .unwrap();
        rx.await.unwrap()
    }
}
