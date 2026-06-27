use bytes::Bytes;
use tokio::sync::oneshot;

pub enum TransportMessage {
    /// Get the node ID as a string
    GetNodeId(oneshot::Sender<String>),
    /// Get the secret key as bytes
    GetSecretKey(oneshot::Sender<Vec<u8>>),
    /// Connect to a specific peer
    Connect(String),
    /// Disconnect from a specific peer
    Disconnect(String),
    /// Send data to a specific peer
    Send(String, Bytes),
    /// Shutdown the transport
    Shutdown,
}
