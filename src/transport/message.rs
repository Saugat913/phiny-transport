use bytes::Bytes;
use tokio::sync::oneshot;

pub enum TransportMessage {
    GetNodeId(oneshot::Sender<String>),
    //connect to specific peer
    Connect(String),
    //disconnect from specific peer
    Disconnect(String),
    //send data to specific peer
    Send(String, Bytes),
    //shutdown the transport
    Shutdown,
}
