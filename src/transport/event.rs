use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum TransportEvent{
    NewConnectionArrived(String),
    ConnectionClosed(String),
    DataReceived(String, Bytes),
    Error(String),
}
