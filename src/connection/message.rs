use bytes::Bytes;

#[derive(Debug)]
pub enum ConnectionMessage {
    Send(Bytes),
    Shutdown,
}
