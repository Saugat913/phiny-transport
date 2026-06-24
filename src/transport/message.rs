pub enum TransportMessage {
    //connect to specific peer
    Connect(String),
    //disconnect from specific peer
    Disconnect(String),
    //send data to specific peer
    Send(String, Vec<u8>),
}
