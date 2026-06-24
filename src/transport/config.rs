#[derive(Debug, Clone)]
pub struct TransportConfig {
   pub secret_key:Option<[u8; 32]>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            secret_key: None,
        }
    }
}