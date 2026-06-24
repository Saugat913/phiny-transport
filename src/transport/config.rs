#[derive(Debug, Clone)]
pub struct TransportConfig {
   pub endpoint_id:Option<String>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            endpoint_id: None,
        }
    }
}