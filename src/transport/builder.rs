use crate::transport::{config::TransportConfig, handle::TransportHandle};

pub struct TransportBuilder {
    config: TransportConfig,
}

impl TransportBuilder {
    pub fn new() -> Self {
        Self {
            config: TransportConfig::default(),
        }
    }

    pub fn with_endpoint_id(mut self, endpoint_id: String) -> Self {
        self.config.endpoint_id = Some(endpoint_id);
        self
    }

    pub async fn build(self) -> anyhow::Result<TransportHandle> {
        super::actor::TransportActor::spawn(self.config).await
    }
}
