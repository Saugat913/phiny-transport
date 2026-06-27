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

    pub fn with_secret_key(mut self, secret_key: Vec<u8>) -> Self {
        let secret_key = secret_key.try_into().unwrap();
        self.config.secret_key = Some(secret_key);
        self
    }

    pub async fn build(self) -> anyhow::Result<TransportHandle> {
        super::actor::TransportActor::spawn(self.config).await
    }
}
