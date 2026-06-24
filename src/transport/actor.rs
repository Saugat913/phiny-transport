use std::sync::Arc;

use crate::{
    connection::{ConnectionActor, ConnectionHandle}, transport::{
        builder::TransportBuilder, config::TransportConfig, handle::TransportHandle,
        message::TransportMessage,
    },
};
use anyhow::Result;
use dashmap::DashMap;
use iroh::{Endpoint, endpoint::presets::N0};
use log::info;
use tokio::sync::mpsc;

pub struct TransportActor {
    endpoint: Endpoint,
    connections: Arc<DashMap<String, ConnectionHandle>>,
}

impl TransportActor {
    pub fn builder() -> TransportBuilder {
        TransportBuilder::new()
    }

    pub async fn spawn(config: TransportConfig) -> Result<TransportHandle> {
        let (tx, mut rx) = mpsc::channel(100);
        let endpoint = Endpoint::builder(N0).bind().await?;

        let connections =Arc::new( DashMap::new());
        let connections_cloned= connections.clone();

        let actor = Self {
            endpoint: endpoint.clone(),
            connections,
        };

        tokio::spawn(async move {
            info!("Transport actor accept loop started");
            while let Some(connection) = endpoint.accept().await {
                info!("Accepted connection from: {:?}", connection);
                let connection = connection.await.unwrap();
                let connection_id= connection.remote_id().to_string();
                let handle = ConnectionActor::spawn(connection).await.unwrap();
                info!("Spawned connection handler with handle");
                connections_cloned.insert(connection_id, handle);
            }
        });

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    TransportMessage::Shutdown => {
                        actor.endpoint.close().await;
                        break;
                    }
                    _ => {
                        let _ = actor.handle_message(msg).await;
                    }
                }
            }
        });
        Ok(TransportHandle {
            message_channel: tx,
        })
    }

    async fn handle_message(&self, message: TransportMessage) {
        match message {
            TransportMessage::GetNodeId(tx) => {
                let _ = tx.send(self.endpoint.id().to_string());
            }
            TransportMessage::Connect(peer_id) => {
                // Handle connect
                info!("Connecting to peer: {}", peer_id);
            }
            TransportMessage::Disconnect(peer_id) => {
                // Handle disconnect
                info!("Disconnecting from peer: {}", peer_id);
                if let Some(handle) = self.connections.get(&peer_id) {
                    handle.shutdown().await;
                    self.connections.remove(&peer_id);
                }
            }
            TransportMessage::Send(peer_id, data) => {
                // Handle send
                info!("Sending data {:?} to peer: {}", data, peer_id);
                if let Some(handle) = self.connections.get(&peer_id) {
                    handle.send(data).await;
                }
            }
            TransportMessage::Shutdown => {
                // Handle shutdown
                info!("Shutting down transport");
            }
        }
    }
}
