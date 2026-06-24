use std::sync::Arc;

use crate::{
    connection::{ConnectionActor, ConnectionHandle},
    transport::{
        config::TransportConfig, event::TransportEvent, handle::TransportHandle,
        message::TransportMessage,
    },
};
use anyhow::Result;
use dashmap::DashMap;
use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets::N0};
use log::info;
use tokio::sync::{broadcast, mpsc};

const ALPN: &[u8] = b"phiny/1";

pub struct TransportActor {
    endpoint: Endpoint,
    event_tx: broadcast::Sender<TransportEvent>,
    connections: Arc<DashMap<String, ConnectionHandle>>,
}

impl TransportActor {
    pub async fn spawn(config: TransportConfig) -> Result<TransportHandle> {
        let (tx, mut rx) = mpsc::channel(100);
        let secret_key = config.secret_key.map(SecretKey::from);
        let endpoint = Self::setup_endpoint(secret_key).await?;

        let connections = Arc::new(DashMap::new());
        let connections_cloned = connections.clone();

        let (broadcast_tx, _) = broadcast::channel(256);

        let actor = Self {
            endpoint: endpoint.clone(),
            event_tx: broadcast_tx.clone(),
            connections,
        };

        let accept_broadcast = broadcast_tx.clone();
        tokio::spawn(async move {
            info!("Transport actor accept loop started");
            while let Some(incoming) = endpoint.accept().await {
                info!("Accepted connection from: {:?}", incoming);
                let connection = incoming.await.unwrap();
                let peer_id = connection.remote_id().to_string();
                let handle = ConnectionActor::spawn(
                    connection,
                    peer_id.clone(),
                    accept_broadcast.clone(),
                    false,
                )
                .await
                .unwrap();
                info!("Spawned connection handler with handle");
                connections_cloned.insert(peer_id.clone(), handle);
                let _ = accept_broadcast.send(TransportEvent::NewConnectionArrived(peer_id));
            }
        });
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    TransportMessage::Shutdown => {
                        actor.endpoint.close().await;
                        for connection in actor.connections.iter() {
                            connection.value().shutdown().await;
                        }
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
            broadcast_tx,
        })
    }

    async fn setup_endpoint(secret_key: Option<SecretKey>) -> Result<Endpoint> {
        let mut builder = Endpoint::builder(N0).alpns(vec![ALPN.to_vec()]);
        if let Some(sk) = secret_key {
            builder = builder.secret_key(sk);
        }
        Ok(builder.bind().await?)
    }

    async fn handle_message(&self, message: TransportMessage) {
        match message {
            TransportMessage::GetNodeId(tx) => {
                let _ = tx.send(self.endpoint.id().to_string());
            }
            TransportMessage::Connect(peer_id) => self.handle_connect(peer_id).await,
            TransportMessage::Disconnect(peer_id) => {
                info!("Disconnecting from peer: {}", peer_id);
                if let Some(handle) = self.connections.get(&peer_id) {
                    handle.shutdown().await;
                    self.connections.remove(&peer_id);
                }
            }
            TransportMessage::Send(peer_id, data) => {
                info!("Sending data {:?} to peer: {}", data, peer_id);
                if let Some(handle) = self.connections.get(&peer_id) {
                    handle.send(data).await;
                }
            }
            TransportMessage::Shutdown => unreachable!(),
        }
    }

    async fn handle_connect(&self, peer_id: String) {
        info!("Connecting to peer: {}", peer_id);
        match peer_id.parse::<EndpointId>() {
            Ok(node_id) => {
                info!("Parsed node ID: {:?}", node_id);
                let conn = self.endpoint.connect(node_id, ALPN).await;
                match conn {
                    Ok(connection) => {
                        let handle = ConnectionActor::spawn(
                            connection,
                            peer_id.clone(),
                            self.event_tx.clone(),
                            true,
                        )
                        .await
                        .unwrap();
                        self.connections.insert(peer_id.clone(), handle);
                        let _ = self
                            .event_tx
                            .send(TransportEvent::NewConnectionArrived(peer_id));
                    }
                    Err(e) => {
                        let _ = self.event_tx.send(TransportEvent::Error(format!(
                            "connect failed: {e}"
                        )));
                    }
                }
            }
            Err(e) => {
                let _ = self
                    .event_tx
                    .send(TransportEvent::Error(format!("invalid peer id: {e}")));
            }
        }
    }
}
