use iroh::endpoint::{Connection, RecvStream, SendStream};
use tokio::sync::{broadcast, mpsc};
use anyhow::Result;
use crate::connection::{handle::ConnectionHandle, message::ConnectionMessage};
use crate::transport::event::TransportEvent;

pub struct ConnectionActor {
    send_stream: SendStream,
}

impl ConnectionActor {
    pub async fn spawn(
        connection: Connection,
        peer_id: String,
        event_tx: broadcast::Sender<TransportEvent>,
        initiator: bool,
    ) -> Result<ConnectionHandle> {
        let (msg_tx, mut msg_rx) = mpsc::channel(10);
        let (send_stream, recv_stream) = if initiator {
            connection.open_bi().await?
        } else {
            connection.accept_bi().await?
        };

        Self::spawn_recv_loop(recv_stream, peer_id, event_tx);

        let mut actor = Self { send_stream };
        tokio::spawn(async move {
            while let Some(message) = msg_rx.recv().await {
                match message {
                    ConnectionMessage::Send(bytes) => {
                        actor.send_stream.write_all(&bytes).await?;
                    }
                    ConnectionMessage::Shutdown => break,
                }
            }
            Ok::<(), anyhow::Error>(())
        });
        Ok(ConnectionHandle::new(msg_tx))
    }

    fn spawn_recv_loop(
        mut recv_stream: RecvStream,
        peer_id: String,
        event_tx: broadcast::Sender<TransportEvent>,
    ) {
        tokio::spawn(async move {
            let mut buffer = [0u8; 1024];
            loop {
                match recv_stream.read(&mut buffer).await {
                    Ok(Some(size)) => {
                        let _ = event_tx.send(TransportEvent::DataReceived(
                            peer_id.clone(),
                            buffer[..size].to_vec().into(),
                        ));
                    }
                    Ok(None) => {
                        let _ = event_tx.send(TransportEvent::ConnectionClosed(peer_id));
                        break;
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

