use iroh::endpoint::{Connection, RecvStream, SendStream};
use tokio::sync::mpsc;
use anyhow::Result;
use crate::connection::{handle::ConnectionHandle, message::ConnectionMessage};

pub struct ConnectionActor {
    send_stream: SendStream,
}


impl ConnectionActor {
    pub async fn spawn(connection:Connection) -> Result<ConnectionHandle> {
        let(msg_tx,mut msg_rx)= mpsc::channel(10);
        let (send_stream,recv_stream)= connection.accept_bi().await?;

        Self::spawn_accept_loop(recv_stream).await;

        let mut actor = Self {
            send_stream:send_stream
        };
        tokio::spawn(async move{
            while let Some(message) = msg_rx.recv().await {
                match message {
                    ConnectionMessage::Send(bytes) => {
                        actor.send_stream.write_all(&bytes).await?;
                    }
                    ConnectionMessage::Shutdown => {
                        break;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
       Ok(ConnectionHandle::new(msg_tx))
    }

    async fn spawn_accept_loop(mut recv_stream: RecvStream){
        tokio::spawn(async move{
            let mut buffer = [0u8; 1024];
            while let Ok(result) = recv_stream.read(&mut buffer).await {
                match result {
                    Some(size) => {
                        // Handle received data
                    }
                    None => {
                        // Handle error
                        break;
                    }
                }
            }
        });
    }
}

