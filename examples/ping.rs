use std::time::Duration;

use phiny_transport::{TransportBuilder, TransportEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();

    let peer_a = tokio::spawn(async move {
        let transport = TransportBuilder::new().build().await.unwrap();
        let node_id = transport.get_node_id().await;
        println!("A: {node_id}");

        tokio::time::sleep(Duration::from_secs(2)).await;
        tx.send(node_id).unwrap();

        let mut events = transport.subscribe();
        while let Ok(event) = events.recv().await {
            match event {
                TransportEvent::DataReceived(peer, data) => {
                    println!("A: got '{}' from {peer}", String::from_utf8_lossy(&data));
                    transport.send_to(b"pong".to_vec().into(), peer).await;
                    break;
                }
                TransportEvent::NewConnectionArrived(peer) => {
                    println!("A: new connection from {peer}");
                }
                _ => {}
            }
        }
    });

    let peer_b = tokio::spawn(async move {
        let peer_a_id = rx.await.unwrap();
        let transport = TransportBuilder::new().build().await.unwrap();
        println!("B: {}", transport.get_node_id().await);

        let mut events = transport.subscribe();
        transport.connect(peer_a_id.clone()).await;

        while let Ok(event) = events.recv().await {
            match event {
                TransportEvent::NewConnectionArrived(id) if id == peer_a_id => {
                    println!("B: connected, sending ping");
                    transport.send_to(b"ping".to_vec().into(), peer_a_id.clone()).await;
                }
                TransportEvent::DataReceived(_, data) => {
                    println!("B: got '{}'", String::from_utf8_lossy(&data));
                    break;
                }
                _ => {}
            }
        }
    });

    let _ = tokio::join!(peer_a, peer_b);
    Ok(())
}
