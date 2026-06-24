use std::env;

use phiny_transport::{TransportBuilder, TransportEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let transport = TransportBuilder::new().build().await?;
    let node_id = transport.get_node_id().await;

    let args: Vec<String> = env::args().collect();
    let peer_id = args.get(1);

    if let Some(peer) = peer_id {
        println!("Pinging {peer}...");
        transport.connect(peer.clone()).await;
        let mut events = transport.subscribe();
        while let Ok(event) = events.recv().await {
            match event {
                TransportEvent::NewConnectionArrived(id) if id == *peer => {
                    transport.send_to(b"ping".to_vec().into(), peer.clone()).await;
                }
                TransportEvent::DataReceived(_, data) => {
                    println!("pong: {}", String::from_utf8_lossy(&data));
                    break;
                }
                _ => {}
            }
        }
    } else {
        println!("{node_id}");
        let mut events = transport.subscribe();
        while let Ok(event) = events.recv().await {
            match event {
                TransportEvent::DataReceived(peer, _) => {
                    transport.send_to(b"pong".to_vec().into(), peer).await;
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    break;
                }
                _ => {}
            }
        }
    }
    Ok(())
}
