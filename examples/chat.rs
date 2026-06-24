use std::env;
use std::time::Duration;

use phiny_transport::{TransportBuilder, TransportEvent};
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let transport = TransportBuilder::new().build().await?;
    let node_id = transport.get_node_id().await;

    let args: Vec<String> = env::args().collect();
    let peer_id = args.get(1);

    if let Some(peer) = peer_id {
        println!("Connecting to {peer}...");
        transport.connect(peer.clone()).await;
    } else {
        println!("Your node ID: {node_id}");
        println!("Waiting for connection...");
    }

    let mut events = transport.subscribe();

    let connected_peer = if let Some(peer) = peer_id {
        let peer = peer.clone();
        let timeout = tokio::time::timeout(Duration::from_secs(15), async {
            while let Ok(event) = events.recv().await {
                if matches!(&event, TransportEvent::NewConnectionArrived(p) if p == &peer) {
                    break;
                }
            }
        });
        if timeout.await.is_err() {
            eprintln!("Connection timed out");
            return Ok(());
        }
        println!("Connected!");
        peer
    } else {
        loop {
            match events.recv().await {
                Ok(TransportEvent::NewConnectionArrived(peer)) => {
                    println!("Connected to {peer}");
                    break peer;
                }
                _ => continue,
            }
        }
    };

    let (line_tx, mut line_rx) = mpsc::channel::<String>(100);
    tokio::spawn(async move {
        let mut lines = BufReader::new(io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line_tx.send(line).await.is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            line = line_rx.recv() => {
                match line {
                    Some(line) => {
                        transport.send_to(line.into_bytes().into(), connected_peer.clone()).await;
                    }
                    None => break,
                }
            }
            event = events.recv() => {
                match event {
                    Ok(TransportEvent::DataReceived(_, data)) => {
                        println!("[peer]: {}", String::from_utf8_lossy(&data));
                    }
                    Ok(TransportEvent::ConnectionClosed(_)) => {
                        println!("Peer disconnected");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
