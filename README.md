# phiny-transport

Minimal P2P messaging over iroh (QUIC + relay + DHT). Nodes discover each other by node ID and exchange bytes.

## Quick start

```rust
use phiny_transport::{TransportBuilder, TransportEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let transport = TransportBuilder::new().build().await?;
    let my_id = transport.get_node_id().await;

    // share my_id with a peer out of band

    // connect or wait for incoming
    transport.connect(peer_id).await;

    let mut events = transport.subscribe();
    while let Ok(event) = events.recv().await {
        match event {
            TransportEvent::NewConnectionArrived(id) => {
                transport.send_to(b"hello".into(), id).await;
            }
            TransportEvent::DataReceived(_, data) => {
                println!("{}", String::from_utf8_lossy(&data));
            }
            TransportEvent::ConnectionClosed(_) => break,
            TransportEvent::Error(e) => eprintln!("{e}"),
        }
    }
    Ok(())
}
```

## Examples

```
cargo run --example ping           # listen
cargo run --example ping <node_id> # connect + ping
cargo run --example chat           # listen
cargo run --example chat <node_id> # connect + chat
```
