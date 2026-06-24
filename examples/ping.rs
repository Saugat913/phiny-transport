use anyhow::Context;
use iroh::{Endpoint, EndpointAddr, endpoint::presets};
use phiny_transport::TransportBuilder;



#[tokio::main]
async fn main()->anyhow::Result<()> {
   let transport = TransportBuilder::new().build().await.unwrap();
   let node_id = transport.get_node_id().await;
   println!("Node ID: {}", node_id);
   transport.send_to(vec![1, 2, 3].into(), "peer1".to_string()).await;
   transport.connect("peer1".to_string()).await;
   transport.disconnect("peer1".to_string()).await;
   transport.shutdown().await;
   Ok(())
}
