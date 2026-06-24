use phiny_transport::actor::TransportActor;



#[tokio::main]
async fn main() {
   let transport = TransportActor::spawn();
   transport.send_to(vec![1, 2, 3], "peer1".to_string()).await;
   transport.connect("peer1".to_string()).await;
   transport.disconnect("peer1".to_string()).await;
   transport.shutdown().await;
}
