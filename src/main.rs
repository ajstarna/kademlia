use tokio::net::UdpSocket;
use kademlia::protocol::ProtocolManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:8080").await?;
    let k = 20;
    let alpha = 3;
    let manager = ProtocolManager::new_headless(socket, k, alpha)?;

    manager.run().await;
    Ok(())
}
