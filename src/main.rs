use tokio::net::UdpSocket;

mod node;
mod protocol;
#[cfg(test)]
mod test_support;

use node::Node;
use protocol::ProtocolManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:8080").await?;
    let addr = socket.local_addr()?;
    let ip = addr.ip();
    let port = addr.port();

    let k = 20;
    let node = Node::new(k, ip, port);
    let alpha = 3;
    let manager = ProtocolManager::new(node, socket, k, alpha);

    manager.run().await;
    Ok(())
}
