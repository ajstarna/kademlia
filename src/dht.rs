use crate::{
    node::identifier::Key,
    node::storage::Value,
};

use crate::protocol::{ProtocolManager, Command};

use tokio::net::UdpSocket;
use tokio::{sync::{mpsc, oneshot}};

// TODO: these local nodes will only work for testing
const DEFAULT_BOOTSTRAP_NODES: [&str; 2] = ["127.0.0.1:4001", "127.0.0.1:4002"];

pub struct KademliaDHT {
    tx: mpsc::Sender<Command>,
    local_addr: std::net::SocketAddr,
}


impl KademliaDHT {

    /// Start a DHT instance bound to a specific address (e.g., "127.0.0.1:0" for ephemeral).
    pub async fn start(bind_addr: &str, bootstrap_nodes: Option<Vec<&str>>) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel::<Command>(100);
        let socket = UdpSocket::bind(bind_addr).await?;
        let local_addr = socket.local_addr()?;
        let k = 20;
        let alpha = 3;
        let manager = ProtocolManager::new(socket, rx, k, alpha)?;

        // Determine bootstrap nodes: use provided list or fall back to defaults
        let bootstrap_nodes: Vec<&str> = match bootstrap_nodes {
            Some(nodes) => nodes,
            None => DEFAULT_BOOTSTRAP_NODES.to_vec(),
        };

        // Start the main loop first so we don't miss fast replies
        tokio::spawn(manager.run());

        // Send a Bootstrap command so the manager issues FindNode(self) to seeds
        let addrs: Vec<std::net::SocketAddr> = bootstrap_nodes
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        // Fire-and-forget; if the channel is closed, surface the error to the caller
        // before returning the DHT handle
        mpsc::Sender::clone(&tx)
            .send(Command::Bootstrap { addrs })
            .await?;

        Ok(Self { tx, local_addr })
    }

    /// Put a key,value into the dht. Returns a bool whether it succeeded or not,
    pub async fn put(&self, key: Key, value: Value) -> anyhow::Result<bool> {
	let (tx, rx) = oneshot::channel::<bool>();
        self.tx.send(Command::Put { key, value, rx: tx }).await?;
        Ok(rx.await?)
    }

    pub async fn get(&self, key: Key) -> anyhow::Result<Option<Value>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Get { key, rx: tx }).await?;
        Ok(rx.await?)
    }


}
