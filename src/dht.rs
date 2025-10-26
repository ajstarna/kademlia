use crate::{
    node::identifier::Key,
    node::storage::Value,
};

use crate::protocol::{ProtocolManager, Command};

use tokio::net::UdpSocket;
use tokio::{sync::{mpsc, oneshot}};

pub struct KademliaDHT {
    tx: mpsc::Sender<Command>,
    local_addr: std::net::SocketAddr,
}


impl KademliaDHT {
    pub async fn start() -> anyhow::Result<Self> {
        Self::start_on("0.0.0.0:8080").await
    }

    /// Start a DHT instance bound to a specific address (e.g., "127.0.0.1:0" for ephemeral).
    pub async fn start_on(bind_addr: &str) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel::<Command>(100);
        let socket = UdpSocket::bind(bind_addr).await?;
        let local_addr = socket.local_addr()?;
        let k = 20;
        let alpha = 3;
        let manager = ProtocolManager::new(socket, rx, k, alpha)?;

        tokio::spawn(manager.run());

        Ok(Self { tx, local_addr })
    }

    /// Return the local UDP socket address this DHT instance is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr { self.local_addr }

    /// Put a key,value into the dht. Returns a bool whether it succeeded or not,
    /// i.e. did at least a single node in the network successfully store the value.

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
