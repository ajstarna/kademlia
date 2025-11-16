use crate::{
    core::identifier::{Key, NodeInfo},
    core::storage::Value,
};

use crate::protocol::{Command, ProtocolManager};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};

pub struct KademliaDHT {
    tx: mpsc::Sender<Command>,
    _local_addr: std::net::SocketAddr,
    pub node_info: NodeInfo,
}

impl KademliaDHT {
    /// Start a DHT client (non-storing) bound to a specific address (e.g., "127.0.0.1:0").
    /// Uses client role: does not store values and advertises is_client so others avoid routing us.
    pub async fn start_client(
        bind_addr: &str,
        bootstrap_addrs: Vec<std::net::SocketAddr>,
        k: usize,
        alpha: usize,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel::<Command>(100);
        let socket = UdpSocket::bind(bind_addr).await?;
        let local_addr = socket.local_addr()?;
        let manager = ProtocolManager::new_client(socket, rx, k, alpha)?;
        let node_info = manager.node.my_info;

        // Start the main loop first so we don't miss fast replies
        tokio::spawn(manager.run());

        // Now that the manager is running, we send our bootstrap command.
        // This initiates a lookup of our own node id, which populates our routing table with
        // nodes close to ours (as specified by the Kademlia paper).
        let (btx, brx) = oneshot::channel::<()>();
        let seeds_len = bootstrap_addrs.len();
        mpsc::Sender::clone(&tx)
            .send(Command::Bootstrap {
                addrs: bootstrap_addrs,
                tx_done: btx,
            })
            .await?;
        // Wait for bootstrap convergence with a timeout to avoid hanging on bad seeds
        let timeout = std::time::Duration::from_secs(3);
        match tokio::time::timeout(timeout, brx).await {
            Ok(Ok(_)) => {}
            Ok(Err(_canceled)) => {
                return Err(anyhow::anyhow!("bootstrap canceled by protocol task"))
            }
            Err(_elapsed) => {
                return Err(anyhow::anyhow!(
                    "bootstrap timed out after {:?} (seeds={})",
                    timeout,
                    seeds_len
                ))
            }
        }

        // Verify that bootstrap actually discovered peers; fail fast if none
        let (ct_tx, ct_rx) = oneshot::channel::<usize>();
        mpsc::Sender::clone(&tx)
            .send(Command::DebugPeerCount { tx_count: ct_tx })
            .await?;
        let peer_count = ct_rx.await.unwrap_or(0);
        if peer_count == 0 {
            anyhow::bail!("bootstrap completed but discovered 0 peers; check seeds or network");
        }

        Ok(Self { tx, _local_addr: local_addr, node_info })
    }

    /// Start a DHT peer (full participant that stores/caches values) bound to a specific address.
    pub async fn start_peer(
        bind_addr: &str,
        bootstrap_addrs: Vec<std::net::SocketAddr>,
        k: usize,
        alpha: usize,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel::<Command>(100);
        let socket = UdpSocket::bind(bind_addr).await?;
        let local_addr = socket.local_addr()?;
        let manager = ProtocolManager::new(socket, rx, k, alpha)?;
        let node_info = manager.node.my_info;

        tokio::spawn(manager.run());

        let (btx, brx) = oneshot::channel::<()>();
        let seeds_len = bootstrap_addrs.len();
        mpsc::Sender::clone(&tx)
            .send(Command::Bootstrap {
                addrs: bootstrap_addrs,
                tx_done: btx,
            })
            .await?;
        let timeout = std::time::Duration::from_secs(3);
        match tokio::time::timeout(timeout, brx).await {
            Ok(Ok(_)) => {}
            Ok(Err(_canceled)) => {
                return Err(anyhow::anyhow!("bootstrap canceled by protocol task"))
            }
            Err(_elapsed) => {
                return Err(anyhow::anyhow!(
                    "bootstrap timed out after {:?} (seeds={})",
                    timeout,
                    seeds_len
                ))
            }
        }

        let (ct_tx, ct_rx) = oneshot::channel::<usize>();
        mpsc::Sender::clone(&tx)
            .send(Command::DebugPeerCount { tx_count: ct_tx })
            .await?;
        let peer_count = ct_rx.await.unwrap_or(0);
        if peer_count == 0 {
            anyhow::bail!("bootstrap completed but discovered 0 peers; check seeds or network");
        }

        Ok(Self { tx, _local_addr: local_addr, node_info })
}

    /// Put a key,value into the DHT. Fire-and-forget best-effort replication.
    pub async fn put(&self, key: Key, value: Value) -> anyhow::Result<()> {
        let (tx_done, rx_done) = oneshot::channel::<()>();
        self.tx
            .send(Command::Put {
                key,
                value,
                tx_done,
            })
            .await?;
        // Wait until the protocol has converged on k closest and scheduled STORE messages.
        // This does not guarantee they have been delivered, only dispatched.
        let _ = rx_done.await;
        Ok(())
    }

    /// Attempt to return the associated value for a given key.
    /// Possibly None if no nodes have the value for this key.
    pub async fn get(&self, key: Key) -> anyhow::Result<Option<Value>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Get { key, tx_value: tx }).await?;
        Ok(rx.await?)
    }

    /// Test/debug helper: query whether this node currently has a value for `key`.
    pub async fn debug_has_value(&self, key: Key) -> anyhow::Result<bool> {
        let (tx, rx) = oneshot::channel::<bool>();
        self.tx
            .send(Command::DebugHasValue { key, tx_has: tx })
            .await?;
        Ok(rx.await?)
    }
}
