use kademlia::dht::KademliaDHT;
use kademlia::protocol::{Command, ProtocolManager};
use kademlia::{Key, NodeID, Value};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

#[tokio::test()]
async fn end_to_end_put_get() -> anyhow::Result<()> {
    // Spin up two seed ProtocolManagers on ephemeral local ports
    let k = 20;
    let alpha = 3;

    // Seed 1
    let s1_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let s1_addr = s1_socket.local_addr()?;
    let mut s1_pm = ProtocolManager::new_headless(s1_socket, k, alpha)?;
    let s1_info = s1_pm.node.my_info; // capture before moving into task

    // Seed 2
    let s2_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let s2_addr = s2_socket.local_addr()?;
    let mut s2_pm = ProtocolManager::new_headless(s2_socket, k, alpha)?;
    let s2_info = s2_pm.node.my_info;

    // Pre-seed each seed's routing table with the other seed so bootstrap can discover peers.
    eprintln!("About to add known peer to s1");
    s1_pm.add_known_peer(s2_info).await;
    eprintln!("About to add known peer to s2");
    s2_pm.add_known_peer(s1_info).await;

    eprintln!("About to spawn");
    // Now run both protocol managers
    tokio::spawn(s1_pm.run());
    tokio::spawn(s2_pm.run());

    // Give the managers a moment to process the seeds
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Start a DHT node bound to an ephemeral local port, bootstrapping explicitly against the two seeds
    let bootstrap_addrs: Vec<SocketAddr> = vec![s1_addr, s2_addr];
    let dht = KademliaDHT::start_client("127.0.0.1:0", bootstrap_addrs).await?;

    // Allow bootstrap FindNode/Nodes to exchange and populate the DHT's routing table
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Choose a deterministic key and value
    let key: Key = NodeID::from_hashed(&"integration-key");
    let value: Value = b"integration-value".to_vec();

    eprintln!("About to put in the dht");
    // Put should complete (fire-and-forget, no ack)
    dht.put(key, value.clone()).await?;

    // Give time for STORE messages to reach seeds
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Get should find the value via the network
    eprintln!("About to get from the dht");
    let got = dht.get(NodeID::from_hashed(&"integration-key")).await?;
    assert_eq!(got, Some(value), "get returns the stored value");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_to_k_nodes() -> anyhow::Result<()> {
    // Parameters for this test
    let k = 4;
    let alpha = 3;
    let num_nodes = 50;
    let num_seeds = 3;

    // Helper to absorb splits on pre-insert when needed (using add_known_peer is async)
    // We'll just rely on bootstrap behavior and seed knowledge.

    // Spawn seed nodes first
    let mut seed_addrs: Vec<SocketAddr> = Vec::new();
    let mut all_senders: Vec<tokio::sync::mpsc::Sender<Command>> = Vec::new();
    let mut all_infos: Vec<kademlia::NodeInfo> = Vec::new();

    for _ in 0..num_seeds {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let addr = socket.local_addr()?;
        let (tx, rx) = tokio::sync::mpsc::channel::<Command>(100);
        let pm = ProtocolManager::new(socket, rx, k, alpha)?;
        let info = pm.node.my_info;
        tokio::spawn(pm.run());
        seed_addrs.push(addr);
        all_senders.push(tx);
        all_infos.push(info);
    }

    // Spawn the remaining nodes and bootstrap them to the seeds
    for _ in num_seeds..num_nodes {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let (tx, rx) = tokio::sync::mpsc::channel::<Command>(100);
        let pm = ProtocolManager::new(socket, rx, k, alpha)?;
        let info = pm.node.my_info;
        let tx_clone = tx.clone();
        tokio::spawn(pm.run());
        // Bootstrap to seeds
        tx_clone
            .send(Command::Bootstrap {
                addrs: seed_addrs.clone(),
            })
            .await?;
        all_senders.push(tx);
        all_infos.push(info);
    }

    // Give the network some time to exchange bootstrap messages
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Pick a key and value, and create a DHT client node to perform the put
    let key: Key = NodeID::from_hashed(&"replication-key");
    let value: Value = b"replication-value".to_vec();

    // Use a real DHT handle for the client behavior
    let client_dht = KademliaDHT::start_client("127.0.0.1:0", seed_addrs.clone()).await?;
    let client_info = client_dht.node_info;
    let _ = client_dht.put(key, value.clone()).await?;

    // Compute the expected k closest nodes by XOR distance to key
    let mut infos_sorted = all_infos.clone();
    infos_sorted.push(client_info);
    infos_sorted.sort_by_key(|n| n.node_id.distance(&key));
    let expected_topk: Vec<NodeID> = infos_sorted.iter().take(k).map(|n| n.node_id).collect();

    // Poll until replication converges or timeout
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        // query all nodes for presence via debug command
        let mut has_set: std::collections::HashSet<NodeID> = std::collections::HashSet::new();
        for (tx, info) in all_senders.iter().cloned().zip(all_infos.iter().cloned()) {
            let (qtx, qrx) = tokio::sync::oneshot::channel::<bool>();
            // Ignore errors if a node happened to shut down (shouldn't in this test)
            let _ = tx.send(Command::DebugHasValue { key, rx: qtx }).await;
            if let Ok(true) = qrx.await {
                has_set.insert(info.node_id);
            }
        }

        let mut expected_set: std::collections::HashSet<NodeID> =
            expected_topk.iter().copied().collect();

        // Include client node's value presence
        if client_dht.debug_has_value(key).await? {
            has_set.insert(client_info.node_id);
        }

        if has_set == expected_set {
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "replication did not converge to top-k within timeout. got {} nodes, expected {}",
                has_set.len(),
                expected_topk.len()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}
