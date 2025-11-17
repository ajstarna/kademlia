use super::*;
use crate::core::routing_table::InsertResult;
use crate::test_support::test_support::{id_with_first_byte, make_peer};

#[tokio::test]
async fn test_receive_ping() {
    let dummy_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap(); // ephemeral port
    let mut pm = ProtocolManager::new_headless(dummy_socket, 20, 3).unwrap();

    let src_id = NodeID::new();
    let rpc_id = RpcId::new_random();
    let msg = Message::Ping {
        node_id: src_id,
        rpc_id,
        is_client: false,
    };
    let src: SocketAddr = "127.0.0.1:4000".parse().unwrap();

    let effects = pm.handle_message(msg, src).await.unwrap();

    // make sure the addr that sent us the message is now in our table
    assert!(pm.node.routing_table.find(src_id).is_some());

    let effect = effects
        .into_iter()
        .next()
        .expect("there should be an effect");
    match effect {
        Effect::Send { addr, bytes } => {
            assert_eq!(addr, src);
            let reply: Message = rmp_serde::from_slice(&bytes).unwrap();
            assert!(matches!(
                reply,
                Message::Pong {
                node_id,
                rpc_id: pid,
                ..
                } if node_id == pm.node.my_info.node_id && pid == rpc_id
            ));
        }
        _ => panic!("expected Send Pong effect, got {:?}", effect),
    }
}

#[tokio::test]
async fn test_receive_store_and_find_value() {
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap(); // ephemeral port
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 3).unwrap();

    let src_id: NodeID = NodeID::new();
    let src: SocketAddr = "127.0.0.1:4000".parse().unwrap();

    // let the key be the hash of the value.
    // TODO: figure out if we want to mandate this or allow collisions
    let key: Key = NodeID::from_hashed(&"world");
    let value = b"world".to_vec();
    let store_msg = Message::Store {
        node_id: src_id,
        key: key.clone(),
        value: value.clone(),
        ttl_secs: DEFAULT_STORE_TTL_SECS,
        is_client: false,
    };

    pm.handle_message(store_msg, src).await.unwrap();

    // make sure the addr that sent us the message is now in our table
    assert!(pm.node.routing_table.find(src_id).is_some());

    // Value should be in storage
    let stored = pm.node.get(&key).cloned();
    assert_eq!(stored, Some(value.clone()));

    // Now, if someone requests the value, we can return it
    let find_msg = Message::FindValue {
        node_id: src_id,
        key: key.clone(),
        rpc_id: RpcId::new_random(),
        is_client: false,
    };

    let find_effects = pm.handle_message(find_msg, src).await.unwrap();

    let effect = find_effects
        .into_iter()
        .next()
        .expect("expected one effect");
    match effect {
        Effect::Send { addr, bytes } => {
            assert_eq!(addr, src);

            let reply: Message = rmp_serde::from_slice(&bytes).unwrap();
            match reply {
                Message::ValueFound {
                    node_id,
                    key: k,
                    value: v,
                    ..
                } => {
                    assert_eq!(node_id, pm.node.my_info.node_id);
                    assert_eq!(k, key);
                    assert_eq!(v, value);
                }
                _ => panic!("expected ValueFound, got {:?}", reply),
            }
        }
        _ => panic!("expected Send(ValueFound), got {:?}", effect),
    }
}

#[tokio::test]
async fn test_receive_find_value_found() {
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 3).unwrap();

    // Insert a value into this node's storage directly
    let key: Key = NodeID::from_hashed(&"world");
    let value = b"world".to_vec();
    pm.node.store(key, value.clone());

    // Send FindValue
    let src_id: NodeID = NodeID::new();
    let msg: Message = Message::FindValue {
        node_id: src_id,
        key,
        rpc_id: RpcId::new_random(),
        is_client: false,
    };
    let src: SocketAddr = "127.0.0.1:4000".parse().unwrap();

    let effects: Vec<Effect> = pm.handle_message(msg, src).await.unwrap();

    // The effect should be a Send with a ValueFound reply
    let effect = effects
        .into_iter()
        .next()
        .expect("there should be an effect");
    match effect {
        Effect::Send { addr, bytes } => {
            assert_eq!(addr, src);
            let reply: Message = rmp_serde::from_slice(&bytes).unwrap();
            assert!(matches!(reply, Message::ValueFound { key: k, value: v, .. }
			if k == key && v == value));
        }
        _ => panic!("expected Send ValueFound effect, got {:?}", effect),
    }
}

#[tokio::test]
async fn test_receive_find_value_nodes() {
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 3).unwrap();

    let key: Key = NodeID::from_hashed(&"missing");

    // Send FindValue for a key that isn't in storage
    let src_id: NodeID = NodeID::new();
    let msg: Message = Message::FindValue {
        node_id: src_id,
        key,
        rpc_id: RpcId::new_random(),
        is_client: false,
    };
    let src: SocketAddr = "127.0.0.1:4001".parse().unwrap();

    let effects: Vec<Effect> = pm.handle_message(msg, src).await.unwrap();

    // The effect should be a Send with a Nodes reply
    let effect = effects
        .into_iter()
        .next()
        .expect("there should be an effect");
    match effect {
        Effect::Send { addr, bytes } => {
            assert_eq!(addr, src);
            let reply: Message = rmp_serde::from_slice(&bytes).unwrap();
            // the node that made the request got added to the tree
            assert!(matches!(reply,
		    Message::Nodes { target, .. }
		    if target == key));
        }
        _ => panic!("expected Send Nodes effect, got {:?}", effect),
    }
    // sanity check that the node that made the request got added to the tree
    assert!(pm.node.routing_table.find(src_id).is_some());
}

#[tokio::test]
async fn test_probe_timeout_inserts_candidate_and_coalesces_followups() {
    // Arrange a small K so buckets fill quickly
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 2, 1).unwrap();

    // Determine which first-bit is the non-self bucket
    let self_bit = pm.node.my_info.node_id.get_bit_at(0);
    let (non_byte, self_byte) = if self_bit == 0 {
        (0x80, 0x00)
    } else {
        (0x00, 0x80)
    };

    // Fill root with two distinct peers in the same non-self bucket (same MSB, different lower bits)
    let pna = make_peer(10, 9001, non_byte | 0x00);
    let pnb = make_peer(11, 9002, non_byte | 0x01);
    let _ = pm.node.routing_table.insert(pna);
    let _ = pm.node.routing_table.insert(pnb);
    // This insert should split the root into self/non-self buckets
    let ps = make_peer(12, 9003, self_byte);
    let _ = pm.node.routing_table.insert(ps);

    // Now non-self bucket should be full with pna, pnb (K=2).
    // Try to add two more distinct peers that land in the same non-self bucket.
    let c1 = make_peer(13, 9004, non_byte | 0x02);
    let c2 = make_peer(14, 9005, non_byte | 0x03);

    // First candidate triggers a probe to the LRU of that bucket
    let eff1 = pm
        .observe_contact(SocketAddr::new(c1.ip_address, c1.udp_port), c1.node_id)
        .expect("should start a probe for full bucket");
    // Apply the probe effect so it is recorded as pending
    pm.apply_effect(eff1).await;

    // Second candidate should coalesce into the same pending probe (no new effect)
    let eff2 = pm.observe_contact(SocketAddr::new(c2.ip_address, c2.udp_port), c2.node_id);
    assert!(
        eff2.is_none(),
        "second candidate should coalesce into existing probe"
    );

    // Act: advance time and sweep so the probe times out (evict LRU and insert candidate(s))
    let now = Instant::now() + Duration::from_secs(10);
    let effects = pm.sweep_timeouts_and_topup(now, std::time::Instant::now());

    // Assert: first candidate was inserted into the routing table
    assert!(
        pm.node.routing_table.contains(c1.node_id),
        "first candidate should be inserted after LRU timeout"
    );

    // And we should schedule at most one new probe carrying the remaining candidate(s)
    let mut start_probe_count = 0usize;
    let mut carried: Vec<NodeInfo> = Vec::new();
    for eff in effects.into_iter() {
        if let Effect::StartProbe {
            lru: _, candidates, ..
        } = eff
        {
            start_probe_count += 1;
            carried = candidates;
        }
    }
    assert_eq!(
        start_probe_count, 1,
        "should start exactly one follow-up probe"
    );
    assert_eq!(
        carried.len(),
        1,
        "one remaining candidate should be carried"
    );
    assert_eq!(
        carried[0].node_id, c2.node_id,
        "carried candidate should be c2"
    );
}

#[tokio::test]
async fn test_probe_pong_keeps_lru_and_drops_candidates() {
    // Arrange K=2 and fill non-self bucket
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 2, 1).unwrap();

    let self_bit = pm.node.my_info.node_id.get_bit_at(0);
    let (non_byte, self_byte) = if self_bit == 0 {
        (0x80, 0x00)
    } else {
        (0x00, 0x80)
    };

    // Fill root with two distinct peers in the same non-self bucket (same MSB)
    let pna = make_peer(20, 9101, non_byte | 0x00);
    let pnb = make_peer(21, 9102, non_byte | 0x01);
    let _ = pm.node.routing_table.insert(pna);
    let _ = pm.node.routing_table.insert(pnb);
    let ps = make_peer(22, 9103, self_byte);
    let _ = pm.node.routing_table.insert(ps);

    // Two candidates arrive targeting the full non-self bucket (distinct IDs, same MSB)
    let c1 = make_peer(23, 9104, non_byte | 0x02);
    let c2 = make_peer(24, 9105, non_byte | 0x03);

    // First candidate triggers a probe; capture lru and rpc_id
    let eff1 = pm
        .observe_contact(
            std::net::SocketAddr::new(c1.ip_address, c1.udp_port),
            c1.node_id,
        )
        .expect("should start a probe for full bucket");

    let (lru, rpc_id, bytes) = match eff1 {
        Effect::StartProbe {
            lru,
            candidates,
            rpc_id,
            bytes,
        } => {
            assert_eq!(candidates.len(), 1);
            (lru, rpc_id, bytes)
        }
        _ => panic!("expected StartProbe effect"),
    };

    // Apply effect to record the pending probe
    pm.apply_effect(Effect::StartProbe {
        lru,
        candidates: vec![c1],
        rpc_id,
        bytes,
    })
    .await;

    // Second candidate should coalesce
    let eff2 = pm.observe_contact(
        std::net::SocketAddr::new(c2.ip_address, c2.udp_port),
        c2.node_id,
    );
    assert!(eff2.is_none());

    // Act: simulate a Pong from the probed LRU (alive)
    let pong = Message::Pong {
        node_id: lru.node_id,
        rpc_id,
        is_client: false,
    };
    let lru_addr = std::net::SocketAddr::new(lru.ip_address, lru.udp_port);
    let _ = pm.handle_message(pong, lru_addr).await.unwrap();

    // Assert: LRU kept, candidates dropped (not inserted), and no pending probe remains
    assert!(pm.node.routing_table.contains(lru.node_id));
    assert!(!pm.node.routing_table.contains(c1.node_id));
    assert!(!pm.node.routing_table.contains(c2.node_id));
    assert!(pm.pending_probes.is_empty());
    assert!(pm.pending_probe_by_lru.is_empty());
}

#[tokio::test]
async fn test_get_starts_sends_alpha_queries() {
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 3).unwrap();

    // Prepare peers with known distances to the target
    let target: NodeID = id_with_first_byte(0x00);
    let p1 = make_peer(1, 5001, 0x00); // closest
    let p2 = make_peer(2, 5002, 0x01);
    let p3 = make_peer(3, 5003, 0x02);
    let p4 = make_peer(4, 5004, 0x80); // far

    // Helper to absorb splits
    let _ = pm.node.routing_table.insert(p1);
    let _ = pm.node.routing_table.insert(p2);
    let _ = pm.node.routing_table.insert(p3);
    let _ = pm.node.routing_table.insert(p4);

    // Start the lookup via Command::Get
    let key: Key = NodeID(target.0);
    let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
    let effects = pm
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();

    // Collect sends and decode; should be alpha sends to the three closest peers
    let mut dests = Vec::new();
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if let Ok(reply) = rmp_serde::from_slice::<Message>(&bytes) {
                if matches!(reply, Message::FindValue { .. }) {
                    dests.push(addr);
                }
            }
        }
    }

    assert_eq!(dests.len(), 3, "should send alpha FindValue requests");
    let set: std::collections::HashSet<SocketAddr> = dests.into_iter().collect();
    let expected: std::collections::HashSet<SocketAddr> = vec![
        SocketAddr::new(p1.ip_address, p1.udp_port),
        SocketAddr::new(p2.ip_address, p2.udp_port),
        SocketAddr::new(p3.ip_address, p3.udp_port),
    ]
    .into_iter()
    .collect();
    assert_eq!(set, expected);
}

#[tokio::test]
async fn test_nodes_reply_tops_up_lookup() {
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 3).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let p1 = make_peer(1, 6001, 0x00);
    let p2 = make_peer(2, 6002, 0x01);
    let p3 = make_peer(3, 6003, 0x02);

    let _ = pm.node.routing_table.insert(p1);
    let _ = pm.node.routing_table.insert(p2);
    let _ = pm.node.routing_table.insert(p3);

    // Start the lookup via Command::Get
    let key: Key = NodeID(target.0);
    let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
    let effects_init = pm
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();

    // Nodes reply from p1 introducing a new peer p4
    let p4 = make_peer(4, 6004, 0x03);
    // Extract rpc_id for the initial request sent to p1
    let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
    let mut rpc_to_p1: Option<RpcId> = None;
    for eff in effects_init.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == p1_addr {
                if let Ok(Message::FindValue { rpc_id, .. }) =
                    rmp_serde::from_slice::<Message>(&bytes)
                {
                    rpc_to_p1 = Some(rpc_id);
                    break;
                }
            }
        }
    }
    let rpc_to_p1 = rpc_to_p1.expect("should have sent FindValue to p1");
    let nodes_msg = Message::Nodes {
        node_id: p1.node_id,
        target,
        rpc_id: rpc_to_p1,
        nodes: vec![p4],
        is_client: false,
    };
    let effects = pm.handle_message(nodes_msg, p1_addr).await.unwrap();

    // Expect a new FindValue sent to p4 to maintain alpha
    let mut sent_to_p4 = false;
    for eff in effects {
        if let Effect::Send { addr, bytes } = eff {
            if addr == SocketAddr::new(p4.ip_address, p4.udp_port) {
                if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                    if matches!(msg, Message::FindValue { .. }) {
                        sent_to_p4 = true;
                    }
                }
            }
        }
    }
    assert!(sent_to_p4, "should top up with a request to new candidate");
}

#[tokio::test(start_paused = true)]
async fn test_lookup_timeout_tops_up() {
    // Alpha=2, 3 peers → expect 2 initial requests, then after timeout a top-up to the 3rd peer
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 2).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let p1 = make_peer(1, 6101, 0x00); // closest
    let p2 = make_peer(2, 6102, 0x01); // next
    let p3 = make_peer(3, 6103, 0x80); // far

    // Insert peers, absorbing splits
    let _ = pm.node.routing_table.insert(p1);
    let _ = pm.node.routing_table.insert(p2);
    let _ = pm.node.routing_table.insert(p3);

    // Start lookup via Command::Get: should send 2 initial FindValue requests (alpha=2)
    let key: Key = NodeID(target.0);
    let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
    let effects = pm
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();

    let mut initial_dests = Vec::new();
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                if matches!(msg, Message::FindValue { .. }) {
                    initial_dests.push(addr);
                }
            }
        }
    }
    assert_eq!(initial_dests.len(), 2, "initial sends should match alpha");

    // Advance mocked time beyond request timeout and trigger a sweep/top-up
    tokio::time::advance(Duration::from_secs(3)).await;
    let now = Instant::now();

    let effects = pm.sweep_timeouts_and_topup(now, std::time::Instant::now());

    // Expect a new FindValue to the third peer
    let mut sent_to_p3 = false;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == SocketAddr::new(p3.ip_address, p3.udp_port) {
                if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                    if matches!(msg, Message::FindValue { .. }) {
                        sent_to_p3 = true;
                    }
                }
            }
        }
    }
    assert!(sent_to_p3, "timeout should top up to next closest peer");
}

#[tokio::test]
async fn test_nodes_reply_triggers_top_ups_to_new_candidate() {
    // Arrange: alpha=2, two initial peers near the target
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 2).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let p1 = make_peer(1, 7001, 0x00); // closest
    let p2 = make_peer(2, 7002, 0x01); // next
    let p_new = make_peer(3, 7003, 0x02); // new candidate introduced by p1

    // Helper to absorb possible splits while inserting
    let _ = pm.node.routing_table.insert(p1);
    let _ = pm.node.routing_table.insert(p2);

    // Start the lookup for this target via Command::Get
    let key: Key = NodeID(target.0);
    let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
    let effects_init = pm
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();

    // Extract rpc_id for the initial request to p1
    let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
    let mut rpc_to_p1: Option<RpcId> = None;
    for eff in effects_init.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == p1_addr {
                if let Ok(Message::FindValue { rpc_id, .. }) =
                    rmp_serde::from_slice::<Message>(&bytes)
                {
                    rpc_to_p1 = Some(rpc_id);
                    break;
                }
            }
        }
    }
    let rpc_to_p1 = rpc_to_p1.expect("should have FindValue to p1");
    // Act: simulate a Nodes reply from p1 that introduces p_new
    let nodes_msg = Message::Nodes {
        node_id: p1.node_id,
        target,
        rpc_id: rpc_to_p1,
        nodes: vec![p_new],
        is_client: false,
    };
    let effects = pm.handle_message(nodes_msg, p1_addr).await.unwrap();

    // Assert: expect a FindValue sent to p_new to top up concurrency
    let mut sent_to_new = false;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == SocketAddr::new(p_new.ip_address, p_new.udp_port) {
                if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                    if matches!(msg, Message::FindValue { .. }) {
                        sent_to_new = true;
                    }
                }
            }
        }
    }

    assert!(
        sent_to_new,
        "Nodes reply should trigger a top-up FindValue to the newly introduced candidate"
    );
}

#[tokio::test]
async fn test_value_lookup_multiple_hops_via_nodes() {
    // Make a lookup that requires multiple jumps: p1 -> p2 -> p3, where p3 returns the value
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // alpha=1 to force sequential hops
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 1).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let key: Key = NodeID(target.0);

    // Peers increasingly closer to the target
    let p1 = make_peer(1, 8001, 0x40); // far
    let p2 = make_peer(2, 8002, 0x10); // closer
    let p3 = make_peer(3, 8003, 0x00); // closest

    // Insert only p1 initially so the lookup starts there
    let mut insert = |peer: NodeInfo| loop {
        match pm.node.routing_table.try_insert(peer) {
            InsertResult::SplitOccurred => continue,
            _ => break,
        }
    };
    insert(p1);

    // Start the lookup (Value) via Command::Get
    let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
    let effects = pm
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();

    // Expect a FindValue sent to p1, capture its rpc_id
    let mut sent_to_p1 = false;
    let mut rpc_to_p1: Option<RpcId> = None;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == SocketAddr::new(p1.ip_address, p1.udp_port) {
                if let Ok(Message::FindValue { rpc_id, .. }) =
                    rmp_serde::from_slice::<Message>(&bytes)
                {
                    sent_to_p1 = true;
                    rpc_to_p1 = Some(rpc_id);
                }
            }
        }
    }
    assert!(sent_to_p1, "lookup should start by querying p1");
    let rpc_to_p1 = rpc_to_p1.expect("FindValue to p1 should carry rpc_id");

    // Simulate p1 responding with a Nodes message introducing p2
    let nodes_from_p1 = Message::Nodes {
        node_id: p1.node_id,
        target,
        rpc_id: rpc_to_p1,
        nodes: vec![p2],
        is_client: false,
    };
    let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
    let effects = pm.handle_message(nodes_from_p1, p1_addr).await.unwrap();

    // Expect a top-up FindValue to p2 (alpha=1) and capture its rpc_id
    let mut sent_to_p2 = false;
    let mut rpc_to_p2: Option<RpcId> = None;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == SocketAddr::new(p2.ip_address, p2.udp_port) {
                if let Ok(Message::FindValue { rpc_id, .. }) =
                    rmp_serde::from_slice::<Message>(&bytes)
                {
                    sent_to_p2 = true;
                    rpc_to_p2 = Some(rpc_id);
                }
            }
        }
    }
    assert!(sent_to_p2, "Nodes from p1 should trigger query to p2");
    let rpc_to_p2 = rpc_to_p2.expect("FindValue to p2 should carry rpc_id");

    // Simulate p2 responding with a Nodes message introducing p3
    let nodes_from_p2 = Message::Nodes {
        node_id: p2.node_id,
        target,
        rpc_id: rpc_to_p2,
        nodes: vec![p3],
        is_client: false,
    };
    let p2_addr = SocketAddr::new(p2.ip_address, p2.udp_port);
    let effects = pm.handle_message(nodes_from_p2, p2_addr).await.unwrap();

    // Expect a top-up FindValue to p3, capture its rpc_id
    let mut sent_to_p3 = false;
    let mut rpc_to_p3: Option<RpcId> = None;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == SocketAddr::new(p3.ip_address, p3.udp_port) {
                if let Ok(Message::FindValue { rpc_id, .. }) =
                    rmp_serde::from_slice::<Message>(&bytes)
                {
                    sent_to_p3 = true;
                    rpc_to_p3 = Some(rpc_id);
                }
            }
        }
    }
    assert!(sent_to_p3, "Nodes from p2 should trigger query to p3");

    // Finally, simulate p3 returning the value
    let value = b"hello-value".to_vec();
    let found = Message::ValueFound {
        node_id: p3.node_id,
        key,
        rpc_id: rpc_to_p3.expect("FindValue to p3 should carry rpc_id"),
        value: value.clone(),
        is_client: false,
    };
    let p3_addr = SocketAddr::new(p3.ip_address, p3.udp_port);
    let _ = pm.handle_message(found, p3_addr).await.unwrap();

    // Lookup should be cleared and value cached locally
    assert!(pm.pending_lookups.get(&key).is_none());
    assert_eq!(pm.node.get(&key), Some(&value));
}

#[tokio::test]
async fn test_lookup_ends_when_shortlist_unchanged() {
    // Arrange: alpha=2, two initial peers that are the closest to target
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 2).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let p1 = make_peer(1, 9001, 0x00); // closest
    let p2 = make_peer(2, 9002, 0x01); // next closest

    // Insert initial candidates into the routing table (absorb splits if they occur)
    let _ = pm.node.routing_table.insert(p1);
    let _ = pm.node.routing_table.insert(p2);

    // Start a Node lookup towards `target` via Command::Put (performs node lookup under the hood)
    let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel::<()>();
    let effects = pm
        .handle_command(Command::Put {
            key: target,
            value: b"x".to_vec(),
            tx_done: ack_tx,
        })
        .await
        .unwrap();

    // Expect exactly alpha initial FindNode queries and capture rpc_ids
    let mut initial_sends: Vec<SocketAddr> = Vec::new();
    let mut rpc_p1: Option<RpcId> = None;
    let mut rpc_p2: Option<RpcId> = None;
    for eff in &effects {
        if let Effect::Send { addr, bytes } = eff {
            if let Ok(Message::FindNode { rpc_id, .. }) = rmp_serde::from_slice::<Message>(&bytes) {
                if *addr == SocketAddr::new(p1.ip_address, p1.udp_port) {
                    rpc_p1 = Some(rpc_id);
                }
                if *addr == SocketAddr::new(p2.ip_address, p2.udp_port) {
                    rpc_p2 = Some(rpc_id);
                }
                initial_sends.push(*addr);
            }
        }
    }
    assert_eq!(
        initial_sends.len(),
        2,
        "should send alpha FindNode requests"
    );
    let rpc_p1 = rpc_p1.expect("FindNode to p1 should have rpc_id");
    let rpc_p2 = rpc_p2.expect("FindNode to p2 should have rpc_id");

    // Simulate a Nodes reply from p1 that does not improve the shortlist (duplicates only)
    let nodes_from_p1 = Message::Nodes {
        node_id: p1.node_id,
        target,
        rpc_id: rpc_p1,
        nodes: vec![p1, p2],
        is_client: false,
    };
    let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
    let effects = pm.handle_message(nodes_from_p1, p1_addr).await.unwrap();

    // Since one in-flight slot freed but there are no new candidates to try, expect no new FindNode sends
    let topups_after_p1: Vec<SocketAddr> = effects
        .into_iter()
        .filter_map(|eff| match eff {
            Effect::Send { addr, bytes } => {
                if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                    if matches!(msg, Message::FindNode { .. }) {
                        return Some(addr);
                    }
                }
                None
            }
            _ => None,
        })
        .collect();
    assert!(
        topups_after_p1.is_empty(),
        "no new queries should be issued"
    );

    // Simulate a Nodes reply from p2 that also does not improve the shortlist
    let nodes_from_p2 = Message::Nodes {
        node_id: p2.node_id,
        target,
        rpc_id: rpc_p2,
        nodes: vec![p1, p2],
        is_client: false,
    };
    let p2_addr = SocketAddr::new(p2.ip_address, p2.udp_port);
    let effects = pm.handle_message(nodes_from_p2, p2_addr).await.unwrap();

    // With both queries answered and no shortlist changes, the lookup should end with no further sends
    let topups_after_p2: Vec<SocketAddr> = effects
        .into_iter()
        .filter_map(|eff| match eff {
            Effect::Send { addr, bytes } => {
                if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                    if matches!(msg, Message::FindNode { .. }) {
                        return Some(addr);
                    }
                }
                None
            }
            _ => None,
        })
        .collect();
    assert!(
        topups_after_p2.is_empty(),
        "no further queries should be issued"
    );

    // Optional: sweep to mimic periodic maintenance; still no effects expected
    let effects = pm.sweep_timeouts_and_topup(Instant::now(), std::time::Instant::now());
    let find_node_sends = effects
        .into_iter()
        .filter(|eff| match eff {
            Effect::Send { bytes, .. } => {
                matches!(
                    rmp_serde::from_slice::<Message>(bytes),
                    Ok(Message::FindNode { .. })
                )
            }
            _ => false,
        })
        .count();
    assert_eq!(find_node_sends, 0, "no maintenance top-ups should occur");

    // And the lookup should be considered finished (removed)
    assert!(
        pm.pending_lookups.get(&target).is_none(),
        "lookup should be removed once all queries have returned and shortlist is unchanged"
    );
}

#[tokio::test]
async fn test_put_sends_store_to_final_shortlist() {
    // Arrange: alpha=2, two peers near the key. Put should Node-lookup, then STORE to both.
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 2).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let p1 = make_peer(1, 9101, 0x00); // closest
    let p2 = make_peer(2, 9102, 0x01); // next closest

    let _ = pm.node.routing_table.insert(p1);
    let _ = pm.node.routing_table.insert(p2);

    // Act: issue a Put command
    let key: Key = target;
    let value: Value = b"hello-put".to_vec();
    let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel::<()>();
    let effects = pm
        .handle_command(Command::Put {
            key,
            value: value.clone(),
            tx_done: ack_tx,
        })
        .await
        .unwrap();

    // Expect initial FindNode requests to p1 and p2; capture rpc_ids for both
    let mut findnode_dests = Vec::new();
    let mut rpc_p1: Option<RpcId> = None;
    let mut rpc_p2: Option<RpcId> = None;
    for eff in &effects {
        if let Effect::Send { addr, bytes } = eff {
            if let Ok(Message::FindNode { rpc_id, .. }) = rmp_serde::from_slice::<Message>(&bytes) {
                if *addr == std::net::SocketAddr::new(p1.ip_address, p1.udp_port) {
                    rpc_p1 = Some(rpc_id);
                } else if *addr == std::net::SocketAddr::new(p2.ip_address, p2.udp_port) {
                    rpc_p2 = Some(rpc_id);
                }
                findnode_dests.push(*addr);
            }
        }
    }
    let expected: std::collections::HashSet<_> = vec![
        std::net::SocketAddr::new(p1.ip_address, p1.udp_port),
        std::net::SocketAddr::new(p2.ip_address, p2.udp_port),
    ]
    .into_iter()
    .collect();
    let got: std::collections::HashSet<_> = findnode_dests.into_iter().collect();
    assert_eq!(got, expected, "initial FindNode should target both peers");

    // Simulate Nodes replies from both peers that do not improve the shortlist
    let nodes_from_p1 = Message::Nodes {
        node_id: p1.node_id,
        target,
        rpc_id: rpc_p1.expect("FindNode to p1 should carry rpc_id"),
        nodes: vec![p1, p2],
        is_client: false,
    };
    let effects1 = pm
        .handle_message(
            nodes_from_p1,
            std::net::SocketAddr::new(p1.ip_address, p1.udp_port),
        )
        .await
        .unwrap();

    // Second reply should finish the lookup and emit STORE messages
    let nodes_from_p2 = Message::Nodes {
        node_id: p2.node_id,
        target,
        rpc_id: rpc_p2.expect("FindNode to p2 should carry rpc_id"),
        nodes: vec![p1, p2],
        is_client: false,
    };
    let effects2 = pm
        .handle_message(
            nodes_from_p2,
            std::net::SocketAddr::new(p2.ip_address, p2.udp_port),
        )
        .await
        .unwrap();

    // Collect Store sends
    let mut store_dests = Vec::new();
    let mut store_payloads = Vec::new();
    for eff in effects1.into_iter().chain(effects2.into_iter()) {
        if let Effect::Send { addr, bytes } = eff {
            if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                if let Message::Store {
                    key: k, value: v, ..
                } = msg
                {
                    assert_eq!(k, key);
                    assert_eq!(v, value);
                    store_dests.push(addr);
                    store_payloads.push((k, v));
                }
            }
        }
    }
    let expected_store: std::collections::HashSet<_> = vec![
        std::net::SocketAddr::new(p1.ip_address, p1.udp_port),
        std::net::SocketAddr::new(p2.ip_address, p2.udp_port),
    ]
    .into_iter()
    .collect();
    let got_store: std::collections::HashSet<_> = store_dests.into_iter().collect();
    assert!(
        pm.pending_lookups.get(&target).is_none(),
        "lookup should be removed on completion"
    );
    assert_eq!(
        got_store, expected_store,
        "should STORE to both final peers"
    );

    // Put is fire-and-forget; no ack to await
}

#[tokio::test(start_paused = true)]
async fn test_late_nodes_reply_is_accepted() {
    // Arrange alpha=1 so a single in-flight; two initial peers p1, p2
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 1).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let key: Key = NodeID(target.0);
    let p1 = make_peer(1, 9601, 0x10);
    let p2 = make_peer(2, 9602, 0x08);
    let p3 = make_peer(3, 9603, 0x00); // to be introduced by p1
    let _ = pm.node.routing_table.insert(p1);
    let _ = pm.node.routing_table.insert(p2);

    // Start a Get and capture rpc_id sent to the first queried peer (could be p1 or p2)
    let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
    let effects = pm
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();
    let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
    let p2_addr = SocketAddr::new(p2.ip_address, p2.udp_port);
    let mut responder_addr: Option<SocketAddr> = None;
    let mut responder_node: Option<NodeID> = None;
    let mut responder_rpc: Option<RpcId> = None;
    for eff in effects {
        if let Effect::Send { addr, bytes } = eff {
            if let Ok(Message::FindValue { rpc_id, .. }) = rmp_serde::from_slice::<Message>(&bytes)
            {
                if addr == p1_addr {
                    responder_addr = Some(p1_addr);
                    responder_node = Some(p1.node_id);
                    responder_rpc = Some(rpc_id);
                } else if addr == p2_addr {
                    responder_addr = Some(p2_addr);
                    responder_node = Some(p2.node_id);
                    responder_rpc = Some(rpc_id);
                }
            }
        }
    }
    let responder_addr = responder_addr.expect("initial FindValue should target a known peer");
    let responder_node = responder_node.unwrap();
    let responder_rpc = responder_rpc.expect("initial FindValue should carry rpc_id");

    // Timeout p1 and top up to p2
    tokio::time::advance(Duration::from_secs(3)).await;
    let now = Instant::now();
    let _ = pm.sweep_timeouts_and_topup(now, std::time::Instant::now());
    // Assert responder is no longer in_flight for this lookup
    let pl = pm
        .pending_lookups
        .get(&key)
        .expect("lookup should still be active before late reply");
    assert!(
        !pl.lookup.in_flight.contains_key(&responder_node),
        "timed-out responder should be removed from in_flight"
    );

    // Late Nodes from p1 should be accepted (rpc_id matches) and p3 observed
    let nodes_from_p1 = Message::Nodes {
        node_id: responder_node,
        target,
        rpc_id: responder_rpc,
        nodes: vec![p3],
        is_client: false,
    };
    let effects = pm
        .handle_message(nodes_from_p1, responder_addr)
        .await
        .expect("handle nodes");

    assert!(
        pm.node.routing_table.contains(p3.node_id),
        "late Nodes should be accepted while lookup active"
    );

    // Since alpha=1 and another peer should be in-flight, top-up should not emit new FindValue
    let mut saw_findvalue = false;
    for eff in effects.into_iter() {
        if let Effect::Send { bytes, .. } = eff {
            if let Ok(Message::FindValue { .. }) = rmp_serde::from_slice::<Message>(&bytes) {
                saw_findvalue = true;
            }
        }
    }
    assert!(
        !saw_findvalue,
        "no new FindValue should be sent when alpha is full"
    );
}

#[tokio::test(start_paused = true)]
async fn test_late_valuefound_is_accepted() {
    // Arrange alpha=1; two peers, p1 will time out but later return the value
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 1).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let key: Key = NodeID(target.0);
    let p1 = make_peer(1, 9701, 0x10);
    let p2 = make_peer(2, 9702, 0x08);
    let _ = pm.node.routing_table.insert(p1);
    let _ = pm.node.routing_table.insert(p2);

    // Start a Get and capture rpc_id to first queried peer (could be p1 or p2)
    let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
    let effects = pm
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();
    let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
    let p2_addr = SocketAddr::new(p2.ip_address, p2.udp_port);
    let mut responder_addr: Option<SocketAddr> = None;
    let mut responder_node: Option<NodeID> = None;
    let mut responder_rpc: Option<RpcId> = None;
    for eff in effects {
        if let Effect::Send { addr, bytes } = eff {
            if let Ok(Message::FindValue { rpc_id, .. }) = rmp_serde::from_slice::<Message>(&bytes)
            {
                if addr == p1_addr {
                    responder_addr = Some(p1_addr);
                    responder_node = Some(p1.node_id);
                    responder_rpc = Some(rpc_id);
                } else if addr == p2_addr {
                    responder_addr = Some(p2_addr);
                    responder_node = Some(p2.node_id);
                    responder_rpc = Some(rpc_id);
                }
            }
        }
    }
    let responder_addr = responder_addr.expect("initial FindValue should target a known peer");
    let responder_node = responder_node.unwrap();
    let responder_rpc = responder_rpc.expect("initial FindValue should carry rpc_id");

    // Timeout p1 and top up
    tokio::time::advance(Duration::from_secs(3)).await;
    let now = Instant::now();
    let _ = pm.sweep_timeouts_and_topup(now, std::time::Instant::now());
    // Assert responder is no longer in_flight for this lookup
    let pl = pm
        .pending_lookups
        .get(&key)
        .expect("lookup should still be active before late reply");
    assert!(
        !pl.lookup.in_flight.contains_key(&responder_node),
        "timed-out responder should be removed from in_flight"
    );

    // Late ValueFound from the original responder with matching rpc_id should complete lookup
    let value = b"late-hello".to_vec();
    let found = Message::ValueFound {
        node_id: responder_node,
        key,
        rpc_id: responder_rpc,
        value: value.clone(),
        is_client: false,
    };
    let _ = pm.handle_message(found, responder_addr).await.unwrap();

    assert!(
        pm.pending_lookups.get(&key).is_none(),
        "lookup should finish"
    );
    assert_eq!(pm.node.get(&key), Some(&value), "value cached locally");
}

#[tokio::test]
async fn test_bucket_refresh_triggers_lookup() {
    // Arrange: headless PM with some peers in the routing table so refresh has initial candidates
    let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 3, 1).unwrap();

    // Insert a few peers; absorb splits as needed
    let p1 = make_peer(1, 9301, 0x00);
    let p2 = make_peer(2, 9302, 0x40);
    let p3 = make_peer(3, 9303, 0x80);
    let mut insert = |peer: NodeInfo| loop {
        match pm.node.routing_table.try_insert(peer) {
            InsertResult::SplitOccurred => continue,
            _ => break,
        }
    };
    insert(p1);
    insert(p2);
    insert(p3);

    // Act: sweep with std-time far in the future so buckets appear stale
    let effects = pm.sweep_timeouts_and_topup(
        Instant::now(),
        std::time::Instant::now() + std::time::Duration::from_secs(2 * 60 * 60),
    );

    // Assert: at least one FindNode send is scheduled and a pending lookup is registered
    let mut saw_findnode = false;
    for eff in effects {
        if let Effect::Send { bytes, .. } = eff {
            if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                if matches!(msg, Message::FindNode { .. }) {
                    saw_findnode = true;
                }
            }
        }
    }
    assert!(saw_findnode, "refresh sweep should start a FindNode lookup");
    assert!(
        !pm.pending_lookups.is_empty(),
        "refresh should register a pending lookup"
    );
}

#[tokio::test]
async fn test_remote_cache_to_closest_non_holder() {
    let req_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm_req: ProtocolManager = ProtocolManager::new_headless(req_socket, 20, 1).unwrap();

    // Choose a key and value
    let key: Key = NodeID::from_hashed(&"cache-key");
    let value: Value = b"cached-value".to_vec();

    // Create a non-holder peer that will respond with Nodes, and a provider peer that returns the value
    // Make the non-holder appear close to the key by matching the first byte
    let first_byte = key.0.to_fixed_bytes()[0];
    let p_nonholder = make_peer(41, 9501, first_byte);
    let p_provider = make_peer(42, 9502, first_byte ^ 0x01);

    // Insert the non-holder into the routing table
    let _ = pm_req.node.routing_table.insert(p_nonholder);

    // Act 1: start a Get lookup
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let effects = pm_req
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();

    // Expect an initial FindValue to the non-holder
    let mut sent_to_nonholder = false;
    let mut rpc_to_nonholder: Option<RpcId> = None;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == std::net::SocketAddr::new(p_nonholder.ip_address, p_nonholder.udp_port) {
                if let Ok(Message::FindValue { rpc_id, .. }) =
                    rmp_serde::from_slice::<Message>(&bytes)
                {
                    sent_to_nonholder = true;
                    rpc_to_nonholder = Some(rpc_id);
                }
            }
        }
    }
    assert!(
        sent_to_nonholder,
        "lookup should start by querying the non-holder"
    );

    // Act 2: non-holder responds with Nodes introducing the provider; this also records non-holder
    let nodes_from_nonholder = Message::Nodes {
        node_id: p_nonholder.node_id,
        target: key,
        rpc_id: rpc_to_nonholder.expect("FindValue to non-holder should carry rpc_id"),
        nodes: vec![p_provider],
        is_client: false,
    };
    let nh_addr = std::net::SocketAddr::new(p_nonholder.ip_address, p_nonholder.udp_port);
    let effects = pm_req
        .handle_message(nodes_from_nonholder, nh_addr)
        .await
        .unwrap();

    // Expect a FindValue to the provider now
    let mut sent_to_provider = false;
    let mut rpc_to_provider: Option<RpcId> = None;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == std::net::SocketAddr::new(p_provider.ip_address, p_provider.udp_port) {
                if let Ok(Message::FindValue { rpc_id, .. }) =
                    rmp_serde::from_slice::<Message>(&bytes)
                {
                    sent_to_provider = true;
                    rpc_to_provider = Some(rpc_id);
                }
            }
        }
    }
    assert!(
        sent_to_provider,
        "Nodes reply should trigger query to provider"
    );

    // Act 3: provider replies with ValueFound
    let found = Message::ValueFound {
        node_id: p_provider.node_id,
        key,
        rpc_id: rpc_to_provider.expect("FindValue to provider should carry rpc_id"),
        value: value.clone(),
        is_client: false,
    };
    let prov_addr = std::net::SocketAddr::new(p_provider.ip_address, p_provider.udp_port);
    let effects = pm_req.handle_message(found, prov_addr).await.unwrap();

    // Expect a Store to be sent to the closest non-holder (p_nonholder)
    let mut store_bytes: Option<Vec<u8>> = None;
    let mut store_dest: Option<std::net::SocketAddr> = None;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if let Ok(Message::Store {
                key: k, value: v, ..
            }) = rmp_serde::from_slice::<Message>(&bytes)
            {
                assert_eq!(k, key);
                assert_eq!(v, value);
                store_bytes = Some(bytes);
                store_dest = Some(addr);
                break;
            }
        }
    }
    let store_dest = store_dest.expect("should have sent a Store to cache node");
    assert_eq!(
        store_dest,
        std::net::SocketAddr::new(p_nonholder.ip_address, p_nonholder.udp_port),
        "Store should target the closest non-holder"
    );

    // Apply the Store to a separate ProtocolManager representing the non-holder and verify it stored the value
    let cache_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm_cache = ProtocolManager::new_headless(cache_socket, 20, 1).unwrap();
    let store_msg: Message = rmp_serde::from_slice(&store_bytes.unwrap()).unwrap();
    let src = "127.0.0.1:9999".parse().unwrap();
    let _ = pm_cache.handle_message(store_msg, src).await.unwrap();

    assert_eq!(pm_cache.node.get(&key), Some(&value));
}

#[tokio::test]
async fn test_pong_with_unknown_rpc_is_ignored() {
    let dummy_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm = ProtocolManager::new_headless(dummy_socket, 20, 1).unwrap();

    let bogus = NodeID::new();
    let rpc = RpcId::new_random();
    let msg = Message::Pong {
        node_id: bogus,
        rpc_id: rpc,
        is_client: false,
    };
    let src: SocketAddr = "127.0.0.1:4010".parse().unwrap();
    let effects = pm.handle_message(msg, src).await.unwrap();

    // No effects and the bogus peer was not observed/added
    assert!(effects.is_empty());
    assert!(pm.node.routing_table.find(bogus).is_none());
}

#[tokio::test]
async fn test_nodes_with_mismatched_rpc_is_dropped() {
    // Arrange a simple lookup with one initial peer
    let dummy_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut pm = ProtocolManager::new_headless(dummy_socket, 20, 1).unwrap();

    let target: NodeID = id_with_first_byte(0x00);
    let key: Key = NodeID(target.0);
    let p1 = make_peer(1, 9501, 0x00);
    let p_new = make_peer(2, 9502, 0x10);
    let _ = pm.node.routing_table.insert(p1);

    // Start lookup and capture initial FindValue rpc_id to p1
    let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
    let effects = pm
        .handle_command(Command::Get { key, tx_value: tx })
        .await
        .unwrap();
    let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
    let mut rpc_to_p1: Option<RpcId> = None;
    for eff in effects.into_iter() {
        if let Effect::Send { addr, bytes } = eff {
            if addr == p1_addr {
                if let Ok(Message::FindValue { rpc_id, .. }) =
                    rmp_serde::from_slice::<Message>(&bytes)
                {
                    rpc_to_p1 = Some(rpc_id);
                }
            }
        }
    }
    assert!(rpc_to_p1.is_some(), "should send FindValue to p1");

    // Send a Nodes reply with a mismatched rpc_id; it should be dropped fully
    let nodes_msg = Message::Nodes {
        node_id: p1.node_id,
        target,
        rpc_id: RpcId::new_random(), // mismatch
        nodes: vec![p_new],
        is_client: false,
    };
    let effects = pm.handle_message(nodes_msg, p1_addr).await.unwrap();

    // No follow-up FindValue, and p_new was not observed/added
    let mut saw_followup = false;
    for eff in effects.into_iter() {
        if let Effect::Send { bytes, .. } = eff {
            if let Ok(Message::FindValue { .. }) = rmp_serde::from_slice::<Message>(&bytes) {
                saw_followup = true;
            }
        }
    }
    assert!(!saw_followup, "should not top-up on mismatched Nodes");
    assert!(pm.node.routing_table.find(p_new.node_id).is_none());
}
