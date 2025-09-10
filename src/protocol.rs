use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::{
    node::identifier::{Key, NodeID, NodeInfo, ProbeID},
    node::routing_table::{InsertResult},
    node::storage::Value,
    node::Node,
};
use std::collections::{HashMap, HashSet};
use std::time::Instant;


const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Serialize, Deserialize, Debug)]
pub enum LookupKind {
    Node, // FIND_NODE
    Value, // FIND_VALUE
}

// each message type includes the NodeID of the sender
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Message {
    Ping {
        node_id: NodeID,
	probe_id: ProbeID, // a unique id for this specific request
    },
    Pong {
        node_id: NodeID,
	probe_id: ProbeID, // a unique id for this specific request
    },
    Store {
        node_id: NodeID,
        key: Key,
        value: Value,
    },
    FindNode {
        node_id: NodeID,
        target: NodeID,
    },
    Nodes {
        node_id: NodeID,
        target: NodeID, // we need to include the target to map the nodes to the lookup
        nodes: Vec<NodeInfo>,
    },
    FindValue {
        node_id: NodeID,
        key: Key,
    },
    ValueFound {
        node_id: NodeID,
        key: Key,
        value: Value,
    },
    // Note: this is not a real message in the protocol.
    // For development purposes, we have this message to test starting and driving a lookup
    // Once it is working, we are going to have to handle user commands. Possibly from a channel.
    StartLookup {
        node_id: NodeID,
        key: Key,
	kind: LookupKind,
    },

}


/// Effect represents the "side effect" that `handle_message` wants the outer
/// event loop to perform.
///
/// It decouples pure routing-table logic (insertions, probes, splits, etc.)
/// from I/O side effects (sending a Pong, starting a probe, etc.).
#[derive(Debug)]
enum Effect {
    Send {
        addr: SocketAddr,
        bytes: Vec<u8>,
    },
    StartProbe {
        peer: NodeInfo,
        probe_id: ProbeID,
        bytes: Vec<u8>,
    },
}

#[derive(Copy, Debug, Clone)]
struct PendingProbe {
    peer: NodeInfo,
    deadline: Instant,
}

struct Lookup {
    alpha: usize,
    my_node_id: NodeID,
    // keep the target as a NodeID, even though sometimes it is a key
    // TODO: do we need an enum or anything for the two types of lookups?
    target: NodeID,
    kind: LookupKind,
    short_list: Vec<NodeInfo>,
    already_queried: HashSet<NodeID>, // indicate who we have sent a request to already
    in_flight: HashMap<NodeID, Instant>, // active queries with deadlines
}

impl Lookup {
    pub fn new(alpha: usize, my_node_id: NodeID, target: NodeID, kind: LookupKind, initial_candidates: Vec<NodeInfo>) -> Self {
        Lookup {
	    alpha,
	    my_node_id,
            target,
	    kind,
            short_list: initial_candidates,
            already_queried: HashSet::new(),
            in_flight: HashMap::new(),
        }
    }


    /// main method to be called on a lookup struct
    /// If there are fewer than `alpha` queries in flight, we return Effects to top up the difference.
    /// This can be called when we first start a lookup, or when we drive it forward after receiving a Nodes message.
    pub fn top_up_alpha_requests(&mut self) -> Vec<Effect> {

	let mut effects = Vec::new();

	// find up to alpha candidate nodes that we haven't already sent a query to
	let available: Vec<_> = self.short_list.iter()
	    .filter(|c| !self.already_queried.contains(&c.node_id))
	    .filter(|c| !self.in_flight.contains_key(&c.node_id))
	    .take(self.alpha - self.in_flight.len())
	    .cloned()
	    .collect();


	for info in available {
	    if self.already_queried.contains(&info.node_id) {
		continue
	    }
	    if self.in_flight.contains_key(&info.node_id) {
		continue
	    }
	    let query = match self.kind {
		LookupKind::Node => {
		    Message::FindNode {
			node_id: self.my_node_id,
			target: self.target,
		    }
		}
		LookupKind::Value => {
		    Message::FindValue {
			node_id: self.my_node_id,
			key: self.target,
		    }
		}
	    };
            let bytes = rmp_serde::to_vec(&query).expect("serialize FindNode");
            effects.push(Effect::Send {
                addr: SocketAddr::new(info.ip_address, info.udp_port),
                bytes,
            });

	    // now set a deadline and add to in flight
	    let deadline = Instant::now() + LOOKUP_TIMEOUT;
	    self.in_flight.insert(info.node_id, deadline);
	}
	effects
    }
}

struct PendingLookup {
    lookup: Lookup,
    deadline: Instant, // TODO: does a lookup itself need a deadline, or are the in flight request timeouts sufficient?
}

pub struct ProtocolManager {
    pub node: Node,
    pub socket: UdpSocket,
    pub alpha: usize, // concurrency parameter
    pub pending_probes: HashMap<ProbeID, PendingProbe>,
    pub pending_lookups: HashMap<NodeID, PendingLookup>,
}

impl ProtocolManager {
    pub fn new(node: Node, socket: UdpSocket, alpha: usize) -> Self {
        let pending_probes: HashMap<ProbeID, PendingProbe> = HashMap::new();
        let pending_lookups: HashMap<NodeID, PendingLookup> = HashMap::new();
        Self { node, socket, alpha, pending_probes, pending_lookups }
    }

    fn observe_contact(&mut self, src_addr: SocketAddr, node_id: NodeID) -> Option<Effect> {
        let peer = NodeInfo {
            ip_address: src_addr.ip(),
            udp_port: src_addr.port(), // use observed source port (NAT-friendly)
            node_id,
        };

        loop {
            match self.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => {
                    // Keep looping until a split does not happen.
                    // It is possible (though extremely unlikely) that even though we split the leaf bucket,
                    // all existing nodes got moved to the same new bucket, and therefore we need to
                    // continue splitting.
                    continue;
                }
		InsertResult::Full { lru  } => {
                    let probe_id = ProbeID::new_random();
		    let ping = Message::Ping {
			node_id: self.node.my_info.node_id,
			probe_id,
                    };
                    let bytes = rmp_serde::to_vec(&ping).expect("serialize probe Ping");
                    return Some(Effect::StartProbe { peer:lru, probe_id, bytes });
		}
		// TODO: check if we care about other insert result variants
                _other => break None,
            }
        }
    }

    async fn handle_message(
        &mut self,
        msg: Message,
        src_addr: SocketAddr,
    ) -> anyhow::Result<Vec<Effect>> {
        let mut effects = Vec::new();
        let node_id = match msg {
            Message::Ping { node_id, probe_id } => {
                println!("Received Ping from {node_id:?}");
                let pong = Message::Pong {
                    node_id: self.node.my_info.node_id,
		    probe_id,
                };
                let bytes = rmp_serde::to_vec(&pong)?;
                effects.push(Effect::Send {
                    addr: src_addr,
                    bytes,
                });
		node_id
            }

            Message::Pong { node_id, probe_id } => {
                println!("Received Pong from {node_id:?}");
                // Maybe mark the node as alive or update routing table
		if let Some(pending) = self.pending_probes.remove(&probe_id) {
		    self.node.routing_table.resolve_probe(pending.peer, /*alive =*/ true);
		} else {
		    // TODO: is there more to think about here?
		    println!("A Pong was received without an associated probe_id. Interesting. {node_id:?}");
		}
		node_id
            }

            Message::Store {
                node_id,
                key,
                value,
            } => {
                println!("Store request: key={key:?}, value={value:?}");
                // Store the value in your local store or database
                self.node.store(key, value);
		node_id
            }

            Message::FindNode { node_id, target } => {
                println!("FindNode request: looking for {target:?}");
                // Find closest nodes to the given ID in your routing table
                let closest = self.node.routing_table.k_closest(target);
                let nodes = Message::Nodes {
                    node_id: self.node.my_info.node_id,
                    target,
                    nodes: closest,
                };
                let bytes = rmp_serde::to_vec(&nodes)?;
                effects.push(Effect::Send {
                    addr: src_addr,
                    bytes,
                });
		node_id
            }

            Message::Nodes {
                node_id,
                target,
                nodes,
            } => {
		// observe all the new nodes we just learned about
		for n in nodes {
		    if let Some(eff) = self.observe_contact(SocketAddr::new(n.ip_address, n.udp_port), n.node_id) {
			effects.push(eff);
		    }
		}

		// TODO: the target should correspond to a pending lookup right?

		node_id
            }

            Message::FindValue { node_id, key } => {
                println!("FindValue request: key={key:?}");
                // Lookup the value, or return closest nodes if not found
                if let Some(value) = self.node.get(&key) {
                    let found = Message::ValueFound {
                        node_id: self.node.my_info.node_id,
                        key,
                        value: value.clone(),
                    };
                    let bytes = rmp_serde::to_vec(&found)?;
                    effects.push(Effect::Send {
                        addr: src_addr,
                        bytes,
                    });
                } else {
                    // we don't hold the value itself, so we need to check for nodes closer to to the key
                    let closest = self.node.routing_table.k_closest(key);
                    let nodes = Message::Nodes {
                        node_id: self.node.my_info.node_id,
                        target: key,
                        nodes: closest,
                    };
                    let bytes = rmp_serde::to_vec(&nodes)?;
                    effects.push(Effect::Send {
                        addr: src_addr,
                        bytes,
                    });
                }
		node_id
            }
            Message::ValueFound {
                node_id,
                key,
                value,
            } => {

		if let Some(lookup) = self.pending_lookups.get_mut(&key) {
		    // TODO: the target should correspond to a pending lookup right?
		    todo!()
		    // lookup.complete_with_value(value.clone());
		}

		// Optionally Cache the value in our own local storage
		self.node.store(key, value);
		node_id
            }

	    // TODO: this is code for developing the lookup functionality
	    // eventually we will receive commands from our user.
	    Message::StartLookup { node_id, key, kind } => {
		let initial = self.node.routing_table.k_closest(key);
		let mut lookup = Lookup::new(self.alpha, self.node.my_info.node_id, key, kind, initial);

		let lookup_effects = lookup.top_up_alpha_requests();
		println!("\n\n\n{lookup_effects:?}");
		effects.extend(lookup_effects);


		let deadline = Instant::now() + LOOKUP_TIMEOUT;
		self.pending_lookups.insert(key, PendingLookup{ lookup, deadline });
		node_id
	    }
        };

	// now we add the peer to our routing_table
	if let Some(eff) = self.observe_contact(src_addr, node_id) {
	    effects.push(eff);
	}

        Ok(effects)
    }

    async fn apply_effect(&mut self, effect: Effect) {
	match effect {
	    Effect::Send { addr, bytes } => {
		if let Err(e) = self.socket.send_to(&bytes, addr).await {
		    eprintln!("Failed to send to {addr}: {e}");
		}
	    }
	    Effect::StartProbe { peer, probe_id, bytes } => {
                let addr = SocketAddr::new(peer.ip_address, peer.udp_port);
		if let Err(e) = self.socket.send_to(&bytes, addr).await {
		    eprintln!("Failed to send probe to {addr}: {e}");
		} else {
		    // Record the probe so we can resolve it later
		    let deadline = Instant::now() + PROBE_TIMEOUT;
		    self.pending_probes.insert(probe_id, PendingProbe{ peer, deadline });
		}
	    }
	}
    }

    /// This for messages in a loop, and respond accordingly
    pub async fn run(mut self) {
        let mut buf = [0u8; 1024];

        let mut ticker = interval(Duration::from_secs(1)); // how often do we clean expired probes
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
            // message receive arm
            result = self.socket.recv_from(&mut buf) =>  {
                match result {
                Ok((len, src_addr)) => {
                    println!("Received {len} bytes from {src_addr}");
                    let msg = rmp_serde::from_slice::<Message>(&buf[..len]);
                    match msg {
                    Ok(msg) => {
                        let effects = self.handle_message(msg, src_addr).await;
			if let Ok(effects) = effects {
			    for eff in effects {
				self.apply_effect(eff).await;
			    }
			}
                    }
                    Err(e) => {
                        eprintln!("Error receiving message: {e}");
                        continue;
                    }
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving message: {e}");
                    continue;
                }
                }
            }

		//
		// TODO: another arm that reads from a channel from the user of the DHT
		// it can start lookups

            // timeout arm (sweep or heap-based)
            _ = ticker.tick() => {
                let now = Instant::now();
                let mut expired = Vec::new();
                for (probe_id, pending) in self.pending_probes.iter() {
                if pending.deadline <= now {
                    expired.push((*probe_id, *pending));
                }
                }
                for (probe_id, pending) in expired {
                    self.pending_probes.remove(&probe_id);
                    // tell the table that this probe timed out
                    let _ = self.node.routing_table.resolve_probe(pending.peer, /*alive=*/false);
                }

		//TODO: also check for expired lookups
            }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_support::test_support::{id_with_first_byte, make_peer};
    use crate::node::routing_table::InsertResult;

    #[tokio::test]
    async fn test_receive_ping() {
        let node = Node::new(20, "127.0.0.1".parse().unwrap(), 8081);
        let dummy_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap(); // ephemeral port
        let mut pm = ProtocolManager::new(node, dummy_socket, 3);

        let src_id = NodeID::new();
	let probe_id= ProbeID::new_random();
        let msg = Message::Ping { node_id: src_id, probe_id };
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
                assert!(
                    matches!(reply, Message::Pong { node_id, probe_id: pid } if node_id == pm.node.my_info.node_id && pid == probe_id )
                );
            }
            _ => panic!("expected Send Pong effect, got {:?}", effect),
        }
    }

    #[tokio::test]
    async fn test_receive_store_and_find_value() {
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8081);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap(); // ephemeral port
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 3);

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
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8082);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 3);

        // Insert a value into this node's storage directly
        let key: Key = NodeID::from_hashed(&"world");
        let value = b"world".to_vec();
        pm.node.store(key, value.clone());

        // Send FindValue
        let src_id: NodeID = NodeID::new();
        let msg: Message = Message::FindValue {
            node_id: src_id,
            key,
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
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8083);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 3);

        let key: Key = NodeID::from_hashed(&"missing");

        // Send FindValue for a key that isn't in storage
        let src_id: NodeID = NodeID::new();
        let msg: Message = Message::FindValue {
            node_id: src_id,
            key,
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
                         if target == key)
                );
            }
            _ => panic!("expected Send Nodes effect, got {:?}", effect),
        }
	// sanity check that the node that made the request got added to the tree
	assert!(pm.node.routing_table.find(src_id).is_some());
    }

    #[tokio::test]
    async fn test_start_lookup_sends_alpha_queries() {
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8090);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 3);

        // Prepare peers with known distances to the target
        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 5001, 0x00); // closest
        let p2 = make_peer(2, 5002, 0x01);
        let p3 = make_peer(3, 5003, 0x02);
        let p4 = make_peer(4, 5004, 0x80); // far

        // Helper to absorb splits
        let mut insert = |peer: NodeInfo| {
            loop {
                match pm.node.routing_table.try_insert(peer) {
                    InsertResult::SplitOccurred => continue,
                    _ => break,
                }
            }
        };
        insert(p1);
        insert(p2);
        insert(p3);
        insert(p4);

        // Start the lookup
        let key: Key = NodeID(target.0);
        let src_id: NodeID = NodeID::new();
        let msg = Message::StartLookup { node_id: src_id, key, kind: LookupKind::Value };
        let src: SocketAddr = "127.0.0.1:4002".parse().unwrap();

        let effects = pm.handle_message(msg, src).await.unwrap();

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


	// start a new lookup on nodes and confirm we get FindNode messages instead
	let msg = Message::StartLookup { node_id: src_id, key, kind: LookupKind::Node };
        let src: SocketAddr = "127.0.0.1:4002".parse().unwrap();

        let effects = pm.handle_message(msg, src).await.unwrap();

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


    }

    #[tokio::test]
    async fn test_nodes_reply_tops_up_lookup() {
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8091);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 3);

        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 6001, 0x00);
        let p2 = make_peer(2, 6002, 0x01);
        let p3 = make_peer(3, 6003, 0x02);

        let mut insert = |peer: NodeInfo| {
            loop {
                match pm.node.routing_table.try_insert(peer) {
                    InsertResult::SplitOccurred => continue,
                    _ => break,
                }
            }
        };
        insert(p1);
        insert(p2);
        insert(p3);

        // Start the lookup
        let key: Key = NodeID(target.0);
        let src_id: NodeID = NodeID::new();
        let start = Message::StartLookup { node_id: src_id, key, kind: LookupKind::Value };
        let src: SocketAddr = "127.0.0.1:4100".parse().unwrap();
        let _ = pm.handle_message(start, src).await.unwrap();

        // Nodes reply from p1 introducing a new peer p4
        let p4 = make_peer(4, 6004, 0x03);
        let nodes_msg = Message::Nodes {
            node_id: p1.node_id,
            target,
            nodes: vec![p4],
        };
        let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
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
        // Alpha=2, 3 peers → expect 2 initial requests, then after timeout a top-up to the 3rd
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8092);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 2);

        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 6101, 0x00); // closest
        let p2 = make_peer(2, 6102, 0x01); // next
        let p3 = make_peer(3, 6103, 0x80); // far

        // Insert peers, absorbing splits
        let mut insert = |peer: NodeInfo| {
            loop {
                match pm.node.routing_table.try_insert(peer) {
                    InsertResult::SplitOccurred => continue,
                    _ => break,
                }
            }
        };
        insert(p1);
        insert(p2);
        insert(p3);

        // Start lookup: should send 2 initial FindValue requests (alpha=2)
        let key: Key = NodeID(target.0);
        let src_id: NodeID = NodeID::new();
        let start = Message::StartLookup { node_id: src_id, key, kind: LookupKind::Value };
        let src: SocketAddr = "127.0.0.1:4200".parse().unwrap();
        let effects = pm.handle_message(start, src).await.unwrap();

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
        let effects = pm.test_sweep_timeouts_and_topup();

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
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8094);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 2);

        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 7001, 0x00); // closest
        let p2 = make_peer(2, 7002, 0x01); // next
        let p_new = make_peer(3, 7003, 0x02); // new candidate introduced by p1

        // Helper to absorb possible splits while inserting
        let mut insert = |peer: NodeInfo| {
            loop {
                match pm.node.routing_table.try_insert(peer) {
                    InsertResult::SplitOccurred => continue,
                    _ => break,
                }
            }
        };
        insert(p1);
        insert(p2);

        // Start the lookup for this target
        let key: Key = NodeID(target.0);
        let src_id: NodeID = NodeID::new();
        let start = Message::StartLookup { node_id: src_id, key, kind: LookupKind::Value };
        let src: SocketAddr = "127.0.0.1:4300".parse().unwrap();
        let _ = pm.handle_message(start, src).await.unwrap();

        // Act: simulate a Nodes reply from p1 that introduces p_new
        let nodes_msg = Message::Nodes {
            node_id: p1.node_id,
            target,
            nodes: vec![p_new],
        };
        let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
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
}

// Test-only hook to drive timeout sweeps without the async run loop.
#[cfg(test)]
impl ProtocolManager {
    fn test_sweep_timeouts_and_topup(&mut self) -> Vec<Effect> {
        // Placeholder: production code should sweep per-request deadlines,
        // then call the same top-up logic and return the resulting effects.
        Vec::new()
    }
}
