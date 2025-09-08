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
    // keep the target as a NodeID, even thought sometimes it is a
    target: NodeID,
    short_list: Vec<NodeInfo>,
    queried: HashSet<NodeID>, // indicate who we have sent a request to already
    in_flight: HashMap<ProbeID, NodeInfo>, // active queries
}

struct PendingLookup {
    target: NodeID,
    deadline: Instant,
}

pub struct ProtocolManager {
    pub node: Node,
    pub socket: UdpSocket,
    pub alpha: usize, // concurrency parameter
    pub pending_probes: HashMap<ProbeID, PendingProbe>,
}

impl ProtocolManager {
    pub fn new(node: Node, socket: UdpSocket, alpha: usize) -> Self {
        let pending_probes: HashMap<ProbeID, PendingProbe> = HashMap::new();
        Self { node, socket, alpha, pending_probes }
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
                other => break None,
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
                    let closest = self.node.routing_table.k_closest(key.to_node_id());
                    let nodes = Message::Nodes {
                        node_id: self.node.my_info.node_id,
                        target: key.to_node_id(),
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

        // where we store pending probes and clean them up if they time out

        //let mut pending_lookups: HashMap<NodeID, PendingProbe> = HashMap::new();

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
        let key = Key::new(&"world");
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
        let key = Key::new(&b"hello");
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

        let key = Key::new(&b"missing");

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
                         if target == key.to_node_id())
                );
            }
            _ => panic!("expected Send Nodes effect, got {:?}", effect),
        }
	// sanity check that the node that made the request got added to the tree
	assert!(pm.node.routing_table.find(src_id).is_some());
    }
}
