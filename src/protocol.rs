use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::{sync::{mpsc, oneshot}};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};

use crate::{
    node::identifier::{Key, NodeID, NodeInfo, ProbeID},
    node::routing_table::InsertResult,
    node::storage::Value,
    node::Node,
};
use std::collections::{HashMap, HashSet};
//use std::time::Instant;

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Serialize, Deserialize, Debug)]
pub enum LookupKind {
    Node,  // FIND_NODE
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


pub enum Command {
    Get {
	key: Key,
	value: Value,
	response: mpsc::Sender<LookupResult>,
    },
    Put{
	key: Key,
	value: Value,
	response: mpsc::Sender<LookupResult>,
    }
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

#[derive(Debug, Clone)]
enum LookupResult {
    ValueFound(Value),
    NodeFound(NodeInfo),
    Closest(Vec<NodeInfo>),
}


struct Lookup {
    k: usize,
    alpha: usize,
    my_node_id: NodeID,
    // keep the target as a NodeID, even though sometimes it is a key
    target: NodeID,
    kind: LookupKind,
    short_list: Vec<NodeInfo>,
    already_queried: HashSet<NodeID>, // indicate who we have sent a request to already
    in_flight: HashMap<NodeID, Instant>, // active queries with deadlines
}

impl Lookup {
    pub fn new(
        k: usize,
        alpha: usize,
        my_node_id: NodeID,
        target: NodeID,
        kind: LookupKind,
        initial_candidates: Vec<NodeInfo>,
    ) -> Self {
        let mut short_list = initial_candidates;
        short_list.sort_by_key(|n| n.node_id.distance(&target));
        Lookup {
            k,
            alpha,
            my_node_id,
            target,
            kind,
            short_list,
            already_queried: HashSet::new(),
            in_flight: HashMap::new(),
        }
    }

    /// Check if any of the in-flight queries have timed-out, and if so, remove them.
    pub fn sweep_expired(&mut self, now: Instant) {
        let mut expired = Vec::new();
        for (key, deadline) in self.in_flight.iter() {
            if *deadline <= now {
                expired.push(*key);
            }
        }
        for key in expired {
            self.in_flight.remove(&key);
        }
    }

    /// main method to be called on a lookup struct
    /// If there are fewer than `alpha` queries in flight, we return Effects to top up the difference.
    /// This can be called when we first start a lookup, or when we drive it forward after receiving a Nodes message.
    pub fn top_up_alpha_requests(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();

        // find up to alpha candidate nodes that we haven't already sent a query to
        let available: Vec<_> = self
            .short_list
            .iter()
            .filter(|c| !self.already_queried.contains(&c.node_id))
            .filter(|c| !self.in_flight.contains_key(&c.node_id))
            .take(self.alpha - self.in_flight.len())
            .cloned()
            .collect();

        for info in available {
            let query = match self.kind {
                LookupKind::Node => Message::FindNode {
                    node_id: self.my_node_id,
                    target: self.target,
                },
                LookupKind::Value => Message::FindValue {
                    node_id: self.my_node_id,
                    key: self.target,
                },
            };
            let bytes = rmp_serde::to_vec(&query).expect("serialize FindNode");
            effects.push(Effect::Send {
                addr: SocketAddr::new(info.ip_address, info.udp_port),
                bytes,
            });

            // now set a deadline and add to in flight
            let deadline = Instant::now() + LOOKUP_TIMEOUT;
            self.in_flight.insert(info.node_id, deadline);
            self.already_queried.insert(info.node_id);
        }
        effects
    }

    /// A Nodes response message for this lookup was received.
    /// We merge them into our existing shortlist, sort by distance to the target, then only
    /// keep the top k.
    pub fn merge_new_nodes(&mut self, nodes: Vec<NodeInfo>) {
        self.short_list.extend(nodes);

        let mut seen = HashSet::new();
        self.short_list.retain(|n| seen.insert(n.node_id));

        // sort by distance to target
        self.short_list
            .sort_by_key(|n| n.node_id.distance(&self.target));

        if self.short_list.len() > self.k {
            self.short_list.truncate(self.k);
        }
    }


    /// Returns the final results if applicable, or None, if there are still outstanding
    /// requests and we haven't found a target node yet.
    /// Note: this method will not find a target Value, since that would get returned by a
    /// ValueFound message instead.
    pub fn possible_final_result(&self) -> Option<LookupResult> {
	if let LookupKind::Node = self.kind {
	    // we are looking for a specific node;
	    // if it has been found by this point, then we are done.
	    let target_node = self
		.short_list.iter()
		.find(|x| x.node_id == self.target);
	    if let Some(target_node) = target_node {
		return Some(LookupResult::NodeFound(*target_node));
	    }
	}

	if self.in_flight.is_empty() {
	    // nothing left to wait on, so return whatever we have
	    return Some(LookupResult::Closest(self.short_list.clone()));
	}

	// otherwise, if we have more requests in flight, then lets
	// keep waiting -> no final result to report
	None
    }


}

struct PendingLookup {
    lookup: Lookup,
    deadline: Instant, // TODO: does a lookup itself need a deadline, or are the in flight request timeouts sufficient?
}

pub struct ProtocolManager {
    pub node: Node,
    pub socket: UdpSocket,
    rx: mpsc::Receiver<Command>,
    pub k: usize,
    pub alpha: usize, // concurrency parameter
    pub pending_probes: HashMap<ProbeID, PendingProbe>,
    pub pending_lookups: HashMap<NodeID, PendingLookup>,
}

impl ProtocolManager {
    pub fn new(socket: UdpSocket, rx: mpsc::Receiver<Command>, k: usize, alpha: usize) -> anyhow::Result<Self> {
	let addr = socket.local_addr()?;
	let ip = addr.ip();
	let port = addr.port();

	let node = Node::new(k, ip, port);

        let pending_probes: HashMap<ProbeID, PendingProbe> = HashMap::new();
        let pending_lookups: HashMap<NodeID, PendingLookup> = HashMap::new();
        Ok(Self {
            node,
            socket,
	    rx,
            k,
            alpha,
            pending_probes,
            pending_lookups,
        })
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
                InsertResult::Full { lru } => {
                    let probe_id = ProbeID::new_random();
                    let ping = Message::Ping {
                        node_id: self.node.my_info.node_id,
                        probe_id,
                    };
                    let bytes = rmp_serde::to_vec(&ping).expect("serialize probe Ping");
                    return Some(Effect::StartProbe {
                        peer: lru,
                        probe_id,
                        bytes,
                    });
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
                    self.node
                        .routing_table
                        .resolve_probe(pending.peer, /*alive =*/ true);
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
                for n in &nodes {
                    if let Some(eff) =
                        self.observe_contact(SocketAddr::new(n.ip_address, n.udp_port), n.node_id)
                    {
                        effects.push(eff);
                    }
                }

		let mut remove_lookup: bool = false;  // remove if there are no more in-flight requests
                if let Some(pending_lookup) = self.pending_lookups.get_mut(&target) {
                    pending_lookup.lookup.in_flight.remove(&node_id);
                    pending_lookup.lookup.merge_new_nodes(nodes);


                    let lookup_effects = pending_lookup.lookup.top_up_alpha_requests();
                    effects.extend(lookup_effects);

		    if pending_lookup.lookup.is_finished() {
			remove_lookup = true;
			match pending_lookup.lookup.kind {
			    LookupKind::Node => {
				// they were looking for a node; if it is in our remaining shortlist
			    }
			    LookupKind::Value => {
				// if a find value lookup converged on a list of nodes, then we
				// never actually found the value
			    }

			}


			// TODO: return to the user via the channel when that exists
                     }
                } else {
		    // we got a nodes message with no corresponding lookup... curious.
		}
		if remove_lookup {
		    self.pending_lookups.remove(&target);
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
                if let Some(_pending_lookup) = self.pending_lookups.remove(&key) {
                    // we drop the lookup entirely once we get back the value
                    println!("Lookup for {key:?} completed with value from {node_id:?}");
                }

                // Optionally Cache the value in our own local storage
                self.node.store(key, value);

                // TODO: once we have a user DHT channel most likely, we should set the value back to them here I think.

                node_id
            }

            // TODO: this is code for developing the lookup functionality
            // eventually we will receive commands from our user.
            Message::StartLookup { node_id, key, kind } => {
                let initial = self.node.routing_table.k_closest(key);
                let mut lookup = Lookup::new(
                    self.k,
                    self.alpha,
                    self.node.my_info.node_id,
                    key,
                    kind,
                    initial,
                );

                let lookup_effects = lookup.top_up_alpha_requests();
                effects.extend(lookup_effects);

                let deadline = Instant::now() + LOOKUP_TIMEOUT;
                self.pending_lookups
                    .insert(key, PendingLookup { lookup, deadline });
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
            Effect::StartProbe {
                peer,
                probe_id,
                bytes,
            } => {
                let addr = SocketAddr::new(peer.ip_address, peer.udp_port);
                if let Err(e) = self.socket.send_to(&bytes, addr).await {
                    eprintln!("Failed to send probe to {addr}: {e}");
                } else {
                    // Record the probe so we can resolve it later
                    let deadline = Instant::now() + PROBE_TIMEOUT;
                    self.pending_probes
                        .insert(probe_id, PendingProbe { peer, deadline });
                }
            }
        }
    }

    /// check for and resolve expired probes, and check for expired lookups.
    /// Returns the possible new topup requests if there were expired lookups.
    fn sweep_timeouts_and_topup(&mut self, now: Instant) -> Vec<Effect> {
        let mut expired_probes = Vec::new();
        for (probe_id, pending_probe) in self.pending_probes.iter() {
            if pending_probe.deadline <= now {
                expired_probes.push((*probe_id, *pending_probe));
            }
        }
        for (probe_id, pending) in expired_probes {
            self.pending_probes.remove(&probe_id);
            // tell the table that this probe timed out
            let _ = self
                .node
                .routing_table
                .resolve_probe(pending.peer, /*alive=*/ false);
        }

        // each lookup can clear expired lookups and top them up
        let mut lookup_effects = Vec::new();
        for (_key, pending_lookup) in self.pending_lookups.iter_mut() {
            pending_lookup.lookup.sweep_expired(now);
            let current_effects = pending_lookup.lookup.top_up_alpha_requests();
            lookup_effects.extend(current_effects);
        }
        lookup_effects
    }

    /// This for messages in a loop, and respond accordingly
    pub async fn run(mut self) {
        let mut buf = [0u8; 1024];

        let mut ticker = interval(Duration::from_millis(500)); // how often do we clean expired probes and lookups
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

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

                _ = ticker.tick() => {
                    let now = Instant::now();
            let lookup_effects = self.sweep_timeouts_and_topup(now);

                    for eff in lookup_effects {
            self.apply_effect(eff).await;
                    }

                }
                }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::node::routing_table::InsertResult;
    use crate::test_support::test_support::{id_with_first_byte, make_peer};

    #[tokio::test]
    async fn test_receive_ping() {
        let node = Node::new(20, "127.0.0.1".parse().unwrap(), 8081);
        let dummy_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap(); // ephemeral port
        let mut pm = ProtocolManager::new(node, dummy_socket, 20, 3);

        let src_id = NodeID::new();
        let probe_id = ProbeID::new_random();
        let msg = Message::Ping {
            node_id: src_id,
            probe_id,
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
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 3);

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
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 3);

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
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 3);

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
                         if target == key));
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
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 3);

        // Prepare peers with known distances to the target
        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 5001, 0x00); // closest
        let p2 = make_peer(2, 5002, 0x01);
        let p3 = make_peer(3, 5003, 0x02);
        let p4 = make_peer(4, 5004, 0x80); // far

        // Helper to absorb splits
        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);
        insert(p3);
        insert(p4);

        // Start the lookup
        let key: Key = NodeID(target.0);
        let src_id: NodeID = NodeID::new();
        let msg = Message::StartLookup {
            node_id: src_id,
            key,
            kind: LookupKind::Value,
        };
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
    }

    #[tokio::test]
    async fn test_nodes_reply_tops_up_lookup() {
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8091);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 3);

        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 6001, 0x00);
        let p2 = make_peer(2, 6002, 0x01);
        let p3 = make_peer(3, 6003, 0x02);

        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);
        insert(p3);

        // Start the lookup
        let key: Key = NodeID(target.0);
        let src_id: NodeID = NodeID::new();
        let start = Message::StartLookup {
            node_id: src_id,
            key,
            kind: LookupKind::Value,
        };
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
        // Alpha=2, 3 peers → expect 2 initial requests, then after timeout a top-up to the 3rd peer
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8092);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 2);

        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 6101, 0x00); // closest
        let p2 = make_peer(2, 6102, 0x01); // next
        let p3 = make_peer(3, 6103, 0x80); // far

        // Insert peers, absorbing splits
        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);
        insert(p3);

        // Start lookup: should send 2 initial FindValue requests (alpha=2)
        let key: Key = NodeID(target.0);
        let src_id: NodeID = NodeID::new();
        let start = Message::StartLookup {
            node_id: src_id,
            key,
            kind: LookupKind::Value,
        };
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
        let now = Instant::now();

        let effects = pm.sweep_timeouts_and_topup(now);

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
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 2);

        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 7001, 0x00); // closest
        let p2 = make_peer(2, 7002, 0x01); // next
        let p_new = make_peer(3, 7003, 0x02); // new candidate introduced by p1

        // Helper to absorb possible splits while inserting
        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);

        // Start the lookup for this target
        let key: Key = NodeID(target.0);
        let src_id: NodeID = NodeID::new();
        let start = Message::StartLookup {
            node_id: src_id,
            key,
            kind: LookupKind::Value,
        };
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

    #[tokio::test]
    async fn test_value_lookup_multiple_hops_via_nodes() {
        // Make a lookup that requires multiple jumps: p1 -> p2 -> p3, where p3 returns the value
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8096);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        // alpha=1 to force sequential hops
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 1);

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

        // Start the lookup (Value)
        let src_id: NodeID = NodeID::new();
        let start = Message::StartLookup {
            node_id: src_id,
            key,
            kind: LookupKind::Value,
        };
        let src: SocketAddr = "127.0.0.1:4400".parse().unwrap();
        let effects = pm.handle_message(start, src).await.unwrap();

        // Expect a FindValue sent to p1
        let mut sent_to_p1 = false;
        for eff in effects.into_iter() {
            if let Effect::Send { addr, bytes } = eff {
                if addr == SocketAddr::new(p1.ip_address, p1.udp_port) {
                    if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                        if matches!(msg, Message::FindValue { .. }) {
                            sent_to_p1 = true;
                        }
                    }
                }
            }
        }
        assert!(sent_to_p1, "lookup should start by querying p1");

        // Simulate p1 responding with a Nodes message introducing p2
        let nodes_from_p1 = Message::Nodes {
            node_id: p1.node_id,
            target,
            nodes: vec![p2],
        };
        let p1_addr = SocketAddr::new(p1.ip_address, p1.udp_port);
        let effects = pm.handle_message(nodes_from_p1, p1_addr).await.unwrap();

        // Expect a top-up FindValue to p2 (alpha=1, slot freed by p1's reply)
        let mut sent_to_p2 = false;
        for eff in effects.into_iter() {
            if let Effect::Send { addr, bytes } = eff {
                if addr == SocketAddr::new(p2.ip_address, p2.udp_port) {
                    if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                        if matches!(msg, Message::FindValue { .. }) {
                            sent_to_p2 = true;
                        }
                    }
                }
            }
        }
        assert!(sent_to_p2, "Nodes from p1 should trigger query to p2");

        // Simulate p2 responding with a Nodes message introducing p3
        let nodes_from_p2 = Message::Nodes {
            node_id: p2.node_id,
            target,
            nodes: vec![p3],
        };
        let p2_addr = SocketAddr::new(p2.ip_address, p2.udp_port);
        let effects = pm.handle_message(nodes_from_p2, p2_addr).await.unwrap();

        // Expect a top-up FindValue to p3
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
        assert!(sent_to_p3, "Nodes from p2 should trigger query to p3");

        // Finally, simulate p3 returning the value
        let value = b"hello-value".to_vec();
        let found = Message::ValueFound {
            node_id: p3.node_id,
            key,
            value: value.clone(),
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
        let node: Node = Node::new(20, "127.0.0.1".parse().unwrap(), 8097);
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new(node, dummy_socket, 20, 2);

        let target: NodeID = id_with_first_byte(0x00);
        let p1 = make_peer(1, 9001, 0x00); // closest
        let p2 = make_peer(2, 9002, 0x01); // next closest

        // Insert initial candidates into the routing table (absorb splits if they occur)
        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);

        // Start a Node lookup towards `target`
        let src_id: NodeID = NodeID::new();
        let start = Message::StartLookup {
            node_id: src_id,
            key: target, // for Node lookup, the "key" is the NodeID target
            kind: LookupKind::Node,
        };
        let src: SocketAddr = "127.0.0.1:4500".parse().unwrap();
        let effects = pm.handle_message(start, src).await.unwrap();

        // Expect exactly alpha initial FindNode queries
        let initial_sends: Vec<SocketAddr> = effects
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
        assert_eq!(initial_sends.len(), 2, "should send alpha FindNode requests");

        // Simulate a Nodes reply from p1 that does not improve the shortlist (duplicates only)
        let nodes_from_p1 = Message::Nodes {
            node_id: p1.node_id,
            target,
            nodes: vec![p1, p2],
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
        assert!(topups_after_p1.is_empty(), "no new queries should be issued");

        // Simulate a Nodes reply from p2 that also does not improve the shortlist
        let nodes_from_p2 = Message::Nodes {
            node_id: p2.node_id,
            target,
            nodes: vec![p1, p2],
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
        assert!(topups_after_p2.is_empty(), "no further queries should be issued");

        // Optional: sweep to mimic periodic maintenance; still no effects expected
        let effects = pm.sweep_timeouts_and_topup(Instant::now());
        let find_node_sends = effects
            .into_iter()
            .filter(|eff| match eff {
                Effect::Send { bytes, .. } => {
                    matches!(rmp_serde::from_slice::<Message>(bytes), Ok(Message::FindNode { .. }))
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
}
