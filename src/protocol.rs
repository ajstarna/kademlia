use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};

use crate::{
    core::identifier::{Key, NodeID, NodeInfo, RpcId},
    core::routing_table::InsertResult,
    core::storage::Value,
    core::NodeState,
};
use std::collections::HashMap;
//use std::time::Instant;
use tracing::{debug, error, info, trace, warn};

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
// Default TTL for value storage (24 hours) in seconds
const DEFAULT_STORE_TTL_SECS: u64 = 24 * 60 * 60;
mod command;
mod lookup;
pub use self::command::Command;
use self::lookup::{Lookup, LookupKind, PendingLookup};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Peer,
    Client,
}

// each message type includes the NodeID of the sender
// is_client indicates that the sender is not a full peer and should not be added to our routing table
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Message {
    Ping {
        node_id: NodeID,
        rpc_id: RpcId, // request/response correlation token
        is_client: bool,
    },
    Pong {
        node_id: NodeID,
        rpc_id: RpcId, // request/response correlation token
        is_client: bool,
    },
    Store {
        node_id: NodeID,
        key: Key,
        value: Value,
        ttl_secs: u64,
        is_client: bool,
    },
    FindNode {
        node_id: NodeID,
        target: NodeID,
        rpc_id: RpcId,
        is_client: bool,
    },
    Nodes {
        node_id: NodeID,
        target: NodeID, // we need to include the target to map the nodes to the lookup
        rpc_id: RpcId,
        nodes: Vec<NodeInfo>,
        is_client: bool,
    },
    FindValue {
        node_id: NodeID,
        key: Key,
        rpc_id: RpcId,
        is_client: bool,
    },
    ValueFound {
        node_id: NodeID,
        key: Key,
        rpc_id: RpcId,
        value: Value,
        is_client: bool,
    },
    // Note: start of lookups is driven by Command from the API, not via a Message.
}

// Command enum is defined in protocol::command and re-exported above.

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
        lru: NodeInfo,
        candidates: Vec<NodeInfo>,
        rpc_id: RpcId,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
struct PendingProbe {
    lru: NodeInfo,
    candidates: Vec<NodeInfo>,
    deadline: Instant,
}

// Lookup-related types are defined in protocol::lookup submodule.

pub struct ProtocolManager {
    pub node: NodeState,
    pub socket: UdpSocket,
    rx: Option<mpsc::Receiver<Command>>, // Optional: commands from a library user
    pub k: usize,
    pub alpha: usize, // concurrency parameter
pending_probes: HashMap<RpcId, PendingProbe>,
pending_probe_by_lru: HashMap<NodeID, RpcId>,
    pending_lookups: HashMap<NodeID, PendingLookup>,
    role: NodeRole,
    // For bootstrap self-lookup: track rpc_id by seed address for initial FindNode(self) sends
    pending_bootstrap: HashMap<SocketAddr, RpcId>,
}

impl ProtocolManager {
    pub fn new(
        socket: UdpSocket,
        rx: mpsc::Receiver<Command>,
        k: usize,
        alpha: usize,
    ) -> anyhow::Result<Self> {
        let addr = socket.local_addr()?;
        let ip = addr.ip();
        let port = addr.port();

        let node = NodeState::new(k, ip, port);

        let pending_probes: HashMap<RpcId, PendingProbe> = HashMap::new();
        let pending_lookups: HashMap<NodeID, PendingLookup> = HashMap::new();
        Ok(Self {
            node,
            socket,
            rx: Some(rx),
            k,
            alpha,
            pending_probes,
            pending_probe_by_lru: HashMap::new(),
            pending_lookups,
            role: NodeRole::Peer,
            pending_bootstrap: HashMap::new(),
        })
    }

    pub fn new_client(
        socket: UdpSocket,
        rx: mpsc::Receiver<Command>,
        k: usize,
        alpha: usize,
    ) -> anyhow::Result<Self> {
        let mut pm = Self::new(socket, rx, k, alpha)?;
        pm.role = NodeRole::Client;
        Ok(pm)
    }

    /// Add a known peer directly into the routing table.
    /// This is primarily useful for tests or when seeding nodes programmatically
    /// without relying on receiving a message first.
    /// If the appropriate bucket is full, this will initiate a probe to the LRU
    /// entry per Kademlia rules.
    pub async fn add_known_peer(&mut self, peer: NodeInfo) {
        loop {
            match self.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => {
                    // Keep looping until a split does not happen.
                    continue;
                }
                InsertResult::Full { lru } => {
                    // Probe the LRU node to determine if it should be evicted.
                    if let Some(existing) = self.pending_probe_by_lru.get(&lru.node_id).cloned() {
                        if let Some(pp) = self.pending_probes.get_mut(&existing) {
                            pp.candidates.push(peer);
                        }
                        break;
                    }
                    let rpc_id = RpcId::new_random();
                    let ping = Message::Ping {
                        node_id: self.node.my_info.node_id,
                        rpc_id,
                        is_client: self.is_client(),
                    };
                    if let Ok(bytes) = rmp_serde::to_vec(&ping) {
                        let addr = SocketAddr::new(lru.ip_address, lru.udp_port);
                        // Best-effort send; errors are logged by apply_effect code paths elsewhere,
                        // but here we send directly for simplicity.
                        let _ = self.socket.send_to(&bytes, addr).await;
                        let deadline = Instant::now() + PROBE_TIMEOUT;
                        self.pending_probes.insert(
                            rpc_id,
                            PendingProbe {
                                lru,
                                candidates: vec![peer],
                                deadline,
                            },
                        );
                        self.pending_probe_by_lru.insert(lru.node_id, rpc_id);
                    }
                    break;
                }
                _ => break,
            }
        }
    }

    /// Construct a headless ProtocolManager without a command channel.
    /// e.g. useful to run a node that is not being used by the user-facing dht library.
    pub fn new_headless(socket: UdpSocket, k: usize, alpha: usize) -> anyhow::Result<Self> {
        let addr = socket.local_addr()?;
        let ip = addr.ip();
        let port = addr.port();

        let node = NodeState::new(k, ip, port);
        info!("init headless node with {:?}", node);
        Ok(Self {
            node,
            socket,
            rx: None,
            k,
            alpha,
            pending_probes: HashMap::new(),
            pending_probe_by_lru: HashMap::new(),
            pending_lookups: HashMap::new(),
            role: NodeRole::Peer,
            pending_bootstrap: HashMap::new(),
        })
    }

    /// Inverse-distance TTL based on leading zero bits of XOR distance
    /// Useful when caching a value on the nearest node that did not return a requested value
    /// during a lookup, as specified in the paper.
    /// Note: the paper does not give an exact formula.
    fn cache_ttl_secs_for(&self, key: NodeID, node: &NodeInfo) -> u64 {
        const BITS: f64 = 160.0;
        const MIN_TTL_SECS: f64 = (15 * 60) as f64; // 15 minutes
        const MAX_TTL_SECS: f64 = DEFAULT_STORE_TTL_SECS as f64; // 24 hours
        const GAMMA: f64 = 2.0; // shape parameter

        let dist = node.node_id.distance(&key);
        let lz = dist.leading_zeros() as f64;
        let f = (lz / BITS).clamp(0.0, 1.0);
        let ttl = MIN_TTL_SECS + (MAX_TTL_SECS - MIN_TTL_SECS) * f.powf(GAMMA);
        ttl.round() as u64
    }

    pub fn is_client(&self) -> bool {
        matches!(self.role, NodeRole::Client)
    }

    pub fn role(&self) -> NodeRole {
        self.role
    }

    fn observe_contact(&mut self, src_addr: SocketAddr, node_id: NodeID) -> Option<Effect> {
        let peer = NodeInfo {
            ip_address: src_addr.ip(),
            udp_port: src_addr.port(), // use observed source port (NAT-friendly)
            node_id,
        };

        debug!(?src_addr, "Observing peer");
        loop {
            match self.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => {
                    // Keep looping until a split does not happen.
                    // It is possible (though extremely unlikely) that even though we split the leaf bucket,
                    // all existing nodes got moved to the same new bucket, and therefore we need to
                    // continue splitting.
                    debug!("Split occurred");
                    continue;
                }
                InsertResult::Full { lru } => {
                    debug!("Full bucket");
                    let rpc_id = RpcId::new_random();
                    let ping = Message::Ping {
                        node_id: self.node.my_info.node_id,
                        rpc_id,
                        is_client: self.is_client(),
                    };
                    let bytes = rmp_serde::to_vec(&ping).expect("serialize probe Ping");
                    // If a probe for this LRU is already in flight, coalesce by appending the candidate.
                    if let Some(existing) = self.pending_probe_by_lru.get(&lru.node_id).cloned() {
                        if let Some(pp) = self.pending_probes.get_mut(&existing) {
                            pp.candidates.push(peer);
                        }
                        return None;
                    } else {
                        return Some(Effect::StartProbe {
                            lru,
                            candidates: vec![peer],
                            rpc_id,
                            bytes,
                        });
                    }
                }
                // TODO: check if we care about other insert result variants
                other => {
                    debug!("insert result = {:?}", other);
                    break None;
                }
            }
        }
    }

    async fn handle_command(&mut self, command: Command) -> anyhow::Result<Vec<Effect>> {
        let effects = match command {
            Command::Get { key, tx_value } => {
                // Get corresponds to a Value lookup
                info!(?key, "Get Command");
                self.start_lookup(key, LookupKind::Value, tx_value)
            }
            Command::Put {
                key,
                value,
                tx_done,
            } => {
                // Put: perform a Node lookup to find k closest nodes, then send Store to them
                info!(key=?key, value=?value, "Put Command");
                self.start_lookup_with_put(key, value, tx_done)
            }
            Command::Bootstrap { addrs, tx_done } => {
                info!(addrs=?addrs, "Bootstrap Command");
                let my_id = self.node.my_info.node_id;
                // Initialize a pending self-lookup with empty initial candidates
                let mut effs = self.init_lookup(
                    my_id,
                    LookupKind::Node,
                    None,
                    Vec::new(),
                    None,
                    Some(tx_done),
                    /*early_complete_on_empty=*/ false,
                );

                // Send initial FindNode(self) to the seed addresses, tracking rpc_id per address
                for addr in addrs {
                    let rpc_id = RpcId::new_random();
                    let query = Message::FindNode {
                        node_id: my_id,
                        target: my_id,
                        rpc_id,
                        is_client: self.is_client(),
                    };
                    let bytes = rmp_serde::to_vec(&query)?;
                    self.pending_bootstrap.insert(addr, rpc_id);
                    effs.push(Effect::Send { addr, bytes });
                }
                effs
            }
            Command::DebugHasValue { key, tx_has } => {
                let has = self.node.get(&key).is_some();
                let _ = tx_has.send(has);
                Vec::new()
            }
            Command::DebugPeerCount { tx_count } => {
                let cnt = self.node.routing_table.peer_count();
                let _ = tx_count.send(cnt);
                Vec::new()
            }
        };
        Ok(effects)
    }

    fn init_lookup(
        &mut self,
        key: NodeID,
        kind: LookupKind,
        tx_value: Option<oneshot::Sender<Option<Value>>>,
        initial: Vec<NodeInfo>,
        put_value: Option<Value>,
        tx_done: Option<oneshot::Sender<()>>,
        early_complete_on_empty: bool,
    ) -> Vec<Effect> {
        let mut lookup = Lookup::new(
            self.k,
            self.alpha,
            self.node.my_info.node_id,
            self.is_client(),
            key,
            kind,
            initial,
        );

        let lookup_effects = lookup.top_up_alpha_requests();

        if early_complete_on_empty && lookup_effects.is_empty() && lookup.in_flight.is_empty() {
            let rt_peers = self.node.routing_table.peer_count();
            match kind {
                LookupKind::Node => {
                    if rt_peers == 0 {
                        error!(
                            event="lookup_empty_initial",
                            kind="put/node",
                            role=?self.role(),
                            lookup_target=?key,
                            rt_peers,
                            "No initial peers; routing table is empty. Likely bootstrap misconfiguration or network issue. Completing without STOREs."
                        );
                    } else {
                        error!(
                            event="lookup_empty_initial_inconsistent",
                            kind="put/node",
                            role=?self.role(),
                            lookup_target=?key,
                            rt_peers,
                            "Shortlist empty despite non-empty routing table; this may be a bug. Completing without STOREs."
                        );
                    }
                    if let Some(done) = tx_done {
                        let _ = done.send(());
                    }
                }
                LookupKind::Value => {
                    if rt_peers == 0 {
                        error!(
                            event="lookup_empty_initial",
                            kind="get/value",
                            role=?self.role(),
                            lookup_target=?key,
                            rt_peers,
                            "No initial peers; routing table is empty. Likely bootstrap misconfiguration or network issue. Returning None."
                        );
                    } else {
                        error!(
                            event="lookup_empty_initial_inconsistent",
                            kind="get/value",
                            role=?self.role(),
                            lookup_target=?key,
                            rt_peers,
                            "Shortlist empty despite non-empty routing table; this may be a bug. Returning None."
                        );
                    }
                    if let Some(tx) = tx_value {
                        let _ = tx.send(None);
                    }
                }
            }
            return Vec::new();
        }

        let new_pending = PendingLookup {
            lookup,
            put_value,
            tx_done,
            tx_value,
        };

        self.pending_lookups.insert(key, new_pending);

        lookup_effects
    }

    fn start_lookup(
        &mut self,
        key: NodeID,
        kind: LookupKind,
        tx_value: oneshot::Sender<Option<Value>>,
    ) -> Vec<Effect> {
        let initial = self.node.routing_table.k_closest(key);
        self.init_lookup(
            key,
            kind,
            Some(tx_value),
            initial,
            None,
            None,
            /*early_complete_on_empty=*/ true,
        )
    }

    fn start_lookup_with_put(
        &mut self,
        key: NodeID,
        value: Value,
        tx_done: oneshot::Sender<()>,
    ) -> Vec<Effect> {
        let initial = self.node.routing_table.k_closest(key);
        self.init_lookup(
            key,
            LookupKind::Node,
            None,
            initial,
            Some(value),
            Some(tx_done),
            /*early_complete_on_empty=*/ true,
        )
    }

    async fn handle_message(
        &mut self,
        msg: Message,
        src_addr: SocketAddr,
    ) -> anyhow::Result<Vec<Effect>> {
        let mut effects = Vec::new();
        let (node_id, peer_is_client) = match msg {
            Message::Ping {
                node_id,
                rpc_id,
                is_client,
            } => {
                debug!(?node_id, "Received Ping");
                let pong = Message::Pong {
                    node_id: self.node.my_info.node_id,
                    rpc_id,
                    is_client: self.is_client(),
                };
                let bytes = rmp_serde::to_vec(&pong)?;
                effects.push(Effect::Send {
                    addr: src_addr,
                    bytes,
                });
                (node_id, is_client)
            }

            Message::Pong {
                node_id,
                rpc_id,
                is_client,
            } => {
                debug!(?node_id, "Received Pong");
                // Maybe mark the node as alive or update routing table
                if let Some(pending) = self.pending_probes.remove(&rpc_id) {
                    self.pending_probe_by_lru.remove(&pending.lru.node_id);
                    self.node
                        .routing_table
                        .resolve_probe(pending.lru, /*alive =*/ true);
                } else {
                    // TODO: is there more to think about here?
                    warn!(
                        ?node_id,
                        rpc_id = ?rpc_id,
                        "Pong received without matching probe; dropping and not observing contact"
                    );
                    return Ok(effects);
                }
                (node_id, is_client)
            }

            Message::Store {
                node_id,
                key,
                value,
                ttl_secs,
                is_client,
            } => {
                debug!(?key, value_len=?value.len(), "Store request");
                if !self.is_client() {
                    let ttl = std::time::Duration::from_secs(ttl_secs);
                    self.node.store_with_ttl(key, value, ttl);
                }
                (node_id, is_client)
            }

            Message::FindNode {
                node_id,
                target,
                rpc_id,
                is_client,
            } => {
                debug!(?target, "FindNode request");
                // Find closest nodes to the given ID in your routing table
                let closest = self.node.routing_table.k_closest(target);
                let nodes = Message::Nodes {
                    node_id: self.node.my_info.node_id,
                    target,
                    rpc_id,
                    nodes: closest,
                    is_client: self.is_client(),
                };
                let bytes = rmp_serde::to_vec(&nodes)?;
                effects.push(Effect::Send {
                    addr: src_addr,
                    bytes,
                });
                (node_id, is_client)
            }

            Message::Nodes { node_id, target, rpc_id, nodes, is_client } => {
                debug!(?target, ?rpc_id, count=%nodes.len(), "Received Nodes");
                let mut remove_lookup: bool = false; // remove if there are no more in-flight requests
                let i_am_client = self.is_client();
                // Validate rpc_id using immutable borrow
                let mut valid = false;
                if let Some(pl) = self.pending_lookups.get(&target) {
                    valid = pl.lookup.validate_reply(node_id, rpc_id);
                }
                if !valid && target == self.node.my_info.node_id {
                    // Bootstrap special-case: accept Nodes if rpc_id matches what we sent to this addr
                    if let Some(expected) = self.pending_bootstrap.get(&src_addr).copied() {
                        if expected == rpc_id {
                            valid = true;
                            // one-shot: clear tracking for this addr
                            self.pending_bootstrap.remove(&src_addr);
                        }
                    }
                }
                if valid {
                    // Observe contacts after validation
                    for n in &nodes {
                        if let Some(eff) =
                            self.observe_contact(SocketAddr::new(n.ip_address, n.udp_port), n.node_id)
                        {
                            effects.push(eff);
                        }
                    }
                    // Mutably update the lookup state using Lookup API
                    if let Some(pending_lookup) = self.pending_lookups.get_mut(&target) {
                        let responder = NodeInfo {
                            ip_address: src_addr.ip(),
                            udp_port: src_addr.port(),
                            node_id,
                        };
                        let lookup_effects =
                            pending_lookup.lookup.apply_nodes_reply(responder, nodes);
                        effects.extend(lookup_effects);
                        debug!(in_flight=?pending_lookup.lookup.in_flight, "In Flight");
                        if pending_lookup.lookup.is_finished() {
                            info!(?target, "Lookup completed with nodes");
                            if let Some(value) = pending_lookup.put_value.as_ref() {
                                let nodes_to_store = pending_lookup.lookup.short_list.clone();
                                info!(?nodes_to_store, "Nodes to store.");
                                for n in nodes_to_store.iter().cloned() {
                                    let store = Message::Store {
                                        node_id: self.node.my_info.node_id,
                                        key: target,
                                        value: value.clone(),
                                        ttl_secs: DEFAULT_STORE_TTL_SECS,
                                        is_client: i_am_client,
                                    };
                                    let bytes = rmp_serde::to_vec(&store)?;
                                    effects.push(Effect::Send {
                                        addr: SocketAddr::new(n.ip_address, n.udp_port),
                                        bytes,
                                    });
                                }
                                if !i_am_client {
                                    let my_dist = self.node.my_info.node_id.distance(&target);
                                    let max_peer_dist = nodes_to_store
                                        .iter()
                                        .map(|n| n.node_id.distance(&target))
                                        .max();
                                    if max_peer_dist.map_or(true, |d| my_dist <= d) {
                                        self.node.store(target, value.clone());
                                    }
                                }
                            }
                            remove_lookup = true;
                        }
                    }
                } else {
                    // we got a nodes message with no corresponding lookup... curious.
                }
                if remove_lookup {
                    // remove the lookup from pending lookups, and send final results back to the user dht
                    if let Some(mut finished) = self.pending_lookups.remove(&target) {
                        if let Some(done) = finished.tx_done.take() {
                            let _ = done.send(());
                        }
                        // Signal completion to any value-lookup waiters (None when no value found)
                        if let Some(tx) = finished.tx_value.take() {
                            let _ = tx.send(None);
                        }
                    }
                    // If this was the bootstrap self-lookup (FindNode(self)), refresh all non-self buckets.
                    // This is the second part of the process of entering the network as described in the paper.
                    if target == self.node.my_info.node_id {
                        let refresh_targets = self
                            .node
                            .routing_table
                            .non_self_bucket_targets(self.node.my_info.node_id);
                        for t in refresh_targets.into_iter() {
                            let initial = self.node.routing_table.k_closest(t);
                            let mut effs = self.init_lookup(
                                t,
                                LookupKind::Node,
                                None,
                                initial,
                                None,
                                None,
                                /*early_complete_on_empty=*/ true,
                            );
                            effects.append(&mut effs);
                        }
                    }
                }
                (node_id, is_client)
            }

            Message::FindValue { node_id, key, rpc_id, is_client } => {
                debug!(?key, ?rpc_id, "FindValue request");
                // Lookup the value, or return closest nodes if not found
                if let Some(value) = self.node.get(&key) {
                    debug!(value_len=%value.len(), "We had the requested value.");
                    let found = Message::ValueFound {
                        node_id: self.node.my_info.node_id,
                        key,
                        rpc_id,
                        value: value.clone(),
                        is_client: self.is_client(),
                    };
                    let bytes = rmp_serde::to_vec(&found)?;
                    effects.push(Effect::Send {
                        addr: src_addr,
                        bytes,
                    });
                } else {
                    // we don't hold the value itself, so we need to check for nodes closer to to the key
                    debug!("We did not have the value. Return the k closest nodes we know about");
                    let closest = self.node.routing_table.k_closest(key);
                    let nodes = Message::Nodes {
                        node_id: self.node.my_info.node_id,
                        target: key,
                        rpc_id,
                        nodes: closest,
                        is_client: self.is_client(),
                    };
                    let bytes = rmp_serde::to_vec(&nodes)?;
                    effects.push(Effect::Send {
                        addr: src_addr,
                        bytes,
                    });
                }
                (node_id, is_client)
            }
            Message::ValueFound { node_id, key, rpc_id, value, is_client } => {
                debug!(key=?key, ?rpc_id, value_len=%value.len(), "Received ValueFound");
                // Validate via Lookup API before mutating state
		if let Some(pl) = self.pending_lookups.get(&key) {
		    if !pl.lookup.validate_reply(node_id, rpc_id) {
			// An invalid rpc id indicates possible spoofing.
			warn!(
                            event = "rpc_invalid_value",
                            ?key,
                            responder = ?node_id,
                            ?rpc_id,
                            "Dropping ValueFound due to mismatched rpc_id"
			);
			return Ok(effects);
		    }
		} else {
		    // A missing pending_lookup could just mean that we already got the value from another node
		    debug!(
                        event = "missing_pending_lookup",
                        ?key,
                        responder = ?node_id,
                        ?rpc_id,
                        "ValueFound message does not correspond to an existing pending lookup. We likely already received the value "
		    );
		    return Ok(effects);
		}

                debug!(event="rpc_ok_value", ?key, responder=?node_id, ?rpc_id, "Accepted ValueFound reply");
                if let Some(mut pending_lookup) = self.pending_lookups.remove(&key) {
                    // we drop the lookup entirely once we get back the value
                    info!(?key, ?node_id, "Lookup completed with value");

                    // On successful value lookup, cache the value at the closest non-holder (if any).
		    // This helps currently important values return sooner next time someone looks it up.
                    if let LookupKind::Value = pending_lookup.lookup.kind {
                        if let Some(best) = pending_lookup.lookup.best_non_holder() {
                            let ttl_secs = self.cache_ttl_secs_for(key, &best);
                            let lz = best.node_id.distance(&key).leading_zeros();
                            debug!(
                                event = "cache_schedule",
                                ?key,
                                target_node = ?best.node_id,
                                lz,
                                ttl_secs,
                                addr = %SocketAddr::new(best.ip_address, best.udp_port),
                                "Scheduling cache Store to closest non-holder"
                            );
                            let store = Message::Store {
                                node_id: self.node.my_info.node_id,
                                key,
                                value: value.clone(),
                                ttl_secs,
                                is_client: self.is_client(),
                            };
                            if let Ok(bytes) = rmp_serde::to_vec(&store) {
                                effects.push(Effect::Send {
                                    addr: SocketAddr::new(best.ip_address, best.udp_port),
                                    bytes,
                                });
                            }
                        } else {
                            debug!(
                                event = "cache_skip",
                                ?key,
                                reason = "no_non_holders",
                                "No non-holder responders recorded; skipping cache"
                            );
                        }
                    }

                    // Send the found value back to the user
                    if let Some(tx) = pending_lookup.tx_value.take() {
                        let _ = tx.send(Some(value.clone()));
                    }
                }
                // Optionally Cache the value in our own local storage
                if !self.is_client() {
                    self.node.store(key, value);
                }
                (node_id, is_client)
            }
        };

        // now we add the peer to our routing_table
	// Note: any unexpected/mismatched rpc_id returns early from this method,
	// so we won't be adding them as a contact to our routing table. They are suspect.
        if !peer_is_client {
            if let Some(eff) = self.observe_contact(src_addr, node_id) {
                effects.push(eff);
            }
        }

        Ok(effects)
    }

    async fn apply_effect(&mut self, effect: Effect) {
        match effect {
            Effect::Send { addr, bytes } => {
                if let Err(e) = self.socket.send_to(&bytes, addr).await {
                    error!(%addr, error=%e.to_string(), "Failed to send");
                }
            }
            Effect::StartProbe {
                lru,
                candidates,
                rpc_id,
                bytes,
            } => {
                let addr = SocketAddr::new(lru.ip_address, lru.udp_port);
                if let Err(e) = self.socket.send_to(&bytes, addr).await {
                    error!(%addr, error=%e.to_string(), "Failed to send probe");
                } else {
                    // Record the probe so we can resolve it later
                    let deadline = Instant::now() + PROBE_TIMEOUT;
                    self.pending_probes.insert(
                        rpc_id,
                        PendingProbe {
                            lru,
                            candidates,
                            deadline,
                        },
                    );
                    self.pending_probe_by_lru.insert(lru.node_id, rpc_id);
                    debug!(
                        event = "probe_start",
                        lru = ?lru.node_id,
                        rpc_id = ?rpc_id,
                        addr = %addr,
                        "Started LRU probe"
                    );
                }
            }
        }
    }

    /// check for and resolve expired probes, and check for expired lookups.
    /// Returns any new Effects (probe restarts, lookup top-ups).
    fn sweep_timeouts_and_topup(
        &mut self,
        now: Instant,
        now_std: std::time::Instant,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        // Expired probes: evict LRU and try to insert queued candidates; if still full, start a new probe.
        let mut expired_probes = Vec::new();
        for (rpc_id, pending_probe) in self.pending_probes.iter() {
            if pending_probe.deadline <= now {
                expired_probes.push((*rpc_id, pending_probe.clone()));
            }
        }
        for (rpc_id, mut pending) in expired_probes {
            self.pending_probes.remove(&rpc_id);
            self.pending_probe_by_lru.remove(&pending.lru.node_id);
            let _ = self
                .node
                .routing_table
                .resolve_probe(pending.lru, /*alive=*/ false);

            // we add as many candidates as possible (likely just one?) then satrt a new probe.
            // the new probe takes the other remaining candidates
            use std::collections::VecDeque;
            let mut queue: VecDeque<NodeInfo> = pending.candidates.drain(..).collect();
            while let Some(candidate) = queue.front().cloned() {
                match self.node.routing_table.try_insert(candidate) {
                    InsertResult::SplitOccurred => {
                        // Retry same candidate after split
                        continue;
                    }
                    InsertResult::Full { lru } => {
                        // Still full: start a new probe for this (new) LRU and carry remaining candidates.
                        let rpc_id = RpcId::new_random();
                        let ping = Message::Ping {
                            node_id: self.node.my_info.node_id,
                            rpc_id,
                            is_client: self.is_client(),
                        };
                        if let Ok(bytes) = rmp_serde::to_vec(&ping) {
                            let remaining: Vec<NodeInfo> = queue.clone().into_iter().collect();
                            effects.push(Effect::StartProbe {
                                lru,
                                candidates: remaining,
                                rpc_id,
                                bytes,
                            });
                            debug!(
                                event = "probe_restart",
                                new_lru = ?lru.node_id,
                                rpc_id = ?rpc_id,
                                remaining = %queue.len(),
                                "Starting follow-up probe due to full bucket"
                            );
                        }
                        break;
                    }
                    _ => {
                        // Inserted or Updated; pop and try next
                        queue.pop_front();
                    }
                }
            }
        }

        // Lookups: clear expired and top up concurrency
        for (_key, pending_lookup) in self.pending_lookups.iter_mut() {
            pending_lookup.lookup.sweep_expired(now);
            let current_effects = pending_lookup.lookup.top_up_alpha_requests();
            effects.extend(current_effects);
        }

        // Bucket refreshes: periodically refresh stale buckets by initiating a node lookup
        // towards a random ID within each stale bucket's keyspace.
        // Limit the number of refreshes per sweep to avoid bursts.
        let refresh_limit_per_tick = 1usize;
        let ttl = std::time::Duration::from_secs(60 * 60); // 1 hour per Kademlia recommendation
        let targets =
            self.node
                .routing_table
                .stale_bucket_targets(now_std, ttl, refresh_limit_per_tick);
        for target in targets.into_iter() {
            // Pre-mark the bucket to avoid immediate rescheduling on next tick
            self.node
                .routing_table
                .mark_bucket_refreshed(target, now_std);
            let initial = self.node.routing_table.k_closest(target);
            let mut effs = self.init_lookup(
                target,
                LookupKind::Node,
                None,
                initial,
                None,
                None,
                /*early_complete_on_empty=*/ true,
            );
            effects.append(&mut effs);
        }
        effects
    }

    /// Listen for messages, user commands, and timeouts in an infinite loop, and respond accordingly.
    /// This can be considered the "main" method of the running Kademlia node.
    pub async fn run(mut self) {
        let mut buf = [0u8; 1024];

        let mut ticker = interval(Duration::from_millis(500)); // how often do we clean expired probes and lookups
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut last_storage_purge = Instant::now();

        loop {
            tokio::select! {
                // message receive arm
                result = self.socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, src_addr)) => {
                            trace!(bytes=len, %src_addr, "UDP recv");
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
                                    error!(error=%e.to_string(), "Error decoding message");
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            error!(error=%e.to_string(), "Error receiving message");
                            continue;
                        }
                    }
                },

                // See if the user has given us any commands
                maybe_command = async {
                    match self.rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending::<Option<Command>>().await, // effectively disable this select arm
                    }
                } => {
                    match maybe_command {
                        Some(command) => {
                            let effects = self.handle_command(command).await;
                            if let Ok(effects) = effects {
                                for eff in effects {
                                    self.apply_effect(eff).await;
                                }
                            }
                        }
                        None => {
                            // Command channel closed; disable commands and continue headless.
                            self.rx = None;
                        }
                    }
                },

                _ = ticker.tick() => {
                    // Scheduled timeouts for lookups and stored data.
                    let now = Instant::now();
                    let lookup_effects = self.sweep_timeouts_and_topup(now, std::time::Instant::now());

                    for eff in lookup_effects {
                        self.apply_effect(eff).await;
                    }

                    // Periodically purge expired local key/value entries (throttled)
                    if now.duration_since(last_storage_purge) >= Duration::from_secs(5) {
                        self.node.purge_expired();
                        last_storage_purge = now;
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests;
