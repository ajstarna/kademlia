use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};

use crate::{
    core::identifier::{Key, NodeID, NodeInfo, ProbeID},
    core::routing_table::InsertResult,
    core::storage::Value,
    core::NodeState,
};
use std::collections::HashMap;
//use std::time::Instant;
use tracing::{debug, error, info, warn};

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
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
        probe_id: ProbeID, // a unique id for this specific request
        is_client: bool,
    },
    Pong {
        node_id: NodeID,
        probe_id: ProbeID, // a unique id for this specific request
        is_client: bool,
    },
    Store {
        node_id: NodeID,
        key: Key,
        value: Value,
        is_client: bool,
    },
    FindNode {
        node_id: NodeID,
        target: NodeID,
        is_client: bool,
    },
    Nodes {
        node_id: NodeID,
        target: NodeID, // we need to include the target to map the nodes to the lookup
        nodes: Vec<NodeInfo>,
        is_client: bool,
    },
    FindValue {
        node_id: NodeID,
        key: Key,
        is_client: bool,
    },
    ValueFound {
        node_id: NodeID,
        key: Key,
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
        probe_id: ProbeID,
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
    pending_probes: HashMap<ProbeID, PendingProbe>,
    pending_probe_by_lru: HashMap<NodeID, ProbeID>,
    pending_lookups: HashMap<NodeID, PendingLookup>,
    role: NodeRole,
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

        let pending_probes: HashMap<ProbeID, PendingProbe> = HashMap::new();
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
                    let probe_id = ProbeID::new_random();
                    let ping = Message::Ping {
                        node_id: self.node.my_info.node_id,
                        probe_id,
                        is_client: self.is_client(),
                    };
                    if let Ok(bytes) = rmp_serde::to_vec(&ping) {
                        let addr = SocketAddr::new(lru.ip_address, lru.udp_port);
                        // Best-effort send; errors are logged by apply_effect code paths elsewhere,
                        // but here we send directly for simplicity.
                        let _ = self.socket.send_to(&bytes, addr).await;
                        let deadline = Instant::now() + PROBE_TIMEOUT;
                        self.pending_probes.insert(
                            probe_id,
                            PendingProbe {
                                lru,
                                candidates: vec![peer],
                                deadline,
                            },
                        );
                        self.pending_probe_by_lru.insert(lru.node_id, probe_id);
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
        })
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
                    let probe_id = ProbeID::new_random();
                    let ping = Message::Ping {
                        node_id: self.node.my_info.node_id,
                        probe_id,
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
                            probe_id,
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

                // Send initial FindNode(self) to the seed addresses
                let query = Message::FindNode {
                    node_id: my_id,
                    target: my_id,
                    is_client: self.is_client(),
                };
                let bytes = rmp_serde::to_vec(&query)?;
                for addr in addrs {
                    effs.push(Effect::Send {
                        addr,
                        bytes: bytes.clone(),
                    });
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
                probe_id,
                is_client,
            } => {
                debug!(?node_id, "Received Ping");
                let pong = Message::Pong {
                    node_id: self.node.my_info.node_id,
                    probe_id,
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
                probe_id,
                is_client,
            } => {
                debug!(?node_id, "Received Pong");
                // Maybe mark the node as alive or update routing table
                if let Some(pending) = self.pending_probes.remove(&probe_id) {
                    self.pending_probe_by_lru.remove(&pending.lru.node_id);
                    self.node
                        .routing_table
                        .resolve_probe(pending.lru, /*alive =*/ true);
                } else {
                    // TODO: is there more to think about here?
                    warn!(?node_id, probe_id=?probe_id, "Pong received without matching probe");
                }
                (node_id, is_client)
            }

            Message::Store {
                node_id,
                key,
                value,
                is_client,
            } => {
                debug!(?key, value_len=?value.len(), "Store request");
                if !self.is_client() {
                    self.node.store(key, value);
                }
                (node_id, is_client)
            }

            Message::FindNode {
                node_id,
                target,
                is_client,
            } => {
                debug!(?target, "FindNode request");
                // Find closest nodes to the given ID in your routing table
                let closest = self.node.routing_table.k_closest(target);
                let nodes = Message::Nodes {
                    node_id: self.node.my_info.node_id,
                    target,
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

            Message::Nodes {
                node_id,
                target,
                nodes,
                is_client,
            } => {
                debug!(?target, "Received Nodes");
                // observe all the new nodes we just learned about
                for n in &nodes {
                    if let Some(eff) =
                        self.observe_contact(SocketAddr::new(n.ip_address, n.udp_port), n.node_id)
                    {
                        effects.push(eff);
                    }
                }

                let mut remove_lookup: bool = false; // remove if there are no more in-flight requests
                let i_am_client = self.is_client();
                if let Some(pending_lookup) = self.pending_lookups.get_mut(&target) {
                    pending_lookup.lookup.in_flight.remove(&node_id);
                    pending_lookup.lookup.merge_new_nodes(nodes);

                    let lookup_effects = pending_lookup.lookup.top_up_alpha_requests();
                    effects.extend(lookup_effects);

                    debug!(in_flight=?pending_lookup.lookup.in_flight, "In Flight");

                    if pending_lookup.lookup.is_finished() {
                        info!(?target, "Lookup completed with nodes");
                        // If this lookup was initiated by a Put, we send the Store messages now
                        if let Some(value) = pending_lookup.put_value.as_ref() {
                            let nodes_to_store = pending_lookup.lookup.short_list.clone();
                            info!(?nodes_to_store, "Nodes to store.");
                            for n in nodes_to_store.iter().cloned() {
                                let store = Message::Store {
                                    node_id: self.node.my_info.node_id,
                                    key: target,
                                    value: value.clone(),
                                    is_client: i_am_client,
                                };
                                let bytes = rmp_serde::to_vec(&store)?;
                                effects.push(Effect::Send {
                                    addr: SocketAddr::new(n.ip_address, n.udp_port),
                                    bytes,
                                });
                            }
                            if !i_am_client {
                                // check if we are closer than the furthest node that stored this value.
                                // if so, then we should also store the value (it is ok if k+1 nodes store)
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

            Message::FindValue {
                node_id,
                key,
                is_client,
            } => {
                debug!(?key, "FindValue request");
                // Lookup the value, or return closest nodes if not found
                if let Some(value) = self.node.get(&key) {
                    debug!(?value, "We had the requested value.");
                    let found = Message::ValueFound {
                        node_id: self.node.my_info.node_id,
                        key,
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
            Message::ValueFound {
                node_id,
                key,
                value,
                is_client,
            } => {
                debug!(key=?key, value=?value, "Received ValueFound");
                if let Some(mut pending_lookup) = self.pending_lookups.remove(&key) {
                    // we drop the lookup entirely once we get back the value
                    info!(?key, ?node_id, "Lookup completed with value");

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
                probe_id,
                bytes,
            } => {
                let addr = SocketAddr::new(lru.ip_address, lru.udp_port);
                if let Err(e) = self.socket.send_to(&bytes, addr).await {
                    error!(%addr, error=%e.to_string(), "Failed to send probe");
                } else {
                    // Record the probe so we can resolve it later
                    let deadline = Instant::now() + PROBE_TIMEOUT;
                    self.pending_probes.insert(
                        probe_id,
                        PendingProbe {
                            lru,
                            candidates,
                            deadline,
                        },
                    );
                    self.pending_probe_by_lru.insert(lru.node_id, probe_id);
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
        for (probe_id, pending_probe) in self.pending_probes.iter() {
            if pending_probe.deadline <= now {
                expired_probes.push((*probe_id, pending_probe.clone()));
            }
        }
        for (probe_id, mut pending) in expired_probes {
            self.pending_probes.remove(&probe_id);
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
                        let probe_id = ProbeID::new_random();
                        let ping = Message::Ping {
                            node_id: self.node.my_info.node_id,
                            probe_id,
                            is_client: self.is_client(),
                        };
                        if let Ok(bytes) = rmp_serde::to_vec(&ping) {
                            let remaining: Vec<NodeInfo> = queue.into_iter().collect();
                            effects.push(Effect::StartProbe {
                                lru,
                                candidates: remaining,
                                probe_id,
                                bytes,
                            });
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
                            debug!(bytes=len, %src_addr, "UDP recv");
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

		// Scheduled timeouts for lookups and stored data.
                _ = ticker.tick() => {
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
mod test {
    use super::*;
    use crate::core::routing_table::InsertResult;
    use crate::test_support::test_support::{id_with_first_byte, make_peer};

    #[tokio::test]
    async fn test_receive_ping() {
        let dummy_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap(); // ephemeral port
        let mut pm = ProtocolManager::new_headless(dummy_socket, 20, 3).unwrap();

        let src_id = NodeID::new();
        let probe_id = ProbeID::new_random();
        let msg = Message::Ping {
            node_id: src_id,
            probe_id,
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
                assert!(
                    matches!(reply, Message::Pong { node_id, probe_id: pid, .. } if node_id == pm.node.my_info.node_id && pid == probe_id )
                );
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

        // Helper to absorb possible splits while inserting directly into the routing table
        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };

        // Fill root with two distinct peers in the same non-self bucket (same MSB, different lower bits)
        let pna = make_peer(10, 9001, non_byte | 0x00);
        let pnb = make_peer(11, 9002, non_byte | 0x01);
        insert(pna);
        insert(pnb);
        // This insert should split the root into self/non-self buckets
        let ps = make_peer(12, 9003, self_byte);
        insert(ps);

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

        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };

        // Fill root with two distinct peers in the same non-self bucket (same MSB)
        let pna = make_peer(20, 9101, non_byte | 0x00);
        let pnb = make_peer(21, 9102, non_byte | 0x01);
        insert(pna);
        insert(pnb);
        let ps = make_peer(22, 9103, self_byte);
        insert(ps);

        // Two candidates arrive targeting the full non-self bucket (distinct IDs, same MSB)
        let c1 = make_peer(23, 9104, non_byte | 0x02);
        let c2 = make_peer(24, 9105, non_byte | 0x03);

        // First candidate triggers a probe; capture lru and probe_id
        let eff1 = pm
            .observe_contact(
                std::net::SocketAddr::new(c1.ip_address, c1.udp_port),
                c1.node_id,
            )
            .expect("should start a probe for full bucket");

        let (lru, probe_id, bytes) = match eff1 {
            Effect::StartProbe {
                lru,
                candidates,
                probe_id,
                bytes,
            } => {
                assert_eq!(candidates.len(), 1);
                (lru, probe_id, bytes)
            }
            _ => panic!("expected StartProbe effect"),
        };

        // Apply effect to record the pending probe
        pm.apply_effect(Effect::StartProbe {
            lru,
            candidates: vec![c1],
            probe_id,
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
            probe_id,
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

        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);
        insert(p3);

        // Start the lookup via Command::Get
        let key: Key = NodeID(target.0);
        let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
        let _ = pm
            .handle_command(Command::Get { key, tx_value: tx })
            .await
            .unwrap();

        // Nodes reply from p1 introducing a new peer p4
        let p4 = make_peer(4, 6004, 0x03);
        let nodes_msg = Message::Nodes {
            node_id: p1.node_id,
            target,
            nodes: vec![p4],
            is_client: false,
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
        let dummy_socket: UdpSocket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut pm: ProtocolManager = ProtocolManager::new_headless(dummy_socket, 20, 2).unwrap();

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
        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);

        // Start the lookup for this target via Command::Get
        let key: Key = NodeID(target.0);
        let (tx, _rx) = tokio::sync::oneshot::channel::<Option<Value>>();
        let _ = pm
            .handle_command(Command::Get { key, tx_value: tx })
            .await
            .unwrap();

        // Act: simulate a Nodes reply from p1 that introduces p_new
        let nodes_msg = Message::Nodes {
            node_id: p1.node_id,
            target,
            nodes: vec![p_new],
            is_client: false,
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
            is_client: false,
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
            is_client: false,
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
        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);

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
        assert_eq!(
            initial_sends.len(),
            2,
            "should send alpha FindNode requests"
        );

        // Simulate a Nodes reply from p1 that does not improve the shortlist (duplicates only)
        let nodes_from_p1 = Message::Nodes {
            node_id: p1.node_id,
            target,
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

        let mut insert = |peer: NodeInfo| loop {
            match pm.node.routing_table.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };
        insert(p1);
        insert(p2);

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

        // Expect initial FindNode requests to p1 and p2
        let mut findnode_dests = Vec::new();
        for eff in effects.into_iter() {
            if let Effect::Send { addr, bytes } = eff {
                if let Ok(msg) = rmp_serde::from_slice::<Message>(&bytes) {
                    if matches!(msg, Message::FindNode { .. }) {
                        findnode_dests.push(addr);
                    }
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
}
