use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};

use crate::core::identifier::{Key, NodeID, NodeInfo};
use crate::core::storage::Value;

// Timeouts specific to lookup requests
pub(super) const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub(super) enum LookupKind {
    Node,  // FIND_NODE
    Value, // FIND_VALUE
}

#[derive(Debug, Clone)]
pub(super) enum LookupResult {
    ValueFound(Value),
    NodeFound(NodeInfo),
    Closest(Vec<NodeInfo>),
}

pub(super) struct Lookup {
    pub(super) k: usize,
    pub(super) alpha: usize,
    pub(super) my_node_id: NodeID,
    // keep the target as a NodeID, even though sometimes it is a key
    pub(super) target: NodeID,
    pub(super) kind: LookupKind,
    pub(super) rx: oneshot::Sender<Option<Value>>, // result channel for library user
    pub(super) short_list: Vec<NodeInfo>,
    pub(super) already_queried: HashSet<NodeID>,
    pub(super) in_flight: HashMap<NodeID, Instant>,
}

impl Lookup {
    pub(super) fn new(
        k: usize,
        alpha: usize,
        my_node_id: NodeID,
        target: NodeID,
        kind: LookupKind,
        rx: oneshot::Sender<Option<Value>>,
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
            rx,
            short_list,
            already_queried: HashSet::new(),
            in_flight: HashMap::new(),
        }
    }

    /// If there are fewer than `alpha` queries in flight, return Effects to top up.
    pub(super) fn top_up_alpha_requests(&mut self) -> Vec<super::Effect> {
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
                LookupKind::Node => super::Message::FindNode {
                    node_id: self.my_node_id,
                    target: self.target,
                },
                LookupKind::Value => super::Message::FindValue {
                    node_id: self.my_node_id,
                    key: self.target,
                },
            };
            let bytes = rmp_serde::to_vec(&query).expect("serialize FindNode/FindValue");
            effects.push(super::Effect::Send {
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

    /// Merge new Nodes responses into the shortlist and keep top-k by distance.
    pub(super) fn merge_new_nodes(&mut self, nodes: Vec<NodeInfo>) {
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

    /// Remove expired in-flight queries.
    pub(super) fn sweep_expired(&mut self, now: Instant) {
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

    /// Returns the final results if applicable, or None if still pending.
    pub(super) fn possible_final_result(&self) -> Option<LookupResult> {
        if let LookupKind::Node = self.kind {
            // exact target node found
            if let Some(target_node) = self.short_list.iter().find(|x| x.node_id == self.target) {
                return Some(LookupResult::NodeFound(*target_node));
            }
        }

        if self.in_flight.is_empty() {
            // nothing left to wait on, return current shortlist
            return Some(LookupResult::Closest(self.short_list.clone()));
        }
        None
    }

    pub(super) fn is_finished(&self) -> bool {
        self.possible_final_result().is_some()
    }
}

pub(super) struct PendingLookup {
    pub(super) lookup: Lookup,
    pub(super) deadline: Instant,
    pub(super) put_value: Option<Value>,
    pub(super) put_rx: Option<oneshot::Sender<bool>>,
}
