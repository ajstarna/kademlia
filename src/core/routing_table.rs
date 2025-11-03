use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::identifier::{NodeID, NodeInfo};

#[derive(Debug)]
struct KBucket {
    k: usize,
    depth: usize,                   // number of prefix bits fixed to get to this bucket
    prefix: Option<NodeID>,         // only top `self.depth` bits are meaningful
    node_infos: VecDeque<NodeInfo>, // LRU peer is at the back of the vecdeque
    last_refreshed: Instant,        // when this bucket last saw activity
}

impl KBucket {
    pub fn new(k: usize, depth: usize, prefix: Option<NodeID>) -> Self {
        Self {
            k,
            depth,
            prefix,
            node_infos: VecDeque::with_capacity(k),
            last_refreshed: Instant::now(),
        }
    }

    /// Make a dummy bucket. Useful for rust ownership purposes when traversing
    /// and modifying the routing table.
    pub fn dummy() -> Self {
        Self {
            k: 42,
            depth: 9,
            prefix: None,
            node_infos: VecDeque::new(),
            last_refreshed: Instant::now(),
        }
    }

    pub fn is_full(&self) -> bool {
        self.node_infos.len() >= self.k
    }

    pub fn try_insert(&mut self, info: NodeInfo) {
        if self.node_infos.len() >= self.k {
            // we should not even be calling this
            unreachable!("Cannot insert into a full K bucket!");
        }
        self.node_infos.push_front(info);
    }

    /// Insert or update a peer
    /// If it exists, we update its info.
    /// If the bucket is full, we signal for a probe on the LRU peer, else we add to the front as the new MRU
    pub fn upsert(&mut self, peer: NodeInfo) -> InsertResult {
        // Any interaction with this bucket counts as activity
        self.last_refreshed = Instant::now();
        if let Some(pos) = self
            .node_infos
            .iter()
            .position(|n| n.node_id == peer.node_id)
        {
            // Remove the existing node
            let mut node = self.node_infos.remove(pos).unwrap();

            if node == peer {
                // No field changes, just move to MRU
                self.node_infos.push_front(node);
                InsertResult::AlreadyPresent
            } else {
                // Update contact info + move to MRU
                node.ip_address = peer.ip_address;
                node.udp_port = peer.udp_port;
                self.node_infos.push_front(node);
                InsertResult::Updated
            }
        } else if self.is_full() {
            // Return eviction candidate
            let lru = self.node_infos.back().unwrap().clone();
            InsertResult::Full { lru }
        } else {
            // Insert as MRU
            self.node_infos.push_front(peer);
            InsertResult::Inserted
        }
    }

    // Remove the peer with the given node_id if it exists
    // Return a bool to say if the node indeed existed and was removed
    pub fn remove_peer(&mut self, node_id: NodeID) -> bool {
        // Removal also counts as activity
        self.last_refreshed = Instant::now();
        if let Some(pos) = self.node_infos.iter().position(|n| n.node_id == node_id) {
            self.node_infos.remove(pos);
            true
        } else {
            false
        }
    }

    /// Returns true if the given `id` falls within the *range of IDs* this bucket covers.
    ///
    /// Each bucket is defined by `(prefix, depth)`:
    ///   - `depth` = how many leading bits of the ID are fixed to reach this bucket
    ///   - `prefix` = those leading bits (with the rest ignored)
    ///
    /// An `id` is considered "covered" if its first `depth` bits match `prefix`.
    /// This does **not** mean the `id` is one of the peers actually stored in
    /// `node_infos`; it only checks if the ID lies in the numeric range represented
    /// by this bucket.
    fn covers(&self, id: NodeID) -> bool {
        if self.depth == 0 {
            return true; // root bucket covers whole space
        }

        id.prefix_bits(self.depth)
            == self
                .prefix
                .expect("prefix must exist when depth > 0")
                .prefix_bits(self.depth)
    }

    /// Returns a mutable reference to the entry with this NodeID, if present.
    pub fn find_mut(&mut self, id: NodeID) -> Option<&mut NodeInfo> {
        self.node_infos.iter_mut().find(|n| n.node_id == id)
    }

    pub fn find(&self, id: NodeID) -> Option<&NodeInfo> {
        self.node_infos.iter().find(|n| n.node_id == id)
    }
}

/// A binary tree whose leaves are K-buckets.
/// Each k-bucket contains nodes with some common prefix
/// of their ids.
#[derive(Debug)]
enum BucketTree {
    Bucket(KBucket),
    Branch {
        bit_index: usize, // which bit this branch splits on
        one: Box<BucketTree>,
        zero: Box<BucketTree>,
    },
}

/// Split a full `KBucket` into two child buckets at the next bit.
///
/// - Uses `bit_index = depth` as the split point.
/// - Creates two buckets at `depth + 1` with prefixes ending in `0` and `1`.
/// - Redistributes all nodes from the original into the correct child.
///
/// Returns `(zero_child, one_child, bit_index)`.
///
/// Note: the peer that triggered the split must be inserted afterwards.
fn split_bucket(bucket: KBucket, k: usize) -> (Box<BucketTree>, Box<BucketTree>, usize) {
    let bit_index = bucket.depth; // split on the next bit
    let new_depth = bit_index + 1;

    // Build child prefixes
    let zero_prefix = if let Some(prefix) = bucket.prefix {
        Some(prefix.with_bit(new_depth - 1, 0))
    } else {
        Some(NodeID::zero().with_bit(0, 0)) // root bucket special case
    };

    let one_prefix = if let Some(prefix) = bucket.prefix {
        Some(prefix.with_bit(new_depth - 1, 1))
    } else {
        Some(NodeID::zero().with_bit(0, 1))
    };

    // Create the two new buckets
    let mut zero_bucket = KBucket::new(k, new_depth, zero_prefix);
    let mut one_bucket = KBucket::new(k, new_depth, one_prefix);

    // Redistribute nodes
    for node in bucket.node_infos.into_iter().rev() {
        if node.node_id.get_bit_at(bit_index) == 0 {
            zero_bucket.upsert(node);
        } else {
            one_bucket.upsert(node);
        }
    }

    (
        Box::new(BucketTree::Bucket(zero_bucket)),
        Box::new(BucketTree::Bucket(one_bucket)),
        bit_index,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InsertResult {
    Inserted,               // normal insertion
    AlreadyPresent,         // exact nodeinfo already exists
    Updated,                // The node_id existed but with different contact info
    Full { lru: NodeInfo }, // a bucket can say it is full and give the lru in case a probe is needed
    SplitOccurred,
}

#[derive(Debug)]
pub struct RoutingTable {
    my_id: NodeID,
    k: usize,
    tree: BucketTree,
}

impl RoutingTable {
    pub fn new(my_id: NodeID, k: usize) -> Self {
        // the root bucket has 0 depth and no prefix
        Self {
            my_id,
            k,
            tree: BucketTree::Bucket(KBucket::new(k, 0, None)),
        }
    }

    /// Collect every NodeInfo in the tree. Handy for tests and simple impls.
    fn all_nodes(&self) -> Vec<NodeInfo> {
        fn walk(t: &BucketTree, out: &mut Vec<NodeInfo>) {
            match t {
                BucketTree::Bucket(b) => out.extend(b.node_infos.iter().cloned()),
                BucketTree::Branch { one, zero, .. } => {
                    walk(one, out);
                    walk(zero, out);
                }
            }
        }
        let mut v = Vec::new();
        walk(&self.tree, &mut v);
        v
    }

    /// Return a list of random target IDs for buckets that appear stale.
    /// At most `limit` targets are returned.
    pub fn stale_bucket_targets(&self, now: Instant, ttl: Duration, limit: usize) -> Vec<NodeID> {
        let mut out = Vec::new();
        fn walk(
            t: &BucketTree,
            now: Instant,
            ttl: Duration,
            out: &mut Vec<NodeID>,
            limit: usize,
        ) {
            if out.len() >= limit {
                return;
            }
            match t {
                BucketTree::Bucket(b) => {
                    if now.duration_since(b.last_refreshed) >= ttl {
                        // Build a random ID whose first `depth` bits match this bucket's prefix
                        let target = random_id_with_prefix(b.prefix, b.depth);
                        out.push(target);
                    }
                }
                BucketTree::Branch { zero, one, .. } => {
                    walk(zero, now, ttl, out, limit);
                    if out.len() < limit {
                        walk(one, now, ttl, out, limit);
                    }
                }
            }
        }
        walk(&self.tree, now, ttl, &mut out, limit);
        out
    }

    /// Mark the leaf bucket containing `id` as refreshed at `now`.
    pub fn mark_bucket_refreshed(&mut self, id: NodeID, now: Instant) {
        fn walk(t: &mut BucketTree, id: NodeID, now: Instant) {
            match t {
                BucketTree::Bucket(b) => {
                    b.last_refreshed = now;
                }
                BucketTree::Branch {
                    bit_index,
                    zero,
                    one,
                } => {
                    if id.get_bit_at(*bit_index) == 0 {
                        walk(zero, id, now)
                    } else {
                        walk(one, id, now)
                    }
                }
            }
        }
        walk(&mut self.tree, id, now);
    }

    pub fn contains(&self, node_id: NodeID) -> bool {
        self.find(node_id).is_some()
    }

    /// Return the total number of peers currently stored across all buckets.
    pub fn peer_count(&self) -> usize {
        fn count_nodes(t: &BucketTree) -> usize {
            match t {
                BucketTree::Bucket(b) => b.node_infos.len(),
                BucketTree::Branch { zero, one, .. } => count_nodes(zero) + count_nodes(one),
            }
        }
        count_nodes(&self.tree)
    }

    pub fn find(&self, node_id: NodeID) -> Option<&NodeInfo> {
        fn walk(t: &BucketTree, node_id: NodeID) -> Option<&NodeInfo> {
            match t {
                BucketTree::Bucket(b) => b.find(node_id), // assume KBucket has a find
                BucketTree::Branch { zero, one, .. } => {
                    walk(zero, node_id).or_else(|| walk(one, node_id))
                }
            }
        }
        walk(&self.tree, node_id)
    }

    pub fn find_mut(&mut self, node_id: NodeID) -> Option<&mut NodeInfo> {
        fn walk(t: &mut BucketTree, node_id: NodeID) -> Option<&mut NodeInfo> {
            match t {
                BucketTree::Bucket(b) => b.find_mut(node_id),
                BucketTree::Branch { zero, one, .. } => {
                    if let Some(found) = walk(zero, node_id) {
                        Some(found)
                    } else {
                        walk(one, node_id)
                    }
                }
            }
        }
        walk(&mut self.tree, node_id)
    }

    /// Generate random target IDs for all buckets that do NOT cover `my_id`.
    /// These targets can be used to refresh those buckets per Kademlia join procedure.
    pub fn non_self_bucket_targets(&self, my_id: NodeID) -> Vec<NodeID> {
        fn walk(t: &BucketTree, my_id: NodeID, out: &mut Vec<NodeID>) {
            match t {
                BucketTree::Bucket(b) => {
                    if !b.covers(my_id) {
                        out.push(random_id_with_prefix(b.prefix, b.depth));
                    }
                }
                BucketTree::Branch { zero, one, .. } => {
                    walk(zero, my_id, out);
                    walk(one, my_id, out);
                }
            }
        }
        let mut v = Vec::new();
        walk(&self.tree, my_id, &mut v);
        v
    }

    pub fn remove_peer(&mut self, node_id: NodeID) -> bool {
        fn walk(t: &mut BucketTree, node_id: NodeID) -> bool {
            match t {
                BucketTree::Bucket(b) => b.remove_peer(node_id),
                BucketTree::Branch { zero, one, .. } => {
                    // Try left, else right
                    walk(zero, node_id) || walk(one, node_id)
                }
            }
        }
        walk(&mut self.tree, node_id)
    }

    /// Given a node ID, we find the k closet nodes to it
    /// Note: we do the simply and less efficient strategy of sorting all possible nodes.
    /// TODO: do a more efficient search, keeping a heap of buckets and looking at the closest
    /// buckets until we have K nodes.
    pub fn k_closest(&self, target_id: NodeID) -> Vec<NodeInfo> {
        let mut nodes: Vec<NodeInfo> = self.all_nodes();
        nodes.sort_by_key(|n| n.node_id.distance(&target_id));
        nodes.truncate(self.k);
        nodes
    }

    /// return how many leaf k-buckets are store. maximum of 160, but likely far fewer
    fn count_buckets(&self) -> usize {
        fn walk(t: &BucketTree) -> usize {
            match t {
                BucketTree::Bucket(_) => 1,
                BucketTree::Branch { one, zero, .. } => walk(zero) + walk(one),
            }
        }
        walk(&self.tree)
    }

    pub fn try_insert(&mut self, peer: NodeInfo) -> InsertResult {
        let mut current = &mut self.tree;

        loop {
            match current {
                BucketTree::Bucket(_) => {
                    // We replace the current bucket with a dummy bucket for now.
                    // handle_bucket() returns the new_tree to put back in place.
                    let old_bucket =
                        std::mem::replace(current, BucketTree::Bucket(KBucket::dummy()));
                    if let BucketTree::Bucket(bucket) = old_bucket {
                        let (new_tree, result) =
                            RoutingTable::handle_bucket(bucket, peer, self.my_id, self.k);
                        *current = new_tree;
                        return result;
                    } else {
                        unreachable!("old_bucket must be a KBucket");
                    }
                }
                BucketTree::Branch {
                    bit_index,
                    zero,
                    one,
                } => {
                    current = if peer.node_id.get_bit_at(*bit_index) == 0 {
                        zero
                    } else {
                        one
                    };
                }
            }
        }
    }

    fn handle_bucket(
        mut bucket: KBucket,
        peer: NodeInfo,
        my_id: NodeID,
        k: usize,
    ) -> (BucketTree, InsertResult) {
        let result = bucket.upsert(peer);
        match result {
            InsertResult::Full { lru } => {
                if bucket.covers(my_id) {
                    // split the bucket
                    tracing::debug!("Splitting full bucket that covers self");
                    let (zero, one, bit_index) = split_bucket(bucket, k);
                    let new_tree = BucketTree::Branch {
                        bit_index,
                        zero,
                        one,
                    };
                    (new_tree, InsertResult::SplitOccurred)
                } else {
                    (
                        BucketTree::Bucket(bucket),
                        result, // forward the `Full` result to the protocol for a probe
                    )
                }
            }
            _ => (BucketTree::Bucket(bucket), result),
        }
    }

    pub fn resolve_probe(&mut self, peer: NodeInfo, alive: bool) {
        if alive {
            // treat like a fresh contact → move to MRU
            let _ = self.try_insert(peer);
        } else {
            // drop the peer entirely
            self.remove_peer(peer.node_id);
        }
    }
}

/// Construct a random NodeID whose first `depth` bits match `prefix`.
fn random_id_with_prefix(prefix: Option<NodeID>, depth: usize) -> NodeID {
    if depth == 0 {
        return NodeID::new();
    }
    let p = prefix.expect("prefix must exist when depth > 0");
    let mut id = NodeID::new();
    for i in 0..depth {
        id = id.with_bit(i, p.get_bit_at(i));
    }
    id
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_support::test_support::{id_with_first_byte, make_peer};

    #[test]
    fn create_routing_table_and_insert_up_to_k() {
        let my_id = id_with_first_byte(0xAA);
        println!("{my_id:?}");
        let k = 3;
        let mut rt = RoutingTable::new(my_id, k);
        assert_eq!(rt.all_nodes().len(), 0);

        let _ = rt.try_insert(make_peer(1, 4001, 0x02));
        let _ = rt.try_insert(make_peer(3, 4003, 0x03));
        let _ = rt.try_insert(make_peer(4, 4004, 0x04));
        assert_eq!(rt.all_nodes().len(), 3);
    }

    #[test]
    fn removal_and_lookup() {
        let my_id = id_with_first_byte(0xAA);
        let k = 3;
        let mut rt = RoutingTable::new(my_id, k);
        let p1 = make_peer(1, 4001, 0x02);
        let p2 = make_peer(2, 4002, 0x03);
        rt.try_insert(p1);
        rt.try_insert(p2);
        assert!(rt.contains(p1.node_id));
        assert!(rt.remove_peer(p1.node_id));
        assert!(!rt.contains(p1.node_id));
        assert!(rt.find(p2.node_id).is_some());
    }
}
