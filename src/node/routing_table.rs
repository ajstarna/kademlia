use std::collections::VecDeque;

use super::identifier::{NodeID, NodeInfo};

#[derive(Debug)]
struct KBucket {
    k: usize,
    depth: usize,                   // number of prefix bits fixed to get to this bucket
    prefix: Option<NodeID>,         // only top `self.depth` bits are meaningful
    node_infos: VecDeque<NodeInfo>, // LRU peer is at the back of the vecdeque
}

impl KBucket {
    pub fn new(k: usize, depth: usize, prefix: Option<NodeID>) -> Self {
        Self {
            k,
            depth,
            prefix,
            node_infos: VecDeque::with_capacity(k),
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
    fn all_nodes(&self) -> Vec<&NodeInfo> {
        fn walk<'a>(t: &'a BucketTree, out: &mut Vec<&'a NodeInfo>) {
            match t {
                BucketTree::Bucket(b) => out.extend(b.node_infos.iter()),
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

    pub fn contains(&self, node_id: NodeID) -> bool {
        self.find(node_id).is_some()
    }

    pub fn find(&self, node_id: NodeID) -> Option<&NodeInfo> {
        fn walk<'a>(t: &'a BucketTree, node_id: NodeID) -> Option<&'a NodeInfo> {
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
        fn walk<'a>(t: &'a mut BucketTree, node_id: NodeID) -> Option<&'a mut NodeInfo> {
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
        let mut nodes: Vec<NodeInfo> = self.all_nodes().into_iter().cloned().collect();
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

    /// Once we have found the proper leaf bucket during an attempted insertion,
    /// we enter this method.
    /// It will either insert if there is room, trigger a split if the bucket  covers
    /// our own ID, or  trigger an LRU probe.
    ///
    /// Returns the BucketTree to put back in the overall Routing tree
    ///  - can just be the same bucket, or sometimes it will be a branch to split buckets
    /// and returns the InsertResult of what occured.
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
                    println!("lets split");
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

    /// a probe either came back alive, or it timed out
    /// If alive, we keep the lru entry in the bucket and update its status.
    /// If dead, then we remove it from the bucket and complete the original insertion
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

        // Insert <= k peers should succeed
        assert!(matches!(
            rt.try_insert(make_peer(1, 4001, 0x01)),
            InsertResult::Inserted
        ));
        rt.try_insert(make_peer(2, 4002, 0x02));
        rt.try_insert(make_peer(3, 4003, 0x03));

        // attempt to insert the same node info again; it should be a NOOP
        assert!(matches!(
            rt.try_insert(make_peer(1, 4001, 0x01)),
            InsertResult::AlreadyPresent
        ));

        let all = rt.all_nodes();
        assert_eq!(all.len(), 3, "should hold exactly k nodes");

        assert_eq!(rt.count_buckets(), 1);
    }

    #[test]
    fn full_bucket_splits() {
        let my_id: NodeID = id_with_first_byte(0xAA);
        let mut rt: RoutingTable = RoutingTable::new(my_id, 3);

        // Insert <= k peers should succeed

        // the first two starts with 0
        rt.try_insert(make_peer(1, 4001, 0x01));
        rt.try_insert(make_peer(2, 4002, 0x02));

        // the third peer starts with a 1 bit, so it will end up in a different bucket when we split
        rt.try_insert(make_peer(3, 4003, 0x83));

        assert_eq!(
            rt.all_nodes().len(),
            3,
            "should hold exactly k nodes before any split"
        );

        assert_eq!(rt.count_buckets(), 1);

        // the first time there is a split triggered
        let insert_result = rt.try_insert(make_peer(4, 4004, 0x04));
        assert!(matches!(insert_result, InsertResult::SplitOccurred));
        assert_eq!(rt.all_nodes().len(), 3);

        let nodes = rt.all_nodes();
        println!("{nodes:?}");

        // the second bucket is ready to go
        assert_eq!(rt.count_buckets(), 2);

        // try again, and this time it will be there
        let insert_result = rt.try_insert(make_peer(4, 4004, 0x04));
        println!("{insert_result:?}");
        print!("{rt:?}");
        assert!(matches!(insert_result, InsertResult::Inserted));
        assert_eq!(rt.all_nodes().len(), 4);
    }

    #[test]
    fn full_bucket_requires_probe() {
        let my_id = id_with_first_byte(0xAA); // MSB = 1
        let k = 2;
        let mut rt = RoutingTable::new(my_id, k);

        // Fill the root bucket (both are MSB=1 => near side)
        rt.try_insert(make_peer(1, 4001, 0x80));
        rt.try_insert(make_peer(2, 4002, 0x81));
        assert_eq!(rt.all_nodes().len(), 2);

        // Next try_insert (MSB=0) should trigger a split (root contains our prefix)
        let expected_lru = make_peer(3, 4003, 0x00);
        assert!(matches!(
            rt.try_insert(expected_lru),
            InsertResult::SplitOccurred
        ));

        // After split, still 2 nodes but now 2 buckets
        assert_eq!(rt.all_nodes().len(), 2);
        assert_eq!(rt.count_buckets(), 2);

        // now insert it for real
        assert!(matches!(
            rt.try_insert(expected_lru),
            InsertResult::Inserted
        ));

        assert_eq!(rt.all_nodes().len(), 3);

        // Currently far bucket has one node (0x00). Add another MSB=0 to fill it to k=2.
        rt.try_insert(make_peer(4, 4004, 0x02));

        // Now insert yet another MSB=0 node; the far bucket is full and does NOT contain my_id,
        // so this should not split. We expect a probe of the LRU instead.
        let insert_result = rt.try_insert(make_peer(5, 4005, 0x03));

        match insert_result {
            // the lru should be the node we inserted first into this bucket
            // we ignore the random probe ID
            InsertResult::Full { lru } => {
                assert_eq!(
                    lru, expected_lru,
                    "LRU in far bucket should be the earliest far insert"
                );
            }
            other => panic!("expected NeedsProbe, got: {:?}", other),
        }

        // Still only 2 buckets; no extra split happened.
        assert_eq!(rt.count_buckets(), 2);
    }

    /// Test that k_closest works, even in the case where it needs to get k nodes
    /// from across more than one bucket.
    #[test]
    fn k_closest() {
        let my_id: NodeID = id_with_first_byte(0x80); // MSB = 1
        let k: usize = 2;
        let mut rt: RoutingTable = RoutingTable::new(my_id, k);

        // Helper to absorb SplitOccurred
        let mut insert = |peer: NodeInfo| loop {
            match rt.try_insert(peer) {
                InsertResult::SplitOccurred => continue,
                _ => break,
            }
        };

        // Two near-side peers (MSB=1)
        let near1: NodeInfo = make_peer(1, 4001, 0x81);
        let near2: NodeInfo = make_peer(2, 4002, 0x82);
        insert(near1);
        insert(near2);

        // One far-side peer (MSB=0) → causes split
        let far: NodeInfo = make_peer(3, 4003, 0x01);
        insert(far);

        // Sanity: should have two buckets now
        assert_eq!(rt.count_buckets(), 2);

        // Case 1: Pick a target closer to the two near nodes, i.e. a single bucket
        let target: NodeID = id_with_first_byte(0xA0);

        let closest: Vec<NodeInfo> = rt.k_closest(target);
        assert_eq!(closest.len(), k);

        let ids: Vec<NodeID> = closest.iter().map(|n| n.node_id).collect();
        assert!(ids.contains(&near1.node_id));
        assert!(ids.contains(&near2.node_id));

        // Case 2: Pick a target closer to the far peer than to near nodes
        // We will need results from more than one bucket
        let target: NodeID = id_with_first_byte(0x00);

        let closest: Vec<NodeInfo> = rt.k_closest(target);
        assert_eq!(closest.len(), k);

        let ids: Vec<NodeID> = closest.iter().map(|n| n.node_id).collect();
        assert!(ids.contains(&far.node_id));
        // near1 is closer than near2 to the target
        assert!(ids.contains(&near1.node_id));
    }
}
