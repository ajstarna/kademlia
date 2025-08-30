
#[derive(Debug)]
struct KBucket {
    k: usize,
    depth: usize, // number of prefix bits fixed to get to this bucket
    prefix: Option<NodeID>, // only top `self.depth` bits are meaningful
    node_infos: Vec<NodeInfo>, // TODO: LRU
}

impl KBucket {
    pub fn new(k: usize, depth: usize, prefix: Option<NodeID>) -> Self {
        Self {
            k,
	    depth,
	    prefix,
            node_infos: Vec::with_capacity(k),
        }
    }

    pub fn is_full(&self) -> bool {
        self.node_infos.len() >= self.k
    }

    pub fn insert(&mut self, info: NodeInfo){
        if self.node_infos.len() >= self.k {
	    // we should not even be calling this
            unreachable!("Cannot insert into a full K bucket!");
        }
        self.node_infos.push(info);
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

	id.prefix_bits(self.depth) == self.prefix.expect("prefix must exist when depth > 0").prefix_bits(self.depth)
    }


    /// Returns a mutable reference to the entry with this NodeID, if present.
    pub fn find_mut(&mut self, id: NodeID) -> Option<&mut NodeInfo> {
        self.node_infos.iter_mut().find(|n| n.node_id == id)
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
    let bit_index = bucket.depth;       // split on the next bit
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
    let mut one_bucket  = KBucket::new(k, new_depth, one_prefix);

    // Redistribute nodes
    for node in bucket.node_infos {
        if node.node_id.get_bit_at(bit_index) == 0 {
            zero_bucket.node_infos.push(node);
        } else {
            one_bucket.node_infos.push(node);
        }
    }

    (
        Box::new(BucketTree::Bucket(zero_bucket)),
        Box::new(BucketTree::Bucket(one_bucket)),
        bit_index,
    )
}


/// The ID corresponding to an attempted insert on a full K bucket.
/// A probe is sent out to the LRU, and if they do not respond in time,
/// then we boot them from the bucket and finish the new insertion.
/// The ID tells us which initial attempted insert is ready to resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProbeID(u64);

impl ProbeID {
    pub fn new_random() -> Self {
        let val: u64 = rand::rng().random();
        Self(val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InsertResult{
    Inserted,  // normal insertion
    AlreadyPresent, // exact nodeinfo already exists
    Updated, // The node_id existed but with different contact info
    NeedsProbe{lru: NodeInfo, probe_id: ProbeID},
    SplitOccurred
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
    fn all_nodes(& self) -> Vec<& NodeInfo> {
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
                    let old_bucket = std::mem::replace(current, BucketTree::Bucket(KBucket::dummy()));
                    if let BucketTree::Bucket(bucket) = old_bucket {
			let (new_tree, result) = handle_bucket(bucket, peer, self.my_id, self.k);
			*current = new_tree;
			return result;
                    } else {
			unreachable!("old_bucket must be a KBucket"),
                    }
		}
		BucketTree::Branch { bit_index, zero, one } => {
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
    fn handle_bucket(&self, mut bucket: KBucket, peer: NodeInfo)
		     -> (BucketTree, InsertResult)
    {
	// Case 1: node already exists
	if let Some(existing) = bucket.find_mut(peer.node_id) {
            if *existing == peer {
		return (BucketTree::Bucket(bucket), InsertResult::AlreadyPresent);
            } else {
		existing.ip_address = peer.ip_address;
		existing.udp_port = peer.udp_port;
		return (BucketTree::Bucket(bucket), InsertResult::Updated);
            }
	}

	// Case 2: full bucket
	if bucket.is_full() {
            if bucket.covers(self.my_id) {
		let (zero, one, bit_index) = split_bucket(bucket, k);
		return (
                    BucketTree::Branch { bit_index, zero, one },
                    InsertResult::SplitOccurred,
		);
            } else {
		let lru = bucket.node_infos.last().cloned().unwrap();
		let probe_id = ProbeID::new_random();
		return (BucketTree::Bucket(bucket), InsertResult::NeedsProbe { lru, probe_id });
            }
	}

	// Case 3: not full → insert
	bucket.insert(peer);
	(BucketTree::Bucket(bucket), InsertResult::Inserted)
    }
	/*
        loop {
            match current {
                BucketTree::Bucket(_) => {
                    println!("bucket");
		    // Take ownership of the bucket, leaving a dummy in its place
		    // Needed in the case where we split a bucket.
		    // If no split is required, we will simply put the old bucket back where it was.
		    let old_bucket = std::mem::replace(
			current,
			BucketTree::Bucket(KBucket::new(self.k, 0, None)) // dummy bucket
		    );
		    match old_bucket {
			BucketTree::Bucket(mut bucket) => {

			    if let Some(existing) = bucket.find_mut(peer.node_id) {
				if *existing == peer {
				    println!("exists");
				    break InsertResult::AlreadyPresent;
				} else {
				    println!("update");
				    existing.ip_address = peer.ip_address;
				    existing.udp_port = peer.udp_port;
				    break InsertResult::Updated;
				}
			    }
			    if bucket.is_full() {
				if bucket.covers(self.my_id) {
				    // if my id covered bucket, then split
				    println!("we need to split this bucket that contains our own node id");

				    let bit_index = bucket.depth;
				    let new_depth = bit_index + 1;

				    let zero_prefix = if let Some(prefix) = bucket.prefix {
					Some(prefix.with_bit(new_depth - 1, 0))
				    } else {
					// root bucket split → build prefix from scratch
					Some(NodeID::zero().with_bit(0, 0))
				    };

				    let one_prefix = if let Some(prefix) = bucket.prefix {
					Some(prefix.with_bit(new_depth - 1, 1))
				    } else {
					Some(NodeID::zero().with_bit(0, 1))
				    };


				    let mut zero_bucket = KBucket::new(self.k, new_depth, zero_prefix);
				    let mut one_bucket = KBucket::new(self.k, new_depth, one_prefix);
				    for node in bucket.node_infos {
					if node.node_id.get_bit_at(bit_index) == 0 {
					    zero_bucket.node_infos.push(node);
					} else {
					    one_bucket.node_infos.push(node);
					}
				    }

				    // now replace the bucket in the current tree with the new branch
				    *current = BucketTree::Branch {
					bit_index,
					zero: Box::new(BucketTree::Bucket(zero_bucket)),
					one:  Box::new(BucketTree::Bucket(one_bucket)),
				    };
				    break InsertResult::SplitOccurred
				} else {
				    // Bucket full, but doesn't cover our ID → probe LRU
				    println!("We need to probe the LRU node in this full bucket to possibly replace");
				    let lru = *bucket.node_infos.last().expect("We know the bucket is full");
				    let probe_id = ProbeID::new_random();
				    break InsertResult::NeedsProbe{lru, probe_id}
				}
			    }

			    // Case 3: not full → insert
			    println!("not full, simply insert");
			    bucket.insert(peer);
			    break InsertResult::Inserted;
			}
			_ => unreachable!("old_bucket must be a KBucket"),
		    };
                }
                BucketTree::Branch {
                    bit_index,
                    ref mut one,
                    ref mut zero,
                } => {
                    println!("branch");
                    let bit_val = peer.node_id.get_bit_at(*bit_index);
                    match bit_val {
                        0 => current = zero,
                        1 => current = one,
                        _ => panic!("Programmer error: bit value is neither 0 nor 1!"),
                    }
                }
            }
        }
    }
	 */

    /// a probe either came back alive, or it timed out
    /// If alive, we keep the lru entry in the bucket and update its status.
    /// If dead, then we remove it from the bucket and complete the original insertion
    pub fn resolve_probe(&mut self, probe_id: ProbeID, alive: bool) {
    }

}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::Ipv4Addr;
    use ethereum_types::H160;

    /// Make a NodeID with a specific leading byte and the rest zero.
    fn id_with_first_byte(b: u8) -> NodeID {
        let mut id = [0u8; 20];
        id[0] = b;
        NodeID(H160::from(id))
    }

    /// Helper function for making a test node
    fn make_node(last_octet: u8, port: u16, first_byte: u8) -> NodeInfo {
        NodeInfo {
            ip_address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, last_octet)),
            udp_port: port,
            node_id: id_with_first_byte(first_byte),
        }
    }

    /// Helper function that returns a table with a single full bucket.
    fn setup_full_bucket(k: usize) -> RoutingTable {
	let my_id: NodeID = id_with_first_byte(0xAA);
	let mut rt: RoutingTable = RoutingTable::new(my_id, k);

	// Insert <= k peers should succeed
	rt.try_insert(make_node(1, 4001, 0x01));
	rt.try_insert(make_node(2, 4002, 0x02));
	rt.try_insert(make_node(3, 4003, 0x03));
	rt
    }

    #[test]
    fn create_routing_table_and_insert_up_to_k() {
        let my_id = id_with_first_byte(0xAA);
        let k = 3;
        let mut rt = RoutingTable::new(my_id, k);

        // Insert <= k peers should succeed
	assert!(matches!(rt.try_insert(make_node(1, 4001, 0x01)), InsertResult::Inserted));
	rt.try_insert(make_node(2, 4002, 0x02));
        rt.try_insert(make_node(3, 4003, 0x03));

	// attempt to insert the same node info again; it should be a NOOP
	assert!(matches!(rt.try_insert(make_node(1, 4001, 0x01)), InsertResult::AlreadyPresent));

        let all = rt.all_nodes();
        assert_eq!(all.len(), 3, "should hold exactly k nodes");

        assert_eq!(rt.count_buckets(), 1);
    }

    #[test]
    fn full_bucket_splits() {
	let k = 3;
	let mut rt = setup_full_bucket(k);
        assert_eq!(rt.all_nodes().len(), 3, "should hold exactly k nodes before any split");

        assert_eq!(rt.count_buckets(), 1);
	let insert_result = rt.try_insert(make_node(4, 4004, 0x04));

	assert!(matches!(insert_result, InsertResult::SplitOccurred));
        assert_eq!(rt.all_nodes().len(), 4);
        assert_eq!(rt.count_buckets(), 2);

    }

    #[test]
    fn full_bucket_requires_probe() {
        let my_id = id_with_first_byte(0xAA); // MSB = 1
        let k = 2;
        let mut rt = RoutingTable::new(my_id, k);

	// Fill the root bucket (both are MSB=1 => near side)
        rt.try_insert(make_node(1, 4001, 0x80));
        rt.try_insert(make_node(2, 4002, 0x81));
        assert_eq!(rt.all_nodes().len(), 2);

	// Next insert (MSB=0) should trigger a split (root contains our prefix)
	let expected_lru = make_node(3, 4003, 0x00);
        rt.try_insert(expected_lru);

        // After split, 3 nodes and 2 buckets
        assert_eq!(rt.all_nodes().len(), 3);
        assert_eq!(rt.count_buckets(), 2);


	// Currently far bucket has one node (0x00). Add another MSB=0 to fill it to k=2.
	rt.try_insert(make_node(4, 4004, 0x02));

	//NeedsProbe{lru: NodeInfo, probe_id: ProbeID},

	// Now insert yet another MSB=0 node; the far bucket is full and does NOT contain my_id,
	// so this should not split. We expect a probe of the LRU instead.
	let insert_result = rt.try_insert(make_node(5, 4005, 0x03));

	match insert_result {
	    // the lru should be the node we inserted first into this bucket
	    // we ignore the random probe ID
            InsertResult::NeedsProbe { lru, .. } => {
		assert_eq!(lru, expected_lru, "LRU in far bucket should be the earliest far insert");
            }
            other => panic!("expected NeedsProbe, got: {:?}", other),
	}

	// Still only 2 buckets; no extra split happened.
	assert_eq!(rt.count_buckets(), 2);
    }
}
