use std::net::IpAddr;
use rand::Rng;

pub mod identifier;
use identifier::NodeID;

const NUM_BUCKETS: usize = 160; // needs to match SHA1's output length
const K: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    pub ip_address: IpAddr,
    pub udp_port: u16,
    pub node_id: NodeID,
}


#[derive(Debug)]
struct KBucket {
    k: usize,
    //pub range: (NodeID, NodeID),  // inclusive low, exclusive high
    node_infos: Vec<NodeInfo>, // TODO: LRU
}
impl KBucket {
    pub fn new(k: usize) -> Self {
        Self {
            k,
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

    /*
    pub fn contains_range(&self, id: NodeID) -> bool {
        let (low, high) = self.range;
        id >= low && id < high
    }
    */

    pub fn contains_id(&self, search_id: NodeID) -> bool {
        self.node_infos.iter().any(|info| info.node_id == search_id)
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
    tree: BucketTree,
}

impl RoutingTable {
    pub fn new(my_id: NodeID, k: usize) -> Self {
        Self {
            my_id,
            tree: BucketTree::Bucket(KBucket::new(k)),
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

    pub fn insert(&mut self, peer: NodeInfo) -> InsertResult {
        println!("{peer:?}");
        let mut current = &mut self.tree;
        loop {
            match current {
                BucketTree::Bucket(ref mut bucket) => {
                    println!("bucket");
		    let result: InsertResult = {
			if let Some(existing) = bucket.find_mut(peer.node_id) {
			    // If node_id already exists, optionally update its contact info (ip/port).
			    if *existing == peer {
				InsertResult::AlreadyPresent
			    }
			    else {
				existing.ip_address = peer.ip_address;
				existing.udp_port = peer.udp_port;
				InsertResult::Updated
			    }
			}
			else if bucket.is_full() {
                            if bucket.contains_id(self.my_id) {
				// if my id in bucket, then split
				// TODO: actually just check for range, not literal membership
				println!("we need to split this bucket that contains our own node id");
				// TODO: do split
				InsertResult::SplitOccurred
                            } else {
				// ping the lru and return a NeedsProbe result
				println!("We need to probe the LRU node in this full bucket to possibly replace");
				let lru = *bucket.node_infos.last().expect("We know the bucket is full");
				let probe_id = ProbeID::new_random();
				InsertResult::NeedsProbe{lru, probe_id}
                            }
			} else {
                            bucket.insert(peer);
			    InsertResult::Inserted
			}
		    };
                    break result;
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

    /// a probe either came back alive, or it timed out
    /// If alive, we keep the lru entry in the bucket and update its status.
    /// If dead, then we remove it from the bucket and complete the original insertion
    pub fn resolve_probe(&mut self, probe_id: ProbeID, alive: bool) {
    }
}

#[derive(Debug)]
pub struct Node {
    pub my_info: NodeInfo,
    pub routing_table: RoutingTable,
}

impl Node {
    pub fn new(k: usize, ip: IpAddr, port: u16) -> Self {
        let my_id = NodeID::new();
        let my_info = NodeInfo {
            ip_address: ip,
            udp_port: port,
            node_id: my_id,
        };

        Self {
            my_info,
            routing_table: RoutingTable::new(my_id, k),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::Ipv4Addr;

    /// Make a NodeID with a specific leading byte and the rest zero.
    fn id_with_first_byte(b: u8) -> NodeID {
        let mut id = [0u8; 20];
        id[0] = b;
        NodeID(id)
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
	rt.insert(make_node(1, 4001, 0x01));
	rt.insert(make_node(2, 4002, 0x02));
	rt.insert(make_node(3, 4003, 0x03));
	rt
    }

    #[test]
    fn create_routing_table_and_insert_up_to_k() {
        let my_id = id_with_first_byte(0xAA);
        let k = 3;
        let mut rt = RoutingTable::new(my_id, k);

        // Insert <= k peers should succeed
	assert!(matches!(rt.insert(make_node(1, 4001, 0x01)), InsertResult::Inserted));
	rt.insert(make_node(2, 4002, 0x02));
        rt.insert(make_node(3, 4003, 0x03));

	// attempt to insert the same node info again; it should be a NOOP
	assert!(matches!(rt.insert(make_node(1, 4001, 0x01)), InsertResult::AlreadyPresent));

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
	let insert_result = rt.insert(make_node(3, 4003, 0x03));

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
        rt.insert(make_node(1, 4001, 0x80));
        rt.insert(make_node(2, 4002, 0x81));
        assert_eq!(rt.all_nodes().len(), 2);

	// Next insert (MSB=0) should trigger a split (root contains our prefix)
	let expected_lru = make_node(3, 4003, 0x00);
        rt.insert(expected_lru);

        // After split, 3 nodes and 2 buckets
        assert_eq!(rt.all_nodes().len(), 3);
        assert_eq!(rt.count_buckets(), 2);


	// Currently far bucket has one node (0x00). Add another MSB=0 to fill it to k=2.
	rt.insert(make_node(4, 4004, 0x02));

	//NeedsProbe{lru: NodeInfo, probe_id: ProbeID},

	// Now insert yet another MSB=0 node; the far bucket is full and does NOT contain my_id,
	// so this should not split. We expect a probe of the LRU instead.
	let insert_result = rt.insert(make_node(5, 4005, 0x03));

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
