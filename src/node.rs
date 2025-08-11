use std::net::IpAddr;

use anyhow::bail;

mod identifier;
use identifier::NodeID;

const NUM_BUCKETS: usize = 160; // needs to match SHA1's output length
const K: usize = 20;

#[derive(Debug)]
pub struct NodeInfo {
    pub ip_address: IpAddr,
    pub udp_port: u16,
    pub node_id: NodeID,
}


#[derive(Debug)]
struct KBucket {
    k: usize,
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

    pub fn contains_id(&self, search_id: NodeID) -> bool {
        self.node_infos.iter().any(|info| info.node_id == search_id)
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

enum InsertResult{
    Inserted,
    AlreadyPresent,
    NeedsProbe{lru: NodeInfo, probe_id: ProbeID},
    SplitOccured
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

    // TODO: need a result type here
    // Inserted
    // Rejected
    // NeedsProbe(peer) # ping the lru in a bucket
    pub fn insert(&mut self, peer: NodeInfo) -> anyhow::Result<()> {
        println!("{peer:?}");
        let mut current = &mut self.tree;
        loop {
            match current {
                BucketTree::Bucket(ref mut bucket) => {
                    println!("bucket");
                    if bucket.is_full() {
                        if bucket.contains_id(self.my_id) {
                            // if my id in bucket, then split
                            println!("we need to split this bucket that contains our own node id");
                        } else {
                            // ping the lru and return a NeedsProbe result
                            println!("We need to probe the LRU node in this full bucket to possibly replace")
                        }
                    } else {
                        bucket.insert(peer);
                    }
                    break Ok(());
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

    #[test]
    fn create_routing_table_and_insert_up_to_k() {
        let my_id = id_with_first_byte(0xAA);
        let k = 3;
        let mut rt = RoutingTable::new(my_id, k);

        // Insert <= k peers should succeed
        rt.insert(make_node(1, 4001, 0x01));
        rt.insert(make_node(2, 4002, 0x02));
        rt.insert(make_node(3, 4003, 0x03));

        let all = rt.all_nodes();
        assert_eq!(all.len(), 3, "should hold exactly k nodes before any split");

        assert_eq!(rt.count_buckets(), 1);
    }

    #[test]
    fn bucket_splits_when_full_if_it_contains_our_prefix() {
        let my_id = id_with_first_byte(0x80);
        let k = 2;
        let mut rt = RoutingTable::new(my_id, k);

        // Fill the root bucket
        rt.insert(make_node(1, 4001, 0x80));
        rt.insert(make_node(2, 4002, 0x81));
        assert_eq!(rt.all_nodes().len(), 2);

        // Next insert should trigger a split (since the root bucket contains our prefix),
        // moving nodes into child buckets by the next bit.
        rt.insert(make_node(3, 4003, 0x00));

        // After split, total nodes unchanged + 1 new = 3
        assert_eq!(rt.all_nodes().len(), 3);

        assert_eq!(rt.count_buckets(), 2);
        // Optionally assert tree shape here once you expose accessors to inspect it.
    }
}
