use std::net::IpAddr;

use crate::identifier::NodeID;

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

    pub fn insert(&mut self, peer: NodeInfo) {
        println!("{:?}", peer);
	let mut current = &mut self.tree;
	loop {
	    match current {
		BucketTree::Bucket(ref mut bucket) => {
		    println!("bucket");
		    if bucket.is_full() {
			// split the bucket (if not at the last bit)
		    }
		    break;
		},
		BucketTree::Branch { bit_index, ref one, ref zero } => {
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

    #[test]
    fn it_works() {
        let info = NodeInfo {
            ip_address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            udp_port: 8080,
            node_id: NodeID::new(), // assuming you have this
        };
    }
}
