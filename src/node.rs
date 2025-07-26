use crate::identifier::NodeID;

const NUM_BUCKETS: usize = 160; // needs to match SHA1's output length
const K: usize = 20;

struct NodeInfo {
    ip_address: String,
    udp_port: String,
    node_id: NodeID,
}

struct KBucket {
    node_infos: Vec<NodeInfo>, // TODO: LRU
}
impl KBucket {
    pub fn new(k: usize) -> Self {
        Self {
            node_infos: Vec::with_capacity(k),
        }
    }
}

/// A binary tree whose leaves are K-buckets.
/// Each k-bucket contains nodes with some common prefix
/// of their ids.
enum BucketTree {
    Bucket(KBucket),
    Branch {
        bit_index: usize, // which bit this branch splits on
        one: Box<BucketTree>,
        zero: Box<BucketTree>,
    },
}

struct RoutingTable {
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

    //pub fn insert(
}

pub struct Node {
    pub my_info: NodeInfo,
    pub routing_table: RoutingTable,
}

impl Node {
    pub fn new(k: usize) -> Self {
        let my_id = NodeID::new();
        let my_info = NodeInfo {
            ip_address: "todo".to_owned(),
            udp_port: "todo".to_owned(),
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

    #[test]
    fn it_works() {
        let _node = Node::new(K);
    }
}
