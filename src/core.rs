use std::net::IpAddr;

pub mod identifier;
pub mod routing_table;
pub mod storage;

use identifier::{Key, NodeID, NodeInfo};
use routing_table::RoutingTable;
use storage::{Storage, Value};

const NUM_BUCKETS: usize = 160; // needs to match SHA1's output length
const K: usize = 20;

#[derive(Debug)]
pub struct NodeState {
    pub my_info: NodeInfo,
    pub routing_table: RoutingTable,
    storage: Storage,
}

impl NodeState {
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
            storage: Storage::new(),
        }
    }

    pub fn store(&mut self, key: Key, value: Value) {
        self.storage.insert(key, value);
    }

    pub fn get(&self, key: &Key) -> Option<&Value> {
        self.storage.get(key)
    }
}

