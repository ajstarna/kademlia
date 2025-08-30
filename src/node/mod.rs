use std::net::IpAddr;
use rand::Rng;


pub mod identifier;
pub mod storage;

use identifier::NodeID;


const NUM_BUCKETS: usize = 160; // needs to match SHA1's output length
const K: usize = 20;

#[derive(Debug)]
pub struct Node {
    pub my_info: NodeInfo,
    pub routing_table: RoutingTable,
    pub storage: Storage,
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
