#[cfg(test)]
pub mod test_support {
    use crate::node::identifier::{NodeID, NodeInfo};
    use ethereum_types::H160;
    use std::net::{IpAddr, Ipv4Addr};

    pub fn id_with_first_byte(b: u8) -> NodeID {
        let mut id = [0u8; 20];
        id[0] = b;
        NodeID(H160::from(id))
    }

    pub fn make_peer(last_octet: u8, port: u16, first_byte: u8) -> NodeInfo {
        NodeInfo {
            ip_address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, last_octet)),
            udp_port: port,
            node_id: id_with_first_byte(first_byte),
        }
    }
}
