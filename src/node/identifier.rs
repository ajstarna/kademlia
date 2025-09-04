use ethereum_types::H160;
use std::net::IpAddr;
use std::ops::BitXor;

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(pub H160);

impl Key {
    /// Return a new Key given an input, likely a string.
    pub fn new<S: AsRef<[u8]>>(input: &S) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(input.as_ref());
        let digest = hasher.finalize();
        Self(H160::from_slice(&digest))
    }

    pub fn to_node_id(&self) -> NodeID {
        NodeID(self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeID(pub H160);

impl NodeID {
    /// randomly generate a new ID
    pub fn new() -> Self {
        NodeID(H160::random())
    }

    pub fn zero() -> Self {
        NodeID(H160::zero())
    }

    pub fn get_bit_at(&self, bit_index: usize) -> u8 {
        let bytes = self.0.as_bytes();
        let byte_index = bit_index / 8;
        let bit_within_byte = bit_index % 8;
        let shift_amount = 7 - bit_within_byte;
        (bytes[byte_index] >> shift_amount) & 1u8
    }

    /// get just the first `depth` bits
    /// USeful when determingin if a given Kbucket covers a new node ID
    pub fn prefix_bits(&self, depth: usize) -> u128 {
        let mut acc: u128 = 0;
        for i in 0..depth {
            acc = (acc << 1) | self.get_bit_at(i) as u128;
        }
        acc
    }

    /// create a copy of this NodeID but with a given bit set to a given value.
    /// Useful when splitting buckets and assigning the new buckets' prefixes.
    pub fn with_bit(&self, bit_index: usize, bit: u8) -> Self {
        //let mut bytes: [u8; 20] = (*self.0.as_bytes()).clone();
        let mut bytes: [u8; 20] = *self.0.as_fixed_bytes();

        let byte_index = bit_index / 8;
        let bit_within_byte = bit_index % 8;
        let shift_amount = 7 - bit_within_byte;

        if bit == 1 {
            bytes[byte_index] |= 1 << shift_amount;
        } else {
            bytes[byte_index] &= !(1 << shift_amount);
        }

        NodeID(H160::from(bytes))
    }

    // helpful when comparing distances across nodes in our buckets
    pub fn distance(&self, other: &NodeID) -> Distance {
        Distance(self.0 ^ other.0)
    }
}

impl BitXor for NodeID {
    type Output = NodeID;

    fn bitxor(self, rhs: Self) -> Self::Output {
        NodeID(self.0 ^ rhs.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Distance(H160);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeInfo {
    #[serde(with = "serde_ipaddr")]
    pub ip_address: IpAddr,
    pub udp_port: u16,
    pub node_id: NodeID,
}

// Needed for msgpack ser/de of IpAddr.
// Instead of producing a map with the enum variant, we simply convert right into a string.
mod serde_ipaddr {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::net::IpAddr;

    pub fn serialize<S>(ip: &IpAddr, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_str(&ip.to_string())
    }

    pub fn deserialize<'de, D>(d: D) -> Result<IpAddr, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn find_bits() {
        let mut bytes = [0u8; 20];
        bytes[1] = 5; // 00000101
        bytes[10] = 64; // 01000000

        let node_id = NodeID(H160::from(bytes));

        assert_eq!(node_id.get_bit_at(5), 0); // first byte is all zeros

        // second byte
        assert_eq!(node_id.get_bit_at(8), 0);
        assert_eq!(node_id.get_bit_at(13), 1);
        assert_eq!(node_id.get_bit_at(14), 0);
        assert_eq!(node_id.get_bit_at(15), 1);

        // 10th byte
        assert_eq!(node_id.get_bit_at(80), 0);
        assert_eq!(node_id.get_bit_at(81), 1);
        assert_eq!(node_id.get_bit_at(82), 0);
    }
}
