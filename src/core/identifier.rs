use ethereum_types::H160;
use std::net::IpAddr;
use std::ops::BitXor;

use rand::Rng;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProbeID(u64);

impl ProbeID {
    pub fn new_random() -> Self {
        let val: u64 = rand::rng().random();
        Self(val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeID(pub H160);

impl NodeID {
    pub fn new() -> Self {
        NodeID(H160::random())
    }

    pub fn zero() -> Self {
        NodeID(H160::zero())
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(H160::from_slice(bytes))
    }

    pub fn from_hashed<S: AsRef<[u8]>>(input: &S) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(input.as_ref());
        let digest = hasher.finalize();
        Self(H160::from_slice(&digest))
    }

    pub fn get_bit_at(&self, bit_index: usize) -> u8 {
        let bytes = self.0.as_bytes();
        let byte_index = bit_index / 8;
        let bit_within_byte = bit_index % 8;
        let shift_amount = 7 - bit_within_byte;
        (bytes[byte_index] >> shift_amount) & 1u8
    }

    pub fn prefix_bits(&self, depth: usize) -> u128 {
        let mut acc: u128 = 0;
        for i in 0..depth {
            acc = (acc << 1) | self.get_bit_at(i) as u128;
        }
        acc
    }

    pub fn with_bit(&self, bit_index: usize, bit: u8) -> Self {
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

    pub fn distance(&self, other: &NodeID) -> Distance {
        Distance(self.0 ^ other.0)
    }

    /// Return a short, human-friendly hex for logging, like ab12cd34…ef90a1b2
    pub fn short_hex(&self) -> String {
        let b = self.0.to_fixed_bytes();
        format!(
            "{:02x}{:02x}{:02x}{:02x}…{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[16], b[17], b[18], b[19]
        )
    }
}

impl BitXor for NodeID {
    type Output = NodeID;

    fn bitxor(self, rhs: Self) -> Self::Output {
        NodeID(self.0 ^ rhs.0)
    }
}

pub type Key = NodeID;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Distance(H160);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeInfo {
    #[serde(with = "serde_ipaddr")]
    pub ip_address: IpAddr,
    pub udp_port: u16,
    pub node_id: NodeID,
}

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
