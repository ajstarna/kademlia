use rand::RngCore;

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(pub [u8; 20]);

impl Key {
    /// Return a new Key given an input, likely a string.
    pub fn new<S: AsRef<[u8]>>(input: &S) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(input.as_ref());
        let digest = hasher.finalize();
        Self(digest.into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeID(pub [u8; 20]);

impl NodeID {
    /// Generate a new randomly generated NodeID
    pub fn new() -> Self {
        let mut bytes = [0u8; 20];
        rand::rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn get_bit_at(self, bit_index: usize) -> u8 {
        let byte_index = bit_index / 8;
        let bit_within_byte = bit_index % 8;
        let shift_amount = 7 - bit_within_byte;
        (self.0[byte_index] >> shift_amount) & 1u8
    }
}

pub trait KademliaID {
    fn as_bytes(&self) -> &[u8; 20];
}

impl KademliaID for Key {
    fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

impl KademliaID for NodeID {
    fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Distance([u8; 20]);

impl Distance {
    pub fn from_xor<A: KademliaID, B: KademliaID>(a: &A, b: &B) -> Self {
        let mut out = [0u8; 20];
        let a = a.as_bytes();
        let b = b.as_bytes();
        for i in 0..20 {
            out[i] = a[i] ^ b[i];
        }
        Distance(out)
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

        let node_id = NodeID(bytes);

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
