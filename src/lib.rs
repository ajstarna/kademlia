pub mod dht;
mod core;
pub mod protocol;
mod test_support;

// Re-export commonly used types for consumers and integration tests
pub use crate::core::identifier::{Key, NodeID, NodeInfo};
pub use crate::core::storage::Value;
