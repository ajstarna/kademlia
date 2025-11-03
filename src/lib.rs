mod core;
pub mod dht;
pub mod protocol;
mod test_support;

// Re-export commonly used types for consumers and integration tests
pub use crate::core::identifier::{Key, NodeID, NodeInfo};
pub use crate::core::storage::Value;

use tracing_subscriber::{fmt, EnvFilter};
use ctor::ctor;

#[ctor]
fn init_tracing() {
    // Avoid duplicate initialization if multiple tests run in parallel
    let _ = fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .with_target(false)
        .compact()
        .try_init();
}
