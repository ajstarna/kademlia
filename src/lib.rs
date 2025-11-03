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
    let filter = EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| EnvFilter::new("info")); // fallback if RUST_LOG unset

    let _ = fmt()
	.with_env_filter(filter)
	.with_target(false)
	.compact()
	.try_init();
}
