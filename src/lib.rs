mod core;
pub mod dht;
pub mod protocol;
mod test_support;

// Re-export commonly used types for consumers and integration tests
pub use crate::core::identifier::{Key, NodeID, NodeInfo};
pub use crate::core::storage::Value;

#[cfg(test)]
use ctor::ctor;
#[cfg(test)]
use tracing_subscriber::{fmt, EnvFilter};

#[cfg(test)]
#[ctor]
fn init_tracing() {
    // set up a logger for tests
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = fmt()
        .with_env_filter(filter)
        .with_test_writer() // let libtest capture per-test
        .with_target(false)
        .compact()
        .try_init();
}
