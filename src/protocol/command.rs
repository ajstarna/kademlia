use std::net::SocketAddr;
use tokio::sync::oneshot;

use crate::core::identifier::Key;
use crate::core::storage::Value;

/// Commands are the user-facing API into the ProtocolManager event loop.
///
/// A higher-level library (e.g., `KademliaDHT`) holds an `mpsc::Sender<Command>`
/// and sends requests into the single-threaded protocol task. This ensures that
/// all socket I/O, routing-table mutations, and lookup state are owned and
/// serialized by the event loop, avoiding races and out-of-band mutations.
///
/// Variants map to the core DHT operations:
/// - `Get`: Start a value lookup for `key`; the provided oneshot is fulfilled
///   with `Some(value)` if found, or `None` otherwise.
/// - `Put`: Find the k closest nodes to `key` and send `Store` to them; fire-and-forget
///   without an acknowledgement, matching typical Kademlia semantics.
/// - `Bootstrap`: Seed addresses to initiate a Kademlia self-lookup; used to
///   populate the routing table when first joining the network.
pub enum Command {
    /// Start a value lookup for `key`. The oneshot is completed with
    /// `Some(value)` if any node returns the value, or `None` if the lookup
    /// converges without a value.
    Get {
        key: Key,
        tx_value: oneshot::Sender<Option<Value>>,
    },
    /// Store `value` under `key`.
    /// Internally performs a node lookup to find the k closest peers and
    /// enqueues `Store` messages to them. The `ack` oneshot completes when the
    /// lookup converges and STORE messages have been scheduled (fire-and-forget).
    Put { key: Key, value: Value, tx_done: oneshot::Sender<()> },
    /// Initiate bootstrap by sending `FindNode(self)` to the given seed
    /// addresses. Responses are handled by the event loop to populate the
    /// routing table and drive a self-lookup toward convergence.
    Bootstrap { addrs: Vec<SocketAddr> },

    /// Test/debug helper: query whether this node currently has a value for `key`.
    /// Replies `true` if present in local storage.
    DebugHasValue { key: Key, tx_has: oneshot::Sender<bool> },
}
