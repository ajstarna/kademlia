use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::time::{interval, Duration, MissedTickBehavior};

use std::time::Instant;
use std::collections::HashMap;
use crate::{
    node::identifier::{Key, NodeID},
    node::{InsertResult, Node, NodeInfo, ProbeID},
};

type Value = Vec<u8>;

// each message type includes the NodeID of the sender
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Message {
    Ping { node_id: NodeID },
    Pong { node_id: NodeID },
    Store { node_id: NodeID, key: Key, value: Value },
    FindNode { node_id: NodeID, target: NodeID },
    FindValue { node_id: NodeID, key: Key },
}

pub struct ProtocolManager {
    pub node: Node,
    pub socket: UdpSocket,
}


/// Effect represents the "side effect" that `handle_message` wants the outer
/// event loop to perform.
///
/// It decouples pure routing-table logic (insertions, probes, splits, etc.)
/// from I/O side effects (sending a Pong, starting a probe, etc.).
enum Effect {
    Send { addr: SocketAddr, bytes: Vec<u8> },
    StartProbe { addr: SocketAddr, probe_id: ProbeID, bytes: Vec<u8> },
}

/// HandleResult is the combined output of `handle_message`.
///
/// It returns both:
/// - the `InsertResult` from attempting to update the routing table
/// - and any `Effect` that should be executed by the main loop
///
/// This keeps routing decisions and networking actions unified, while
/// still separating concerns: routing-table logic doesn’t send packets
/// itself, but it can request that the manager do so.
type HandleResult = (Vec<Effect>, Option<InsertResult>);


struct PendingProbe {
    peer: SocketAddr,
    deadline: Instant,
}

impl ProtocolManager {
    pub fn new(node: Node, socket: UdpSocket) -> Self {
        Self { node, socket }
    }

    fn observe_contact(&mut self, src_addr: SocketAddr, node_id: NodeID) -> InsertResult {
        let peer = NodeInfo {
            ip_address: src_addr.ip(),
            udp_port: src_addr.port(),       // use observed source port (NAT-friendly)
            node_id,
        };
        let insert_result = self.node.routing_table.insert(peer);
	insert_result
    }

    async fn handle_message(&mut self, msg: Message, src_addr: SocketAddr) -> anyhow::Result<()> {
        match msg {
            Message::Ping { node_id } => {
                println!("Received Ping from {node_id:?}");
		self.observe_contact(src_addr, node_id);
                let pong = Message::Pong {
                    node_id: self.node.my_info.node_id,
                };
                let bytes = rmp_serde::to_vec(&pong)?;
                self.socket.send_to(&bytes, src_addr).await?;
            }

            Message::Pong { node_id } => {
                println!("Received Pong from {node_id:?}");
		self.observe_contact(src_addr, node_id);
                // Maybe mark the node as alive or update routing table
            }

            Message::Store { node_id, key, value } => {
                println!("Store request: key={key:?}, value={value:?}");
		self.observe_contact(src_addr, node_id);
                // Store the value in your local store or database
            }

            Message::FindNode { node_id, target } => {
                println!("FindNode request: looking for {node_id:?}");
		self.observe_contact(src_addr, node_id);
                // Find closest nodes to the given ID in your routing table
            }

            Message::FindValue { node_id, key } => {
                println!("FindValue request: key={key:?}");
		self.observe_contact(src_addr, node_id);
                // Lookup the value, or return closest nodes if not found
            }
        }
        Ok(())
    }

    /// This for messages in a loop, and respond accordingly
    pub async fn run(mut self) {
        let mut buf = [0u8; 1024];

	let mut ticker = interval(Duration::from_secs(1)); // how often do we clean expired probes
	ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

	// where we store pending probes and clean them up if they time out
	let mut pending: HashMap<ProbeID, PendingProbe> = HashMap::new();

        loop {
	    tokio::select! {
		// message receive arm
		result = self.socket.recv_from(&mut buf) =>  {
		    match result {
			Ok((len, src_addr)) => {
			    println!("Received {len} bytes from {src_addr}");
			    let msg = rmp_serde::from_slice::<Message>(&buf[..len]);
			    match msg {
				Ok(msg) => {
				    let _ = self.handle_message(msg, src_addr).await;
				}
				Err(e) => {
				    eprintln!("Error receiving message: {e}");
				    continue;
				}
			    }
			}
			Err(e) => {
			    eprintln!("Error receiving message: {e}");
			    continue;
			}
		    }
		}

		// timeout arm (sweep or heap-based)
		_ = ticker.tick() => {
		    let now = Instant::now();
		    let mut expired = Vec::new();
		    for (probe_id, pending) in pending.iter() {
			if pending.deadline <= now {
			    expired.push(probe_id);
			}
		    }
		    for probe_id in expired {
			pending.remove(&probe_id);
			// tell the table that this probe timed out
			let _ = self.node.routing_table.resolve_probe(probe_id, /*alive=*/false);
		    }

		}
	    }
	}
    }
}
