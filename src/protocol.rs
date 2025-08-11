use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::{
    node::identifier::{Key, NodeID},
    node::{Node, NodeInfo},
};

type Value = Vec<u8>;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Message {
    Ping { node_id: NodeID },
    Pong { node_id: NodeID },
    Store { key: Key, value: Value },
    FindNode { node_id: NodeID },
    FindValue { key: Key },
}

pub struct ProtocolManager {
    pub node: Node,
    pub socket: UdpSocket,
}

impl ProtocolManager {
    pub fn new(node: Node, socket: UdpSocket) -> Self {
        Self { node, socket }
    }

    async fn handle_message(&mut self, msg: Message, src_addr: SocketAddr) -> anyhow::Result<()> {
        match msg {
            Message::Ping { node_id } => {
                println!("Received Ping from {node_id:?}");
                let peer = NodeInfo {
                    ip_address: src_addr.ip(),
                    udp_port: src_addr.port(),
                    node_id,
                };
                self.node.routing_table.insert(peer);

                let pong = Message::Pong {
                    node_id: self.node.my_info.node_id,
                };
                let bytes = rmp_serde::to_vec(&pong)?;
                self.socket.send_to(&bytes, src_addr).await?;
            }

            Message::Pong { node_id } => {
                println!("Received Pong from {node_id:?}");
                // Maybe mark the node as alive or update routing table
            }

            Message::Store { key, value } => {
                println!("Store request: key={key:?}, value={value:?}");
                // Store the value in your local store or database
            }

            Message::FindNode { node_id } => {
                println!("FindNode request: looking for {node_id:?}");
                // Find closest nodes to the given ID in your routing table
            }

            Message::FindValue { key } => {
                println!("FindValue request: key={key:?}");
                // Lookup the value, or return closest nodes if not found
            }
        }
        Ok(())
    }


    /// TODO!!! this select will let us handle timeouts on outgoing probes
    /// we need to handle pongs for specific probes and tell the table the result of the LRU
    loop {
    tokio::select! {
        Ok((n, addr)) = sock.recv_from(&mut buf) => {
            let (txid, kind) = parse_header(&buf[..n]);
            match kind {
                MsgKind::Pong => {
                    if let Some(probe_id) = pending.remove(&(addr, txid)) {
                        // direct call: no channel hop needed
                        let outcome = rt.resolve_probe(probe_id, /*alive=*/true);
                        // optionally react to outcome (metrics/logging)
                        // e.g., if outcome == RtInsertOutcome::Replaced { evicted, .. } { ... }
                    } else {
                        // late/unexpected pong; ignore
                    }
                }
                // other message types...
                _ => handle_other(&mut rt, &buf[..n], addr)?,
            }
        }

        // timeout arm (sweep or heap-based)
        _ = &mut tick => {
            let now = Instant::now();
            let mut expired = Vec::new();
            for (&key, probe_id) in pending.iter() {
                if deadline_for(key) <= now { expired.push((key, *probe_id)); }
            }
            for (key, probe_id) in expired {
                pending.remove(&key);
                let _ = rt.resolve_probe(probe_id, /*alive=*/false);
            }
        }
    }
}

    /// This for messages in a loop, and respond accordingly
    pub async fn run(mut self) {
        let mut buf = [0u8; 1024];
        loop {
            match self.socket.recv_from(&mut buf).await {
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
    }
}
