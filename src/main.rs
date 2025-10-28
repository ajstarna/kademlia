use std::net::SocketAddr;

use clap::{Parser, Subcommand};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use kademlia::dht::KademliaDHT;
use kademlia::protocol::{Command, ProtocolManager};
use kademlia::{Key, NodeID, Value};

/// Run a Kademlia node as a seed or join via bootstrap peers.
#[derive(Parser, Debug)]
#[command(name = "kademlia", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a long-lived peer node (seed or join via bootstrap peers)
    Peer {
        /// Address to bind on (ip:port)
        #[arg(long = "bind", default_value = "0.0.0.0:8080", value_parser = clap::value_parser!(SocketAddr))]
        bind: SocketAddr,

        /// Bootstrap peer(s) to join (repeatable). When omitted, runs as a pure seed.
        #[arg(long = "bootstrap", value_parser = clap::value_parser!(SocketAddr))]
        bootstrap: Vec<SocketAddr>,

        /// Kademlia bucket size (replication parameter)
        #[arg(long = "k", default_value_t = 20)]
        k: usize,

        /// Kademlia parallelism (concurrency of lookups)
        #[arg(long = "alpha", default_value_t = 3)]
        alpha: usize,
    },

    /// Fetch a value by key (hex-encoded 20-byte ID)
    Get {
        /// Address to bind on (ip:port). Use port 0 for ephemeral.
        #[arg(long = "bind", default_value = "0.0.0.0:0", value_parser = clap::value_parser!(SocketAddr))]
        bind: SocketAddr,

        /// Bootstrap peer(s) to query (repeatable). At least one is typically required.
        #[arg(long = "bootstrap", value_parser = clap::value_parser!(SocketAddr))]
        bootstrap: Vec<SocketAddr>,

        /// 40 hex chars (optionally 0x-prefixed) representing the key
        key: String,
    },

    /// Store a value; if --key omitted, key = SHA1(value)
    #[command(alias = "store")]
    Put {
        /// Address to bind on (ip:port). Use port 0 for ephemeral.
        #[arg(long = "bind", default_value = "0.0.0.0:0", value_parser = clap::value_parser!(SocketAddr))]
        bind: SocketAddr,

        /// Bootstrap peer(s) to query (repeatable). At least one is typically required.
        #[arg(long = "bootstrap", value_parser = clap::value_parser!(SocketAddr))]
        bootstrap: Vec<SocketAddr>,

        /// Positional value to store (raw string bytes). Required unless --value is used.
        #[arg(required_unless_present = "value")]
        data: Option<String>,

        /// Value to store (raw string bytes)
        #[arg(long = "value", required_unless_present = "data")]
        value: Option<String>,

        /// Optional 40-hex key (optionally 0x-prefixed); when omitted, SHA1(value)
        #[arg(long = "key")]
        key: Option<String>,

    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Peer {
            bind,
            bootstrap,
            k,
            alpha,
        } => {
            println!(
                "Starting node on {} with k={} alpha={} ({} mode)",
                bind,
                k,
                alpha,
                if bootstrap.is_empty() {
                    "seed"
                } else {
                    "client"
                }
            );

            let socket = UdpSocket::bind(bind).await?;

            if bootstrap.is_empty() {
                // Pure seed: no bootstrap, run headless.
                let manager = ProtocolManager::new_headless(socket, k, alpha)?;
                manager.run().await;
            } else {
                // Join existing network by bootstrapping to provided peers.
                let (tx, rx) = mpsc::channel::<Command>(100);
                let manager = ProtocolManager::new(socket, rx, k, alpha)?;
                let boot_addrs = bootstrap.clone();

                tokio::spawn(manager.run());

                // Give the protocol task a moment to start
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                // Initiate bootstrap
                tx.send(Command::Bootstrap { addrs: boot_addrs }).await?;

                println!(
                    "Bootstrapping to {} peer(s). Press Ctrl-C to exit.",
                    bootstrap.len()
                );

                // Stay alive until Ctrl-C
                tokio::signal::ctrl_c().await?;
            }
        }

        Commands::Get { bind, bootstrap, key } => {
            println!("Starting ephemeral get on {}...", bind);
            let dht = KademliaDHT::start_client(&bind.to_string(), bootstrap.clone()).await?;
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;

            let key = parse_node_id_hex(&key)?;
            match dht.get(key).await? {
                Some(value) => {
                    println!("OK: {} bytes", value.len());
                    println!("value(hex)={}", to_hex(&value));
                    if let Ok(s) = String::from_utf8(value.clone()) {
                        println!("value(utf8)={}", s);
                    }
                }
                None => {
                    println!("NOT FOUND");
                }
            }
        }

        Commands::Put { bind, bootstrap, data, value, key } => {
            println!("Starting ephemeral put on {}...", bind);
            let dht = KademliaDHT::start_client(&bind.to_string(), bootstrap.clone()).await?;
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;

            let val_str = match (value, data) {
                (Some(v), _) => v,
                (None, Some(d)) => d,
                (None, None) => unreachable!("clap enforces one of --value or positional data"),
            };
            let value_bytes: Value = val_str.into_bytes();
            let key: Key = match key {
                Some(hex) => parse_node_id_hex(&hex)?,
                None => NodeID::from_hashed(&value_bytes),
            };
            dht.put(key, value_bytes.clone()).await?;
            println!("PUT enqueued: {} bytes", value_bytes.len());
            println!("key(hex)={}", node_id_to_hex(key));
        }
    }

    Ok(())
}

fn strip_0x_prefix(s: &str) -> &str {
    if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        rest
    } else {
        s
    }
}

fn parse_node_id_hex(s: &str) -> anyhow::Result<NodeID> {
    let s = strip_0x_prefix(s);
    if s.len() != 40 {
        // H160 = 20 bytes = 40 hex chars
        anyhow::bail!("expected 40 hex chars for key, got {}", s.len());
    }
    let mut bytes = [0u8; 20];
    for i in 0..20 {
        let j = i * 2;
        bytes[i] = u8::from_str_radix(&s[j..j + 2], 16)
            .map_err(|_| anyhow::anyhow!("invalid hex at bytes {}..{}", j, j + 2))?;
    }
    Ok(NodeID::from_bytes(&bytes))
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn node_id_to_hex(id: NodeID) -> String {
    let b = id.0.to_fixed_bytes();
    to_hex(&b)
}
