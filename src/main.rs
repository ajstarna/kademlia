use std::net::SocketAddr;

use clap::Parser;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use kademlia::protocol::{Command, ProtocolManager};

/// Run a Kademlia node as a seed or join via bootstrap peers.
#[derive(Parser, Debug)]
#[command(name = "kademlia-node", version, about)]
struct Cli {
    /// Address to listen on (ip:port)
    #[arg(long = "listen", default_value = "0.0.0.0:8080", value_parser = clap::value_parser!(SocketAddr))]
    listen: SocketAddr,

    /// Bootstrap peer(s) to join (repeatable). When omitted, runs as a pure seed.
    #[arg(long = "bootstrap", value_parser = clap::value_parser!(SocketAddr))]
    bootstrap: Vec<SocketAddr>,

    /// Kademlia bucket size (replication parameter)
    #[arg(long = "k", default_value_t = 20)]
    k: usize,

    /// Kademlia parallelism (concurrency of lookups)
    #[arg(long = "alpha", default_value_t = 3)]
    alpha: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    println!(
        "Starting node on {} with k={} alpha={} ({} mode)",
        cli.listen,
        cli.k,
        cli.alpha,
        if cli.bootstrap.is_empty() { "seed" } else { "client" }
    );

    let socket = UdpSocket::bind(cli.listen).await?;

    if cli.bootstrap.is_empty() {
        // Pure seed: no bootstrap, run headless.
        let manager = ProtocolManager::new_headless(socket, cli.k, cli.alpha)?;
        manager.run().await;
    } else {
        // Join existing network by bootstrapping to provided peers.
        let (tx, rx) = mpsc::channel::<Command>(100);
        let manager = ProtocolManager::new(socket, rx, cli.k, cli.alpha)?;
        let boot_addrs = cli.bootstrap.clone();

        tokio::spawn(manager.run());

        // Give the protocol task a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Initiate bootstrap
        tx.send(Command::Bootstrap { addrs: boot_addrs }).await?;

        println!("Bootstrapping to {} peer(s). Press Ctrl-C to exit.", cli.bootstrap.len());

        // Stay alive until Ctrl-C
        tokio::signal::ctrl_c().await?;
    }

    Ok(())
}
