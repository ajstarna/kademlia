# kademlia
Implementing the Kademlia distributed hash table from scratch.

## CLI

A simple CLI is provided to run a headless node either as a seed or bootstrapping to known peers.

Examples:

- Run as a seed on port 8080:
  - `cargo run -- --listen 0.0.0.0:8080`

- Join an existing network via two seeds:
  - `cargo run -- --listen 0.0.0.0:0 --bootstrap 127.0.0.1:8080 --bootstrap 127.0.0.1:8081`

Flags:
- `--listen <ip:port>`: address to bind (default `0.0.0.0:8080`)
- `--bootstrap <ip:port>`: repeatable list of seed peers; omit to run as a seed
- `--k <int>`: bucket size/replication factor (default 20)
- `--alpha <int>`: lookup parallelism (default 3)
