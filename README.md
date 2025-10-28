# kademlia
Implementing the Kademlia distributed hash table from scratch.

## CLI

A simple CLI is provided with subcommands:

- `peer`: run a long‑lived node (seed or join via bootstrap peers)
- `get`: short‑lived client to fetch a value by key
- `put` (alias: `store`): short‑lived client to store a value

Examples:

- Run as a seed on port 8080:
  - `cargo run -- peer --bind 0.0.0.0:8080`

- Join an existing network via two seeds:
  - `cargo run -- peer --bind 0.0.0.0:0 --bootstrap 127.0.0.1:8080 --bootstrap 127.0.0.1:8081`

- Get (hex key, optionally 0x‑prefixed):
  - `cargo run -- get --bootstrap 127.0.0.1:8080 0x0123...cdef`

- Put (key omitted = SHA1(value)):
  - Positional value: `cargo run -- put --bootstrap 127.0.0.1:8080 "hello world"`
  - Flag value: `cargo run -- put --bootstrap 127.0.0.1:8080 --value "hello world"`
  - Explicit key: `cargo run -- put --bootstrap 127.0.0.1:8080 --key 0123...cdef "hello world"`
  - Tip: if your value begins with a dash, use `--` before it, or use `--value`: `cargo run -- put --bootstrap 127.0.0.1:8080 -- --starts-with-dash`

Peer flags:
- `--bind <ip:port>`: address to bind (default `0.0.0.0:8080`)
- `--bootstrap <ip:port>`: repeatable list of seed peers; omit to run as a seed
- `--k <int>`: bucket size/replication factor (default 20)
- `--alpha <int>`: lookup parallelism (default 3)

Get/Put flags:
- `--bind <ip:port>`: address to bind (default `0.0.0.0:0`)
- `--bootstrap <ip:port>`: repeatable list of seed peers
