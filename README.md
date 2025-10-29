# kademlia
Implementing the Kademlia distributed hash table from scratch.

Follows as much as possible the original paper: https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf

Uses MessagePack for the wire format: https://msgpack.org/

Known, intentional deviations from the paper:
- An `is_client` flag on messages so non‑storing clients aren’t added to routing tables.
- An explicit `Pong` message in response to `Ping`.

## Code Structure

- `src/core/*`: identifiers, routing table, and value storage.
- `src/protocol/*`: main networking loop; message handling, lookups, probes, routing table updates.
    - note: see `ProtocolManager::run()` for an entry point to the code base.
- `src/dht.rs`: small async client API that drives `ProtocolManager` via commands.
- `src/main.rs`: CLI entry point.

## Build and Test

- Build: `cargo build`
- Tests: `cargo test`

## CLI

Subcommands:
- `peer`: run a long‑lived node (seed or join via bootstrap peers)
- `get`: short‑lived client to fetch a value by key
- `put` (alias: `store`): short‑lived client to store a value

Run examples (after `cargo build`):
- Seed on port 8080:
  - `cargo run peer --bind 0.0.0.0:8080`
- Join via two seeds:
  - `cargo run peer --bind 0.0.0.0:0 --bootstrap 127.0.0.1:8080 --bootstrap 127.0.0.1:8081`
- Get (40‑hex key, optionally 0x‑prefixed):
  - `cargo run get --bootstrap 127.0.0.1:8080 0x0123...cdef`
- Put (key omitted = SHA1(value)):
  - Positional: `target/debug/kademlia put --bootstrap 127.0.0.1:8080 "hello world"`
  - With flag: `target/debug/kademlia put --bootstrap 127.0.0.1:8080 --value "hello world"`
  - Explicit key: `target/debug/kademlia put --bootstrap 127.0.0.1:8080 --key 0123...cdef "hello world"`

Flags:
- Peer: `--bind <ip:port>`, `--bootstrap <ip:port>` (repeatable), `--k <int>`, `--alpha <int>`
- Get/Put: `--bind <ip:port>`, `--bootstrap <ip:port>` (repeatable)

Keys are 20 bytes (H160), printed/accepted as 40 hex chars.

## Demo

For a quick multi‑node demo, see `tests/manual/test_network.sh`.
