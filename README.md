# kademlia
Implementing the Kademlia distributed hash table from scratch.

Follows as much as possible the original paper: https://www.cs.cornell.edu/people/egs/714-spring05/kademlia.pdf

Uses MessagePack for the wire format: https://msgpack.org/

Known, intentional deviations from the paper:
- An `is_client` flag on messages so non‑storing clients aren’t added to routing tables.
- An explicit `Pong` message in response to `Ping`.

## Code Structure

- `src/protocol/*`: main networking loop; messaging, lookups, routing updates.
- `src/core/*`: identifiers, routing table, and value storage.
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
  - `target/debug/kademlia peer --bind 0.0.0.0:8080`
- Join via two seeds:
  - `target/debug/kademlia peer --bind 0.0.0.0:0 --bootstrap 127.0.0.1:8080 --bootstrap 127.0.0.1:8081`
- Get (40‑hex key, optionally 0x‑prefixed):
  - `target/debug/kademlia get --bootstrap 127.0.0.1:8080 0x0123...cdef`
- Put (key omitted = SHA1(value)):
  - Positional: `target/debug/kademlia put --bootstrap 127.0.0.1:8080 "hello world"`
  - With flag: `target/debug/kademlia put --bootstrap 127.0.0.1:8080 --value "hello world"`
  - Explicit key: `target/debug/kademlia put --bootstrap 127.0.0.1:8080 --key 0123...cdef "hello world"`
  - Tip: if your value begins with a dash, use `--` before it, or use `--value`.

Flags:
- Peer: `--bind <ip:port>`, `--bootstrap <ip:port>` (repeatable), `--k <int>`, `--alpha <int>`
- Get/Put: `--bind <ip:port>`, `--bootstrap <ip:port>` (repeatable)

Keys are 20 bytes (H160), printed/accepted as 40 hex chars.

For a quick multi‑node demo, see `tests/manual/run_network.sh`.
