#!/usr/bin/env bash

# Simple manual test that:
# - Starts two seed peers and N joining peers
# - Performs a Put using the CLI client
# - Performs a Get for the returned key
# - Cleans up all peers on exit

set -euo pipefail

# Config (override via env vars when calling the script)
: "${NUM_SEEDS:=2}"
: "${SEED_BASE_PORT:=8080}"
: "${NUM_JOIN_PEERS:=2}"
: "${K:=20}"
: "${ALPHA:=3}"
: "${VALUE:=hello-from-manual-script}"

# Resolve repo root from this script's location
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd)
LOG_DIR="${REPO_ROOT}/tests/manual/logs"
mkdir -p "${LOG_DIR}"

pushd "${REPO_ROOT}" >/dev/null

echo "Building binary (once) ..."
cargo build --quiet >/dev/null
BIN="${REPO_ROOT}/target/debug/kademlia"
if [[ ! -x "${BIN}" ]]; then
  echo "Binary not found at ${BIN}" >&2
  exit 1
fi

PEER_PIDS=()
SEED_ADDRS=()

start_seed() {
  local port="$1"
  local log="${LOG_DIR}/peer-${port}.log"
  echo "Starting seed peer on 127.0.0.1:${port} (logs: ${log})"
  # Run as a seed (no bootstrap)
  # Default to verbose logs; allow override via outer RUST_LOG
  ( RUST_LOG="${RUST_LOG:-kademlia=debug}" "${BIN}" peer --bind 127.0.0.1:"${port}" --k "${K}" --alpha "${ALPHA}" \
      >"${log}" 2>&1 ) &
  PEER_PIDS+=($!)
  SEED_ADDRS+=("127.0.0.1:${port}")
}

start_join_peer() {
  local idx="$1"
  local log="${LOG_DIR}/join-${idx}.log"
  echo "Starting join peer #${idx} (logs: ${log})"
  local bootstrap_args=()
  for a in "${SEED_ADDRS[@]}"; do
    bootstrap_args+=(--bootstrap "$a")
  done
  # Default to verbose logs; allow override via outer RUST_LOG
  ( RUST_LOG="${RUST_LOG:-kademlia=debug}" "${BIN}" peer --bind 127.0.0.1:0 "${bootstrap_args[@]}" --k "${K}" --alpha "${ALPHA}" \
      >"${log}" 2>&1 ) &
  PEER_PIDS+=($!)
}

cleanup() {
  echo "\nStopping peers ..."
  if [ ${#PEER_PIDS[@]} -gt 0 ]; then
    kill "${PEER_PIDS[@]}" 2>/dev/null || true
    wait "${PEER_PIDS[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# Start seeds
for ((i=0; i<NUM_SEEDS; i++)); do
  port=$((SEED_BASE_PORT + i))
  start_seed "${port}"
done

# Give seeds a brief moment
sleep 0.4

# Start joining peers
for ((i=1; i<=NUM_JOIN_PEERS; i++)); do
  start_join_peer "${i}"
done

echo "Waiting for peers to settle ..."
sleep 0.6

bootstrap_flags=()
for a in "${SEED_ADDRS[@]}"; do
  bootstrap_flags+=(--bootstrap "$a")
done

echo "\nPUT via client ..."
PUT_OUT=$("${BIN}" put --bind 127.0.0.1:0 "${bootstrap_flags[@]}" --value "${VALUE}" || true)
echo "${PUT_OUT}" | sed 's/^/  /'

# Extract key(hex)=... from Put output (robust against spacing)
# Example line: "key(hex)=900b16cb30..."
KEY_HEX=$(echo "${PUT_OUT}" | sed -n 's/.*key(hex)=//p' | head -n1 | tr -d '[:space:]')
if [[ -z "${KEY_HEX}" ]]; then
  echo "Failed to parse key from PUT output. Aborting." >&2
  exit 1
fi

echo "\nGET via client for key=${KEY_HEX} (retrying) ..."
FOUND=0
for attempt in {1..10}; do
  GET_OUT=$("${BIN}" get --bind 127.0.0.1:0 "${bootstrap_flags[@]}" "${KEY_HEX}" || true)
  echo "Attempt ${attempt}:"; echo "${GET_OUT}" | sed 's/^/  /'
  if echo "${GET_OUT}" | grep -q "^OK:"; then
    FOUND=1
    break
  fi
  sleep 0.5
done
if [[ "${FOUND}" -ne 1 ]]; then
  echo "\nValue not found after retries. Check peer logs in ${LOG_DIR}." >&2
fi

echo "\nDone. Logs in ${LOG_DIR}. Peers will be terminated now."

popd >/dev/null
