#!/usr/bin/env bash
# One-command verification for local development:
#   build → vet → unit tests → start server on a temp port → acceptance replay
# Fails loudly if anything is red. Read this output, not just the exit code:
# the replay prints every assertion it made.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${PORT:-18099}"
BASE="http://127.0.0.1:$PORT"
LOG="$(mktemp)"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -f "$LOG"
}
trap cleanup EXIT

echo "== go build =="
go build ./...
go vet ./...

echo "== unit tests =="
go test ./... -count=1

echo "== start server on $PORT =="
go build -o /tmp/agrisync-verify ./cmd/server
HTTP_ADDR=":$PORT" /tmp/agrisync-verify >"$LOG" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 60); do
  if curl -sf "$BASE/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.5
done
if ! curl -sf "$BASE/healthz" >/dev/null 2>&1; then
  echo "server failed to start; log:" >&2
  cat "$LOG" >&2
  exit 1
fi

echo "== acceptance replay (cross-region sync, DR, delivery audit) =="
./scripts/replay.sh "$BASE"

echo "== acceptance replay (field operation sequence integrity) =="
./scripts/sequence_replay.sh "$BASE"
