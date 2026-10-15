#!/usr/bin/env bash
# Start a single-node SeaweedFS (master + volume + leveldb filer) under test-env/.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

PID_FILE="$ROOT/weed.pid"
LOG_FILE="$ROOT/weed.log"
DATA_DIR="$ROOT/data"

# Prefer an explicit binary, then a binary built into test-env/, then PATH.
if [[ -n "${WEED_BIN:-}" ]]; then
  WEED="$WEED_BIN"
elif [[ -x "$ROOT/weed" ]]; then
  WEED="$ROOT/weed"
elif command -v weed >/dev/null 2>&1; then
  WEED="$(command -v weed)"
else
  echo "building weed into $ROOT/weed ..."
  (cd "$ROOT/.." && go build -o "$ROOT/weed" ./weed)
  WEED="$ROOT/weed"
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "already running pid=$(cat "$PID_FILE")"
  echo "  master  http://127.0.0.1:9333"
  echo "  volume  http://127.0.0.1:8080"
  echo "  filer   http://127.0.0.1:8888"
  exit 0
fi

mkdir -p "$DATA_DIR/volume" "$DATA_DIR/master" "$DATA_DIR/filerldb2"

# filer.toml in cwd enables leveldb2 at ./data/filerldb2
nohup "$WEED" server \
  -ip=127.0.0.1 \
  -ip.bind=127.0.0.1 \
  -dir="$DATA_DIR/volume" \
  -master.dir="$DATA_DIR/master" \
  -master.port=9333 \
  -master.defaultReplication=000 \
  -master.volumeSizeLimitMB=100 \
  -master.telemetry=false \
  -volume.port=8080 \
  -volume.max=32 \
  -filer \
  -filer.port=8888 \
  -filer.maxMB=4 \
  >"$LOG_FILE" 2>&1 &

echo $! >"$PID_FILE"
echo "started pid=$(cat "$PID_FILE") log=$LOG_FILE"

# Wait until filer answers
for i in $(seq 1 60); do
  if curl -sf -o /dev/null "http://127.0.0.1:8888/"; then
    echo "ready:"
    echo "  master  http://127.0.0.1:9333"
    echo "  volume  http://127.0.0.1:8080"
    echo "  filer   http://127.0.0.1:8888"
    echo "  volume data: $DATA_DIR/volume"
    echo "  filer store: $DATA_DIR/filerldb2 (leveldb2)"
    exit 0
  fi
  sleep 0.5
done

echo "filer did not become ready; last log lines:" >&2
tail -n 40 "$LOG_FILE" >&2 || true
exit 1
