#!/usr/bin/env bash
# Start one single-node SeaweedFS cluster.
#   ./start.sh      cluster A (default)
#   ./start.sh b    cluster B, different ports, for blob migration tests
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
NAME="${1:-a}"

case "$NAME" in
  a|A)
    NAME=a
    MASTER_PORT=9333
    VOLUME_PORT=8080
    FILER_PORT=8888
    DATA_DIR="$ROOT/a/data"
    WORK_DIR="$ROOT/a"
    PID_FILE="$ROOT/a/weed.pid"
    LOG_FILE="$ROOT/a/weed.log"
    ;;
  b|B)
    NAME=b
    MASTER_PORT=9334
    VOLUME_PORT=8081
    FILER_PORT=9888
    DATA_DIR="$ROOT/b/data"
    WORK_DIR="$ROOT/b"
    PID_FILE="$ROOT/b/weed.pid"
    LOG_FILE="$ROOT/b/weed.log"
    ;;
  *)
    echo "usage: $0 [a|b]" >&2
    exit 1
    ;;
esac

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
  echo "cluster $NAME already running pid=$(cat "$PID_FILE")"
  echo "  master  http://127.0.0.1:${MASTER_PORT}"
  echo "  volume  http://127.0.0.1:${VOLUME_PORT}"
  echo "  filer   http://127.0.0.1:${FILER_PORT}"
  exit 0
fi

mkdir -p "$DATA_DIR/volume" "$DATA_DIR/master" "$DATA_DIR/filerldb2"

# filer.toml in WORK_DIR selects this cluster's leveldb2 directory.
cd "$WORK_DIR"
nohup "$WEED" server \
  -ip=127.0.0.1 \
  -ip.bind=127.0.0.1 \
  -dir="$DATA_DIR/volume" \
  -master.dir="$DATA_DIR/master" \
  -master.port="$MASTER_PORT" \
  -master.defaultReplication=000 \
  -master.volumeSizeLimitMB=100 \
  -master.telemetry=false \
  -volume.port="$VOLUME_PORT" \
  -volume.max=32 \
  -filer \
  -filer.port="$FILER_PORT" \
  -filer.maxMB=4 \
  >"$LOG_FILE" 2>&1 &

echo $! >"$PID_FILE"
echo "cluster $NAME started pid=$(cat "$PID_FILE") log=$LOG_FILE"

for _ in $(seq 1 60); do
  if curl -sf -o /dev/null "http://127.0.0.1:${FILER_PORT}/"; then
    echo "cluster $NAME ready:"
    echo "  master  http://127.0.0.1:${MASTER_PORT}"
    echo "  volume  http://127.0.0.1:${VOLUME_PORT}"
    echo "  filer   http://127.0.0.1:${FILER_PORT}"
    echo "  volume data: $DATA_DIR/volume"
    exit 0
  fi
  sleep 0.5
done

echo "cluster $NAME filer did not become ready; last log lines:" >&2
tail -n 40 "$LOG_FILE" >&2 || true
exit 1
