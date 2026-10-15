#!/usr/bin/env bash
# Stop the test-env SeaweedFS process started by start.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$ROOT/weed.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "not running (no $PID_FILE)"
  exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  kill "$PID" 2>/dev/null || true
  for i in $(seq 1 30); do
    if ! kill -0 "$PID" 2>/dev/null; then
      break
    fi
    sleep 0.2
  done
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
  fi
  echo "stopped pid=$PID"
else
  echo "stale pid file (process $PID not running)"
fi
rm -f "$PID_FILE"
