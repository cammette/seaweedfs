#!/usr/bin/env bash
# Stop cluster A, cluster B, or both.
#   ./stop.sh      both
#   ./stop.sh a
#   ./stop.sh b
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
NAME="${1:-all}"

stop_one() {
  local pid_file="$1"
  if [[ ! -f "$pid_file" ]]; then
    echo "not running (no $pid_file)"
    return 0
  fi
  local pid
  pid="$(cat "$pid_file")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    echo "stopped pid=$pid"
  else
    echo "stale pid file (process $pid not running)"
  fi
  rm -f "$pid_file"
}

case "$NAME" in
  a|A)
    stop_one "$ROOT/a/weed.pid"
    stop_one "$ROOT/weed.pid"
    ;;
  b|B) stop_one "$ROOT/b/weed.pid" ;;
  all|"")
    stop_one "$ROOT/a/weed.pid"
    stop_one "$ROOT/weed.pid"
    stop_one "$ROOT/b/weed.pid"
    ;;
  *)
    echo "usage: $0 [a|b]" >&2
    exit 1
    ;;
esac
