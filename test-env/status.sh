#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"

check() {
  local name="$1" pid_file="$2" master="$3" volume="$4" filer="$5"
  echo "== cluster $name =="
  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "process: running pid=$(cat "$pid_file")"
  else
    echo "process: not running"
  fi
  for item in "master:http://127.0.0.1:${master}/cluster/status" "volume:http://127.0.0.1:${volume}/status" "filer:http://127.0.0.1:${filer}/"; do
    local label="${item%%:*}"
    local url="${item#*:}"
    if curl -sf -o /dev/null "$url"; then
      echo "$label: ok ($url)"
    else
      echo "$label: down ($url)"
    fi
  done
}

check a "$ROOT/a/weed.pid" 9333 8080 8888
check b "$ROOT/b/weed.pid" 9334 8081 9888
