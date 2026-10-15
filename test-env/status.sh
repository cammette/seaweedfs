#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$ROOT/weed.pid"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "process: running pid=$(cat "$PID_FILE")"
else
  echo "process: not running"
fi

for name_url in "master:http://127.0.0.1:9333/cluster/status" "volume:http://127.0.0.1:8080/status" "filer:http://127.0.0.1:8888/"; do
  name="${name_url%%:*}"
  url="${name_url#*:}"
  if curl -sf -o /dev/null "$url"; then
    echo "$name: ok ($url)"
  else
    echo "$name: down ($url)"
  fi
done
