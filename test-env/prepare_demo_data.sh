#!/usr/bin/env bash
# Seed source collection src_demo on the local single-node cluster.
# Target migration will write into collection dst_demo (different volume files).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

FILER="${FILER:-http://127.0.0.1:8888}"
COUNT="${1:-1000}"

if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [[ "$COUNT" -lt 1 ]]; then
  echo "usage: $0 [count]" >&2
  echo "  count  positive integer, default 1000" >&2
  exit 1
fi

./status.sh >/dev/null || { echo "start cluster first: ./start.sh"; exit 1; }

BATCH="$(date +%s)"
echo "seeding ${COUNT} files into ${FILER}/src_demo/seed/${BATCH}/ (collection=src_demo) ..."
for ((i = 1; i <= COUNT; i++)); do
  body="demo-seed-$i-$(date +%s)-$(head -c 64 /dev/urandom | base64 | tr -d '\n')"
  printf '%s' "$body" | curl -sf -X PUT --data-binary @- \
    -H "Content-Type: text/plain" \
    "${FILER}/src_demo/seed/${BATCH}/file_${i}.txt?collection=src_demo" >/dev/null
  if ((i % 100 == 0 || i == COUNT)); then
    echo "  uploaded ${i}/${COUNT}"
  fi
done

echo
echo "source volume files (collection src_demo):"
ls -la a/data/volume/src_demo_*.dat a/data/volume/src_demo_*.idx 2>/dev/null | sed 's/^/  /'

echo
echo "idx entries:"
for f in a/data/volume/src_demo_*.idx; do
  [[ -f "$f" ]] || continue
  vid=$(basename "$f" .idx | sed 's/src_demo_//')
  sz=$(stat -c%s "$f")
  echo "  volume $vid: $((sz / 16)) idx rows"
done

echo
echo "ready. See COMMANDS.txt for migrate / verify / status."
