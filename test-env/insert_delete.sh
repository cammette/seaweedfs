#!/usr/bin/env bash
# Insert or delete src_demo files on cluster A (filer http://127.0.0.1:8888).
# The file list is kept in blob_sync/inserted.list for a later delete.
#
#   ./insert_delete.sh insert [count]   default 100
#   ./insert_delete.sh delete [count]   default: every path in the list
#   ./insert_delete.sh both [count]     insert count, then delete half of them
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

FILER="${FILER:-http://127.0.0.1:8888}"
LIST="${LIST:-$ROOT/blob_sync/inserted.list}"
ACTION="${1:-}"
COUNT="${2:-}"

usage() {
  echo "usage: $0 insert [count] | delete [count] | both [count]" >&2
  echo "  insert  upload count files (default 100) into collection src_demo" >&2
  echo "  delete  delete count paths from the last insert list (default all)" >&2
  echo "  both    insert count files, then delete half of them" >&2
  exit 1
}

need_count() {
  local n="${1:-100}"
  if ! [[ "$n" =~ ^[0-9]+$ ]] || [[ "$n" -lt 1 ]]; then
    echo "count must be a positive integer, got ${1:-}" >&2
    usage
  fi
  echo "$n"
}

insert_files() {
  local n="$1"
  local batch path body i
  batch="$(date +%s)"
  mkdir -p "$(dirname "$LIST")"
  : >"$LIST"
  echo "inserting ${n} files into ${FILER}/src_demo/mutate/${batch}/ (collection=src_demo)"
  for ((i = 1; i <= n; i++)); do
    path="/src_demo/mutate/${batch}/file_${i}.txt"
    body="mutate-${batch}-${i}-$(head -c 32 /dev/urandom | base64 | tr -d '\n')"
    printf '%s' "$body" | curl -sf -X PUT --data-binary @- \
      -H "Content-Type: text/plain" \
      "${FILER}${path}?collection=src_demo" >/dev/null
    echo "$path" >>"$LIST"
    if ((i % 100 == 0 || i == n)); then
      echo "  inserted ${i}/${n}"
    fi
  done
  echo "list: $LIST"
}

delete_files() {
  local n="${1:-}"
  local total i path
  if [[ ! -s "$LIST" ]]; then
    echo "no insert list at $LIST; run insert first" >&2
    exit 1
  fi
  total="$(grep -c . "$LIST" || true)"
  if [[ -z "$n" ]]; then
    n="$total"
  fi
  if ! [[ "$n" =~ ^[0-9]+$ ]] || [[ "$n" -lt 1 ]]; then
    echo "count must be a positive integer, got $n" >&2
    usage
  fi
  if ((n > total)); then
    n="$total"
  fi
  echo "deleting ${n} of ${total} files via ${FILER}"
  i=0
  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    ((i++)) || true
    if ((i > n)); then
      break
    fi
    curl -sf -X DELETE "${FILER}${path}" >/dev/null
    if ((i % 100 == 0 || i == n)); then
      echo "  deleted ${i}/${n}"
    fi
  done <"$LIST"
  tail -n +"$((n + 1))" "$LIST" >"${LIST}.tmp" || true
  mv "${LIST}.tmp" "$LIST"
  echo "remaining in list: $(grep -c . "$LIST" || true)"
}

case "$ACTION" in
  insert)
    insert_files "$(need_count "${COUNT:-100}")"
    ;;
  delete)
    delete_files "$COUNT"
    ;;
  both)
    n="$(need_count "${COUNT:-100}")"
    insert_files "$n"
    half=$((n / 2))
    if ((half < 1)); then
      half=1
    fi
    delete_files "$half"
    ;;
  *)
    usage
    ;;
esac
