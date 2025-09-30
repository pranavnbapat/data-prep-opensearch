#!/bin/sh
set -eu

: "${TARGET_URL:=http://data-prep-opensearch:8000/run}"
: "${ENV_MODE:=PRD}"
: "${SPLAY:=0}"   # optional random delay in seconds to avoid thundering herd

# optional small random start delay
sleep "$SPLAY" || true

echo "Scheduler started. Posting to: $TARGET_URL  (env_mode=$ENV_MODE)  TZ=$TZ"

while true; do
  # sleep until the top of the next hour (UTC unless TZ set)
  now=$(date +%s)
  next=$(( ( (now / 3600) + 1 ) * 3600 ))
  sleep_seconds=$(( next - now ))
  sleep "$sleep_seconds"

  # fire the job
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") posting job…"
  curl -sS -H "Content-Type: application/json" -X POST \
    -d "{\"background\": true, \"env_mode\": \"${ENV_MODE}\"}" \
    "${TARGET_URL}" || echo "POST failed (will retry next hour)"
done
