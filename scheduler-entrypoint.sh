#!/bin/sh
set -eu

: "${TARGET_URL:=http://data-prep-opensearch:8000/run_pipeline}"
: "${ENV_MODE:=PRD}"
: "${TZ:=UTC}"    # avoid unbound var in echo under set -u
: "${SPLAY:=0}"   # optional random delay in seconds to avoid thundering herd

# optional small random start delay
sleep "$SPLAY" || true

echo "Scheduler started. Posting to: $TARGET_URL  (env_mode=$ENV_MODE)  TZ=$TZ"

# Wait until the target endpoint responds (simple readiness gate)
# This avoids missing the first day's run if the app isn't up yet.
until curl -fsS "${TARGET_URL%/run_pipeline}/healthz" >/dev/null 2>&1; do
  echo "Waiting for app healthz..."
  sleep 5
done

while true; do
  # sleep until the next 23:00 in the container's TZ
  # (TZ is controlled by the TZ env var; see docker-compose.yml)
  h=$(date +%H)
  m=$(date +%M)
  s=$(date +%S)

  # seconds since local midnight; use 10# to avoid octal interpretation on leading zeros
  since_midnight=$(( 10#$h*3600 + 10#$m*60 + 10#$s ))

  target=82800            # 23 * 3600
  day=86400

  if [ "$since_midnight" -lt "$target" ]; then
    sleep_seconds=$(( target - since_midnight ))
  else
    sleep_seconds=$(( day - since_midnight + target ))
  fi

  sleep "$sleep_seconds"

  # fire the job
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") posting job ^` "

  curl -sS -H "Content-Type: application/json" -X POST \
    -d "{\"background\": true, \"env_mode\": \"${ENV_MODE}\"}" \
    "${TARGET_URL}" || echo "POST failed (will try again tomorrow)"

  # IMPORTANT: do NOT sleep 86400 here; the loop recalculates next 23:00
done