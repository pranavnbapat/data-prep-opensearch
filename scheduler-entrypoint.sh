#!/bin/sh
set -eu

: "${API_BASE_URL:=http://data-prep-opensearch:8000}"
: "${HEALTH_URL:=${API_BASE_URL}/healthz}"
: "${TZ:=UTC}"
: "${SPLAY:=0}"
: "${SCHEDULER_POLL_SECONDS:=20}"

STATE_DIR="/tmp/data-prep-scheduler"
mkdir -p "$STATE_DIR"

sleep "$SPLAY" || true

echo "Scheduler started. api_base=$API_BASE_URL tz=$TZ poll=${SCHEDULER_POLL_SECONDS}s"

until curl -fsS "$HEALTH_URL" >/dev/null 2>&1; do
  echo "Waiting for app healthz..."
  sleep 5
done

post_job() {
  job_name="$1"
  endpoint="$2"
  payload="$3"

  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") posting ${job_name} -> ${endpoint}"

  if ! curl -fsS \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -X POST \
    -d "$payload" \
    "${API_BASE_URL}${endpoint}"; then
    echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ${job_name} POST failed"
    return 1
  fi

  echo
  return 0
}

run_if_due() {
  job_name="$1"
  day_filter="$2"
  hour="$3"
  minute="$4"
  endpoint="$5"
  payload="$6"

  now_day="$(date +%u)"
  now_hm="$(date +%H:%M)"
  today="$(date +%F)"
  state_file="${STATE_DIR}/${job_name}.last"
  last_run=""

  if [ -f "$state_file" ]; then
    last_run="$(cat "$state_file" 2>/dev/null || true)"
  fi

  if [ "$day_filter" != "*" ] && [ "$now_day" != "$day_filter" ]; then
    return 0
  fi

  if [ "$now_hm" != "${hour}:${minute}" ]; then
    return 0
  fi

  if [ "$last_run" = "$today" ]; then
    return 0
  fi

  if post_job "$job_name" "$endpoint" "$payload"; then
    printf '%s' "$today" > "$state_file"
  fi
}

# Daily KO chain (times are in the scheduler's TZ; .env sets Europe/Amsterdam).
# Spacing leaves room for a long-running job to finish
# before the next fires, and the chain completes well before data-ingest's
# 09:30 index build (which consumes the 08:00 final-improved export):
#   02:00 backend-core sync -> 04:00 translations -> 05:30 fast pipeline
#   -> 08:00 final-improved export -> (09:30 data-ingest build_index)
# pipeline_deferred stays on its weekly Friday-night slot; projects_export is
# decoupled (separate JSON snapshot) and parked at noon, clear of everything.
while true; do
  run_if_due \
    "sync_backend_core" "*" "02" "00" "/sync/backend-core" \
    '{"page_size":0,"env_mode":"PRD","sort_criteria":1,"dl_workers":0}'

  run_if_due \
    "sync_translations" "*" "04" "00" "/sync/translations" \
    '{"env_mode":"PRD","only_missing":false}'

  run_if_due \
    "pipeline_fast" "*" "05" "30" "/pipeline/fast" \
    '{"env_mode":"PRD"}'

  run_if_due \
    "export_final_improved" "*" "08" "00" "/exports/final-improved" \
    '{"env_mode":"PRD","processed_only":false,"eligible_only":true}'

  run_if_due \
    "projects_export" "*" "12" "00" "/projects/export" \
    '{"env_mode":"PRD","page_size":200}'

  run_if_due \
    "pipeline_deferred" "5" "22" "00" "/pipeline/deferred" \
    '{"env_mode":"PRD"}'

  sleep "$SCHEDULER_POLL_SECONDS"
done
