#!/bin/sh
set -eu

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
NO_CACHE="${NO_CACHE:-0}"

echo "Stopping and removing local stack from: ${COMPOSE_FILE}"
docker compose -f "${COMPOSE_FILE}" down

if [ "${NO_CACHE}" = "1" ]; then
  echo "Rebuilding images without cache"
  docker compose -f "${COMPOSE_FILE}" build --no-cache
else
  echo "Rebuilding images"
  docker compose -f "${COMPOSE_FILE}" build
fi

echo "Starting local stack"
docker compose -f "${COMPOSE_FILE}" up -d

echo "Done."
