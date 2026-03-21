#!/bin/sh
set -eu

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

echo "Pulling latest images from compose file: ${COMPOSE_FILE}"
docker compose -f "${COMPOSE_FILE}" pull

echo "Recreating services with freshly pulled images"
docker compose -f "${COMPOSE_FILE}" up -d --force-recreate --remove-orphans

echo "Done."
