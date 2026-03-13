#!/bin/sh
set -eu

REGISTRY="${REGISTRY:-ghcr.io/pranavnbapat}"
APP_IMAGE="${APP_IMAGE:-$REGISTRY/data-prep-opensearch}"
SCHEDULER_IMAGE="${SCHEDULER_IMAGE:-$REGISTRY/data-prep-opensearch-scheduler}"
TAG="${1:-latest}"

echo "Building app image: ${APP_IMAGE}:${TAG}"
docker build -t "${APP_IMAGE}:${TAG}" .

echo "Pushing app image: ${APP_IMAGE}:${TAG}"
docker push "${APP_IMAGE}:${TAG}"

echo "Building scheduler image: ${SCHEDULER_IMAGE}:${TAG}"
docker build -f Dockerfile.scheduler -t "${SCHEDULER_IMAGE}:${TAG}" .

echo "Pushing scheduler image: ${SCHEDULER_IMAGE}:${TAG}"
docker push "${SCHEDULER_IMAGE}:${TAG}"

echo "Done."
echo "App image: ${APP_IMAGE}:${TAG}"
echo "Scheduler image: ${SCHEDULER_IMAGE}:${TAG}"
