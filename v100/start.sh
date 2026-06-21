#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PROJECT="${PROJECT:-local-dev}"
FIRESTORE_PORT="${FIRESTORE_PORT:-8791}"
PUBSUB_PORT="${PUBSUB_PORT:-8794}"
BATCHAPI_PORT="${BATCHAPI_PORT:-8799}"

echo "Building sparkles..."
mkdir -p ./bin
go build -o ./bin/sparkles ./cmd/sparkles

SPARKLES="$(pwd)/bin/sparkles"

cat > /tmp/mprocs-sparkles.yaml <<EOF
procs:
  firestore:
    cmd: ["gcloud", "emulators", "firestore", "start", "--host-port=localhost:${FIRESTORE_PORT}"]
  pubsub:
    cmd: ["gcloud",  "beta", "emulators", "pubsub", "start", "--host-port=localhost:${PUBSUB_PORT}"]
  batchapi-emulator:
    cmd: ["${SPARKLES}", "dev", "batchapi-emulator", "--addr", ":${BATCHAPI_PORT}"]
  autoscaler:
    cmd: ["${SPARKLES}", "autoscale", "--project", "${PROJECT}", "--verbose"]
    env:
      FIRESTORE_EMULATOR_HOST: "localhost:${FIRESTORE_PORT}"
      PUBSUB_EMULATOR_HOST: "localhost:${PUBSUB_PORT}"
      SPARKLES_BATCH_API_EMULATOR: "http://localhost:${BATCHAPI_PORT}"
  shell:
    cmd: ["bash"]
    env:
      FIRESTORE_EMULATOR_HOST: "localhost:${FIRESTORE_PORT}"
      PUBSUB_EMULATOR_HOST: "localhost:${PUBSUB_PORT}"
      SPARKLES_BATCH_API_EMULATOR: "http://localhost:${BATCHAPI_PORT}"
EOF

exec mprocs --config /tmp/mprocs-sparkles.yaml
