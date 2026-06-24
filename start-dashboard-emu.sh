#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "${REPO_ROOT}/v100"

PROJECT="${PROJECT:-local-dev}"
FIRESTORE_PORT="${FIRESTORE_PORT:-8791}"
PUBSUB_PORT="${PUBSUB_PORT:-8794}"
# Must match the proxy target in dashboard/vite.config.ts
BACKEND_PORT="${BACKEND_PORT:-8080}"

echo "Building sparkles..."
mkdir -p ./bin
go build -o ./bin/sparkles ./cmd/sparkles

SPARKLES="$(pwd)/bin/sparkles"
DASHBOARD_DIR="${REPO_ROOT}/dashboard"

cat > /tmp/mprocs-dashboard-emu.yaml <<EOF
procs:
  firestore:
    cmd: ["gcloud", "emulators", "firestore", "start", "--host-port=localhost:${FIRESTORE_PORT}"]
  pubsub:
    cmd: ["gcloud", "beta", "emulators", "pubsub", "start", "--host-port=localhost:${PUBSUB_PORT}"]
  backend:
    cmd: ["${SPARKLES}", "dev", "dashboard-backend", "--project", "${PROJECT}", "--addr", ":${BACKEND_PORT}"]
    env:
      FIRESTORE_EMULATOR_HOST: "localhost:${FIRESTORE_PORT}"
      PUBSUB_EMULATOR_HOST: "localhost:${PUBSUB_PORT}"
  frontend:
    cmd: ["npm", "run", "dev"]
    cwd: "${DASHBOARD_DIR}"
  simulator:
    cmd: ["${SPARKLES}", "dev", "simulate", "--project", "${PROJECT}"]
    env:
      FIRESTORE_EMULATOR_HOST: "localhost:${FIRESTORE_PORT}"
      PUBSUB_EMULATOR_HOST: "localhost:${PUBSUB_PORT}"
EOF

exec mprocs --config /tmp/mprocs-dashboard-emu.yaml
