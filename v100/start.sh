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

# Write a small setup script that waits for the Pub/Sub emulator to accept
# connections, then creates the topic and subscription the autoscaler needs.
cat > /tmp/sparkles-pubsub-setup.sh <<SETUP
#!/usr/bin/env bash
echo "pubsub-setup: waiting for emulator on port ${PUBSUB_PORT}..."
until curl -sf "http://localhost:${PUBSUB_PORT}" > /dev/null 2>&1; do sleep 0.5; done
echo "pubsub-setup: creating topic and subscription autoscaler-in"
PUBSUB_EMULATOR_HOST="localhost:${PUBSUB_PORT}" \\
  gcloud beta pubsub topics create autoscaler-in --project="${PROJECT}" 2>/dev/null || true
PUBSUB_EMULATOR_HOST="localhost:${PUBSUB_PORT}" \\
  gcloud beta pubsub subscriptions create autoscaler-in \\
    --topic=autoscaler-in --project="${PROJECT}" 2>/dev/null || true
echo "pubsub-setup: done"
SETUP
chmod +x /tmp/sparkles-pubsub-setup.sh

cat > /tmp/mprocs-sparkles.yaml <<EOF
procs:
  firestore:
    cmd: ["gcloud", "emulators", "firestore", "start", "--host-port=localhost:${FIRESTORE_PORT}"]
  pubsub:
    cmd: ["gcloud",  "beta", "emulators", "pubsub", "start", "--host-port=localhost:${PUBSUB_PORT}"]
  pubsub-setup:
    cmd: ["/tmp/sparkles-pubsub-setup.sh"]
  batchapi-emulator:
    cmd: ["${SPARKLES}", "dev", "batchapi-emulator", "--addr", ":${BATCHAPI_PORT}"]
  autoscaler:
    cmd: ["${SPARKLES}", "autoscale", "--project", "${PROJECT}"]
    env:
      FIRESTORE_EMULATOR_HOST: "localhost:${FIRESTORE_PORT}"
      PUBSUB_EMULATOR_HOST: "localhost:${PUBSUB_PORT}"
      SPARKLES_BATCH_API_EMULATOR: "http://localhost:${BATCHAPI_PORT}"
EOF

exec mprocs --config /tmp/mprocs-sparkles.yaml
