#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PROJECT="${PROJECT:-sparkles-test-0625}"

echo "Building sparkles..."
mkdir -p ./bin
go build -o ./bin/sparkles ./cmd/sparkles

echo "Loading config"
./bin/sparkles dev set-config sample-config.json --project "${PROJECT}"

SPARKLES="$(pwd)/bin/sparkles"

cat > /tmp/mprocs-sparkles.yaml <<EOF
procs:
  # "serve" runs the monitor and the dashboard backend in one process, the
  # same way it's deployed. Run "dev monitor" / "dev dashboard-backend"
  # separately if you need to restart just one of them.
  serve:
    cmd: ["${SPARKLES}", "serve", "--project", "${PROJECT}", "--verbose"]
    log: "monitor.log"
  frontend:
    cmd: ["bash", "-c", "cd ../dashboard && npm run dev"]
  shell:
    cmd: ["bash"]
    stop: "SIGKILL"
EOF

# GOOGLE_APPLICATION_CREDENTIALS=$HOME/.sparkles-cache/service-keys/ts-i28btmv9nw4jok.json go run . --project ts-i28btmv9nw4jok --subscriber-sa sparkles-dashboard-user@depmap-portal-pipeline.iam.gserviceaccount.com
exec mprocs --config /tmp/mprocs-sparkles.yaml
