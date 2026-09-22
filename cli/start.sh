#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PROJECT="${PROJECT:-sprinkles-test-0625}"

echo "Building sprinkles..."
mkdir -p ./bin
go build -o ./bin/sprinkles ./cmd/sprinkles

echo "Loading config"
./bin/sprinkles dev set-config sample-config.json --project "${PROJECT}"

SPRINKLES="$(pwd)/bin/sprinkles"

cat > /tmp/mprocs-sprinkles.yaml <<EOF
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

# GOOGLE_APPLICATION_CREDENTIALS=$HOME/.sprinkles-cache/service-keys/ts-i28btmv9nw4jok.json go run . --project ts-i28btmv9nw4jok --subscriber-sa sprinkles-dashboard-user@depmap-portal-pipeline.iam.gserviceaccount.com
exec mprocs --config /tmp/mprocs-sprinkles.yaml
