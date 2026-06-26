#!/usr/bin/env bash
set -euo pipefail

VERSION=${1:-dev}
OUTPUT="bin/sparkles-linux-amd64-${VERSION}"

echo "Building ${OUTPUT}..."

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags "-X main.Version=${VERSION}" \
    -o "${OUTPUT}" \
    ./cmd/sparkles

echo "Done: ${OUTPUT}"

echo "Uploading"
gcloud storage cp bin/sparkles-linux-amd64-dev gs://sparkles-test-0625/bin/sparkles-linux-amd64-dev
