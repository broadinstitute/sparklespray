#!/usr/bin/env bash
# Builds the "sparkles" binary: worker + submit/kill CLI + serve (monitor +
# dashboard-backend, with the dashboard UI embedded) + dev subcommands, all
# in one self-contained linux/amd64 executable. The same binary bootstraps
# worker VMs (see monitor/batch_api.go) and is deployed as the control plane
# via "sparkles serve" -- see docs/deploying-dashboard-backend.md.
#
# Usage: build.sh [version]
#   version defaults to `git describe --tags --always --dirty`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DASHBOARD_DIR="${REPO_ROOT}/dashboard"
EMBED_DIST="${REPO_ROOT}/v100/dev/webui/dist"

VERSION=${1:-$(git -C "${REPO_ROOT}" describe --tags --always --dirty)}
OUTPUT="${REPO_ROOT}/v100/bin/sparkles-linux-amd64-${VERSION}"

echo "Building frontend..."
( cd "${DASHBOARD_DIR}" && npm ci && npm run build )

echo "Copying frontend build into ${EMBED_DIST}..."
rm -rf "${EMBED_DIST}"
mkdir -p "${EMBED_DIST}"
cp -r "${DASHBOARD_DIR}/dist/." "${EMBED_DIST}/"

echo "Building ${OUTPUT}..."
( cd "${REPO_ROOT}/v100" && \
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags "-X main.Version=${VERSION}" \
    -o "${OUTPUT}" \
    ./cmd/sparkles )

echo "Done: ${OUTPUT}"
echo
echo "Note: this overwrote the tracked placeholder at"
echo "  ${EMBED_DIST}/index.html"
echo "with the real frontend build. Don't commit that change -- either leave"
echo "it uncommitted or run:"
echo "  git checkout -- v100/dev/webui/dist/index.html"
