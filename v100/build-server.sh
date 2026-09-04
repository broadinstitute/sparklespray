#!/usr/bin/env bash
# Builds a self-contained "sparkles" binary with the dashboard frontend
# embedded, for deploying the control plane (monitor + dashboard-backend +
# UI) to a remote server as a single artifact. See
# v100/docs/deploying-dashboard-backend.md.
set -euo pipefail

VERSION=${1:-dev}
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DASHBOARD_DIR="${REPO_ROOT}/dashboard"
EMBED_DIST="${REPO_ROOT}/v100/dev/webui/dist"
OUTPUT="${REPO_ROOT}/v100/bin/sparkles-server-linux-amd64-${VERSION}"

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
