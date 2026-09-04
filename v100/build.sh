#!/usr/bin/env bash
# Builds the "sparkles" binary: worker + submit/kill CLI + serve (monitor +
# dashboard-backend, with the dashboard UI embedded) + dev subcommands, all
# in one self-contained linux/amd64 executable. The same binary bootstraps
# worker VMs (see monitor/batch_api.go) and is deployed as the control plane
# via "sparkles serve" -- see docs/deploying-dashboard-backend.md.
#
# Usage: build.sh [--force] [version]
#   --force   rebuild the frontend even if nothing under dashboard/ looks
#             newer than the last build (see BUILD_MARKER below).
#   version   defaults to `git describe --tags --always --dirty`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DASHBOARD_DIR="${REPO_ROOT}/dashboard"
EMBED_DIST="${REPO_ROOT}/v100/dev/webui/dist"
# Not inside EMBED_DIST: that directory gets wiped and recreated on every
# frontend build, which would erase the marker along with it.
BUILD_MARKER="${REPO_ROOT}/v100/dev/webui/.frontend-build-marker"

FORCE=0
if [ "${1:-}" = "--force" ]; then
  FORCE=1
  shift
fi

VERSION=${1:-$(git -C "${REPO_ROOT}" describe --tags --always --dirty)}
OUTPUT="${REPO_ROOT}/v100/bin/sparkles-linux-amd64-${VERSION}"

# Skip the frontend build if it's already embedded (EMBED_DIST/assets exists
# -- Vite always emits one) and nothing under dashboard/, other than
# node_modules/dist themselves, has changed since BUILD_MARKER was last
# touched. This is a plain mtime check, not a content hash: it can only ever
# cause an unnecessary rebuild (e.g. after `git checkout` resets mtimes),
# never skip a real change, so it's safe to keep simple.
skip_frontend_build=0
if [ "${FORCE}" -eq 0 ] && [ -f "${BUILD_MARKER}" ] && [ -d "${EMBED_DIST}/assets" ]; then
  changed=$(find "${DASHBOARD_DIR}" \
    \( -path "${DASHBOARD_DIR}/node_modules" -o -path "${DASHBOARD_DIR}/dist" \) -prune \
    -o -type f -newer "${BUILD_MARKER}" -print)
  if [ -z "${changed}" ]; then
    skip_frontend_build=1
  fi
fi

if [ "${skip_frontend_build}" -eq 1 ]; then
  echo "Frontend unchanged since last build; skipping (use --force to rebuild anyway)."
else
  echo "Building frontend..."
  ( cd "${DASHBOARD_DIR}" && npm ci && npm run build )

  echo "Copying frontend build into ${EMBED_DIST}..."
  rm -rf "${EMBED_DIST}"
  mkdir -p "${EMBED_DIST}"
  cp -r "${DASHBOARD_DIR}/dist/." "${EMBED_DIST}/"
  touch "${BUILD_MARKER}"

  echo
  echo "Note: this overwrote the tracked placeholder at"
  echo "  ${EMBED_DIST}/index.html"
  echo "with the real frontend build. Don't commit that change -- either leave"
  echo "it uncommitted or run:"
  echo "  git checkout -- v100/dev/webui/dist/index.html"
  echo
fi

echo "Building ${OUTPUT}..."
( cd "${REPO_ROOT}/v100" && \
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags "-X main.Version=${VERSION}" \
    -o "${OUTPUT}" \
    ./cmd/sparkles )

echo "Done: ${OUTPUT}"
