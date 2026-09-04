#!/usr/bin/env bash
# Builds the sparkles binary (via build.sh) and uploads it to the fixed GCS
# path worker VMs bootstrap from -- see the "sparklesWorkerGCSPath" field in
# SparklesConfig/workpool specs (sample-config.json, sample-workpool.json)
# and monitor/batch_api.go, which `gcloud storage cp`s this exact path onto
# each worker VM at startup.
#
# Usage: upload-worker-binary.sh [version] [gcs-path]
#   version defaults to `git describe --tags --always --dirty`.
#   gcs-path defaults to $GCS_PATH, or the sample config's path if unset.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VERSION=${1:-$(git -C "${REPO_ROOT}" describe --tags --always --dirty)}
GCS_PATH=${2:-${GCS_PATH:-gs://sparkles-test-0625/bin/sparkles-linux-amd64-dev}}

"${REPO_ROOT}/build.sh" "${VERSION}"

OUTPUT="${REPO_ROOT}/bin/sparkles-linux-amd64-${VERSION}"
echo "Uploading ${OUTPUT} to ${GCS_PATH}..."
gcloud storage cp "${OUTPUT}" "${GCS_PATH}"
