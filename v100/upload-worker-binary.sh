#!/usr/bin/env bash
# Builds the sparkles binary (via build.sh) and uploads it to the fixed GCS
# path worker VMs bootstrap from -- see the "sparklesWorkerGCSPath" field in
# SparklesConfig/workpool specs (sample-config.json, sample-workpool.json)
# and monitor/batch_api.go, which `gcloud storage cp`s this exact path onto
# each worker VM at startup.
#
# Usage: upload-worker-binary.sh [--gcs-prefix <prefix>] [version] [gcs-path]
#   version defaults to `git describe --tags --always --dirty`.
#   gcs-path defaults to:
#     - the explicit [gcs-path] argument, if given
#     - "<prefix>/sparkles-linux-amd64-<version>", if --gcs-prefix was given
#       (e.g. the "gs://<bucket>/bin" prefix "sparkles dev bootstrap-project"
#       prints)
#     - $GCS_PATH, or the sample config's path, otherwise
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GCS_PREFIX=""
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --gcs-prefix) GCS_PREFIX="$2"; shift 2 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
set -- "${ARGS[@]+"${ARGS[@]}"}"

VERSION=${1:-$(git -C "${REPO_ROOT}" describe --tags --always --dirty)}
if [[ -n "${GCS_PREFIX}" ]]; then
  DEFAULT_GCS_PATH="${GCS_PREFIX%/}/sparkles-linux-amd64-${VERSION}"
else
  DEFAULT_GCS_PATH=${GCS_PATH:-gs://sparkles-test-0625/bin/sparkles-linux-amd64-${VERSION}}
fi
GCS_PATH=${2:-${DEFAULT_GCS_PATH}}

"${REPO_ROOT}/build.sh" "${VERSION}"

OUTPUT="${REPO_ROOT}/bin/sparkles-linux-amd64-${VERSION}"
echo "Uploading ${OUTPUT} to ${GCS_PATH}..."
gcloud storage cp "${OUTPUT}" "${GCS_PATH}"
