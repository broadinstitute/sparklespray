#!/usr/bin/env bash
# One-time, broad-access bootstrap for a fresh GCP project: enables every API
# sparklespray depends on, creates a single service account, grants it every
# IAM role the rest of setup and the running app will ever need, and mints a
# key for it. Run this with an identity that has broad access (e.g. Project
# Owner/Editor) -- everything after this should run as the created service
# account with its intentionally-narrower permissions (see
# `sparkles dev bootstrap-project`, which does the rest: Firestore database +
# indexes, GCS bucket, Pub/Sub topics, SparklesConfig, and an initial API key).
#
# Usage: setup-gcp-project.sh --project <id> [--service-account-name <name>] [--key-file <path>]
set -euo pipefail

SERVICE_ACCOUNT_NAME="sparkles"
KEY_FILE="./sparkles-sa-key.json"
PROJECT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --service-account-name) SERVICE_ACCOUNT_NAME="$2"; shift 2 ;;
    --key-file) KEY_FILE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${PROJECT}" ]]; then
  echo "usage: setup-gcp-project.sh --project <id> [--service-account-name <name>] [--key-file <path>]" >&2
  exit 1
fi

SA_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT}.iam.gserviceaccount.com"

echo "==> Enabling required APIs on ${PROJECT}..."
gcloud services enable \
  firestore.googleapis.com \
  pubsub.googleapis.com \
  batch.googleapis.com \
  compute.googleapis.com \
  logging.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  --project="${PROJECT}"

echo "==> Creating service account ${SA_EMAIL}..."
if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT}" >/dev/null 2>&1; then
  echo "    already exists"
else
  gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
    --project="${PROJECT}" \
    --display-name="sparklespray"
fi

# Union of every runtime permission traced across the codebase (Firestore,
# Pub/Sub, GCP Batch, Compute, Cloud Logging, GCS) plus what "dev
# bootstrap-project" needs to set the rest of this up itself (Firestore
# database/index creation, bucket creation, topic creation).
echo "==> Granting project-level IAM roles to ${SA_EMAIL}..."
ROLES=(
  roles/datastore.owner
  roles/pubsub.editor
  roles/batch.jobsEditor
  roles/compute.instanceAdmin.v1
  roles/logging.viewer
  roles/storage.admin
)
for ROLE in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${ROLE}" \
    --condition=None \
    >/dev/null
  echo "    granted ${ROLE}"
done

# Self-grant: generateSubscriberToken (dashboard_backend.go) mints short-lived
# Pub/Sub tokens by impersonating SparklesConfig.subscriber_sa. In this
# single-SA setup that's this same service account, so it needs
# serviceAccountTokenCreator on itself.
echo "==> Granting roles/iam.serviceAccountTokenCreator on ${SA_EMAIL} to itself..."
gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --project="${PROJECT}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/iam.serviceAccountTokenCreator" \
  >/dev/null

echo "==> Creating key for ${SA_EMAIL} at ${KEY_FILE}..."
gcloud iam service-accounts keys create "${KEY_FILE}" \
  --iam-account="${SA_EMAIL}" \
  --project="${PROJECT}"

cat <<EOF

Done. Next steps:

  export GOOGLE_APPLICATION_CREDENTIALS="$(cd "$(dirname "${KEY_FILE}")" && pwd)/$(basename "${KEY_FILE}")"
  sparkles dev bootstrap-project \\
    --project ${PROJECT} \\
    --region <gcp-region> \\
    --zones <zone-a> --zones <zone-b> \\
    --bucket <bucket-name> \\
    --service-account ${SA_EMAIL} \\
    --admin-user <you>

This key file grants broad access to sparklespray's own resources in this
project -- store it somewhere safe, and delete/rotate it if it leaks.
EOF
