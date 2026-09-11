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

# Retries a command a few times with backoff, tolerating the propagation lag
# between a service account being created and it being usable in IAM policy
# bindings (describe can succeed before that propagation finishes).
retry() {
  local tries=8 count=0 wait=2
  until "$@"; do
    count=$((count + 1))
    if [[ "${count}" -ge "${tries}" ]]; then
      echo "    giving up after ${tries} attempts: $*" >&2
      return 1
    fi
    echo "    retrying in ${wait}s..."
    sleep "${wait}"
    wait=$((wait * 2))
  done
}

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

# A brand-new GCP project in the Broad Institute org has no "default" VPC network (unlike a
# project created elsewhere with the legacy default-network behavior), and
# worker VMs need one to get network connectivity.
echo "==> Ensuring a default VPC network exists..."
if gcloud compute networks describe default --project="${PROJECT}" >/dev/null 2>&1; then
  echo "    already exists"
else
  gcloud compute networks create default \
    --project="${PROJECT}" \
    --subnet-mode=auto \
    --bgp-routing-mode=regional
fi

# No inbound traffic is allowed to worker VMs except SSH via IAP tunneling
# (e.g. "SSH" in the Cloud Console, or `gcloud compute ssh --tunnel-through-iap`).
# Not required for batch jobs themselves to run -- outbound traffic (to
# Firestore/Pub/Sub/GCS/docker registries) is allowed by default regardless
# of firewall rules, and sparklespray's jobs are single-VM with no
# internal/inter-instance traffic -- this rule exists purely for debugging
# access. 35.235.240.0/20 is Google's fixed IAP TCP-forwarding source range.
echo "==> Ensuring IAP-tunneled SSH firewall rule exists..."
if gcloud compute firewall-rules describe allow-ssh-from-iap --project="${PROJECT}" >/dev/null 2>&1; then
  echo "    already exists"
else
  gcloud compute firewall-rules create allow-ssh-from-iap \
    --project="${PROJECT}" \
    --network=default \
    --direction=INGRESS \
    --action=ALLOW \
    --rules=tcp:22 \
    --source-ranges=35.235.240.0/20
fi

echo "==> Creating service account ${SA_EMAIL}..."
if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT}" >/dev/null 2>&1; then
  echo "    already exists"
else
  gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
    --project="${PROJECT}" \
    --display-name="sparklespray"
fi

# A freshly created service account isn't always immediately usable in
# IAM policy bindings elsewhere (eventual consistency) -- poll until it
# resolves before granting it anything.
echo "==> Waiting for ${SA_EMAIL} to become usable..."
for i in $(seq 1 30); do
  if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT}" >/dev/null 2>&1; then
    echo "    ready"
    break
  fi
  if [[ "${i}" -eq 30 ]]; then
    echo "    timed out waiting for ${SA_EMAIL} to become usable" >&2
    exit 1
  fi
  sleep 2
done

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
  retry gcloud projects add-iam-policy-binding "${PROJECT}" \
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
retry gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
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
    --bucket <bucket-name> \\
    --service-account ${SA_EMAIL} \\
    --admin-user <you>

This key file grants broad access to sparklespray's own resources in this
project -- store it somewhere safe, and delete/rotate it if it leaks.
EOF
