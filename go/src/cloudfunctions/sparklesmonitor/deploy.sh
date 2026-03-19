#!/bin/bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
TOPIC="${TOPIC:-sparkles-events}"
FUNCTION_NAME="${FUNCTION_NAME:-sparklesmonitor}"

gcloud functions deploy "$FUNCTION_NAME" \
    --gen2 \
    --runtime=go121 \
    --region="$REGION" \
    --source=. \
    --entry-point=HandlePubSub \
    --trigger-topic="$TOPIC" \
    --project="$PROJECT_ID"
