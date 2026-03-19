# sparklesmonitor Cloud Function

A Google Cloud Function (2nd gen) that subscribes to a Pub/Sub topic and logs all incoming events for debugging.

## Overview

This function listens on a Pub/Sub topic and logs each message with:

- Message ID and publish time
- Decoded message data (base64 → plain text)
- Message attributes
- A structured `event_json` log line for easy querying in Cloud Logging

## Prerequisites

- Go 1.21+
- `gcloud` CLI authenticated with appropriate permissions
- A GCP project with Cloud Functions and Pub/Sub APIs enabled

## Local Testing

Install the functions framework if you haven't already:

```bash
go get github.com/GoogleCloudPlatform/functions-framework-go/funcframework
go mod tidy
```

Run the function locally:

```bash
cd cloudfunctions/sparklesmonitor
go run github.com/GoogleCloudPlatform/functions-framework-go/funcframework \
    --target HandlePubSub --port 8080
```

In a separate terminal, send a test CloudEvent:

```bash
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -H "ce-specversion: 1.0" \
  -H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
  -H "ce-source: //pubsub.googleapis.com/projects/my-project/topics/my-topic" \
  -H "ce-id: 1234" \
  -d '{
    "message": {
      "data": "aGVsbG8gd29ybGQ=",
      "messageId": "test-msg-1",
      "publishTime": "2024-01-01T00:00:00Z",
      "attributes": {"key": "value"}
    },
    "subscription": "projects/my-project/subscriptions/my-sub"
  }'
```

Note: `aGVsbG8gd29ybGQ=` is base64 for `hello world`.

You should see log output like:

```
=== Pub/Sub Event ===
Message ID:    test-msg-1
Publish Time:  2024-01-01 00:00:00 +0000 UTC
Subscription:  projects/my-project/subscriptions/my-sub
Data (raw):    hello world
Attributes:    map[key:value]
event_json: {"attributes":{"key":"value"},"data":"hello world",...}
```

## Deploying

```bash
cd cloudfunctions/sparklesmonitor
./deploy.sh
```

Override defaults with environment variables:

```bash
PROJECT_ID=my-project \
REGION=us-east1 \
TOPIC=my-topic \
FUNCTION_NAME=my-monitor \
./deploy.sh
```

| Variable        | Default                           | Description                   |
| --------------- | --------------------------------- | ----------------------------- |
| `PROJECT_ID`    | `gcloud config get-value project` | GCP project ID                |
| `REGION`        | `us-central1`                     | Cloud Functions region        |
| `TOPIC`         | `sparkles-events`                 | Pub/Sub topic to subscribe to |
| `FUNCTION_NAME` | `sparklesmonitor`                 | Deployed function name        |

## Verifying in GCP Console

1. Navigate to **Pub/Sub** → your topic → **Messages** tab
2. Click **Publish Message**, enter any message body, and click **Publish**
3. Navigate to **Cloud Logging** and filter:
   ```
   resource.type="cloud_run_revision"
   textPayload=~"event_json"
   ```
4. You should see the structured log entry with the decoded message data
