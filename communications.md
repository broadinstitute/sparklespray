# Sparklespray Pub/Sub Communications

All topic names use a configurable prefix (default: `sparkles`), set via `pubsub_topic_prefix` in the `.sparkles` config file or `--pubsubTopicPrefix` CLI flag.

Topics are created during cluster setup by `setup_pubsub_topics()` in `cli/sparklespray/gcp_setup.py`.

---

## Topic: `{prefix}-worker-inbound`

**Direction:** CLI → Worker

**Purpose:** The CLI sends requests to a specific worker. Each worker creates its own subscription named `{topic}-{workerId}` (e.g. `sparkles-worker-inbound-us-central1-a/my-vm`).

### Messages published by: Python CLI (`cli/sparklespray/pubsub_client.py`)

#### `read_output`

Request a chunk of a task's stdout/stderr output. Used for live log streaming.

```json
{
  "type": "read_output",
  "request_id": "<uuid>",
  "payload": {
    "task_id": "string",
    "size": "int32",
    "offset": "int64",
    "worker_id": "string"
  }
}
```

#### `get_process_status`

Request the worker's current CPU/memory stats.

```json
{
  "type": "get_process_status",
  "request_id": "<uuid>",
  "payload": {
    "worker_id": "string"
  }
}
```

---

## Topic: `{prefix}-worker-outbound`

**Direction:** Worker → CLI

**Purpose:** Workers publish two kinds of messages: responses to requests from the CLI, and unsolicited status events.

### Messages published by: Go Worker (`go/src/control/`)

#### Response: `read_output`

Reply to a `read_output` request. The CLI matches it to the original request via `request_id`.

```json
{
  "type": "read_output",
  "request_id": "<uuid>",
  "payload": {
    "data": "<base64-encoded bytes>",
    "end_of_file": "boolean"
  }
}
```

#### Response: `get_process_status`

Reply to a `get_process_status` request.

```json
{
  "type": "get_process_status",
  "request_id": "<uuid>",
  "payload": {
    "process_count": "int32",
    "total_memory": "int64",
    "total_data": "int64",
    "total_shared": "int64",
    "total_resident": "int64",
    "cpu_user": "int64",
    "cpu_system": "int64",
    "cpu_idle": "int64",
    "cpu_iowait": "int64",
    "mem_total": "int64",
    "mem_available": "int64",
    "mem_free": "int64",
    "mem_pressure_some_avg10": "int32",
    "mem_pressure_full_avg10": "int32"
  }
}
```

#### Event: `worker_started`

Published when the worker process starts up.

```json
{
  "type": "worker_started",
  "worker_id": "string",
  "timestamp": "int64 (milliseconds)"
}
```

#### Event: `worker_stopping`

Published just before the worker process exits.

```json
{
  "type": "worker_stopping",
  "worker_id": "string",
  "timestamp": "int64 (milliseconds)"
}
```

#### Event: `task_started`

Published when the worker begins executing a task.

```json
{
  "type": "task_started",
  "worker_id": "string",
  "timestamp": "int64 (milliseconds)",
  "task_id": "string"
}
```

#### Event: `task_completed`

Published when the worker finishes a task (success or failure).

```json
{
  "type": "task_completed",
  "worker_id": "string",
  "timestamp": "int64 (milliseconds)",
  "task_id": "string",
  "exit_code": "string",
  "error": "string (omitted if no error)"
}
```

### Subscribed by: Python CLI (`cli/sparklespray/pubsub_client.py`)

The CLI creates a short-lived subscription named `sparkles-cli-{random-8-char-hex}` per session. It uses `task_started` and `task_completed` events to trigger immediate task-status refreshes during `watch` (`cli/sparklespray/commands/watch.py`).

---

## Topic: `{prefix}-batchapi-out`

**Direction:** Google Cloud Batch API → System

**Purpose:** Google Cloud Batch notifies the system of job and task state changes. This topic is registered as a notification target when a Batch job is created.

### Messages published by: Google Cloud Batch API (external)

Batch publishes messages on two trigger conditions, both configured when submitting a job (Python: `cli/sparklespray/batch_api.py`; Go: `go/src/batch_api.go`):

- **`JOB_STATE_CHANGED`** — fired when the overall batch job transitions state (e.g. QUEUED → RUNNING → SUCCEEDED)
- **`TASK_STATE_CHANGED`** — fired when an individual task transitions state (e.g. PENDING → ASSIGNED → RUNNING → SUCCEEDED/FAILED)

The message envelope from Batch is a CloudEvent with a base64-encoded data field containing a protobuf payload with the job/task status update.

### Subscribed by: Cloud Function `sparklesmonitor` (`go/src/cloudfunctions/sparklesmonitor/function.go`)

Deployed as a CloudEvent-triggered Cloud Function. Currently receives and logs all Batch API notifications. The function logs the raw event data but does not yet act on it (e.g. to update Firestore task state).
