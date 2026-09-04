# Sparklespray: Pub/Sub Data Model

> **Historical:** this document describes the pre-v100 (`SparklesV6`) design.
> The Python CLI (`cli/sparklespray/`), `simulator/main.go`, and
> `dashboard-backend/main.go` referenced below have all since been deleted
> and superseded by the Go rewrite under `v100/`. See `v100/datamodel.md` and
> `v100/docs/arc42/` for the current Pub/Sub topics and data model.

Sparklespray uses three Pub/Sub topics. All topic names are versioned with the `v6` suffix to allow in-place schema migrations.

**Services referenced (historical, since deleted/replaced by `v100/`):**

- **CLI** — Python CLI (`cli/sparklespray/`)
- **Worker** — Go worker binary (`src/sparklesworker/`) and its simulator (`simulator/main.go`)
- **Dashboard** — Go dashboard backend (`dashboard-backend/main.go`)

---

## Topic: `sparkles-v6-events` — Lifecycle Notifications

**Direction:** CLI and Worker → Dashboard (and any other consumers)

**Purpose:** Notify subscribers that a lifecycle event has been appended to `SparklesV6Event` in Firestore. The message body is intentionally empty — it is a wake-up signal only. Subscribers fetch the actual event data from Firestore.

The `type` attribute enables server-side Pub/Sub filter expressions so subscribers can receive only the event types they care about (e.g. `attributes.type = "job_started" OR attributes.type = "task_claimed"`).

### Publishers

| Service | Events published                                                                                                                                                                        |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Worker  | All lifecycle events: `cluster_started`, `worker_started`, `worker_stopped`, `task_claimed`, `task_exec_started`, `task_exec_complete`, `task_complete`, `task_failed`, `task_orphaned` |
| CLI     | `job_started` (when a job is submitted)                                                                                                                                                 |

### Subscribers

| Service   | How                                                                                                                                                   |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dashboard | Creates on-demand subscriptions via its REST API; the subscription is given to the browser client, which pulls directly from the GCP Pub/Sub REST API |

### Message format

- **Data:** empty (zero bytes)
- **Attributes:**
  - `type` — event type string, e.g. `task_complete`

One message is published per event written to `SparklesV6Event`.

---

## Topic: `sparkles-v6-task-out` — Task Metrics and Log Output

**Direction:** Worker → Dashboard

**Purpose:** Real-time stream of resource metrics and stdout/stderr from an actively running task. Unlike lifecycle events, these messages carry a full JSON body. Volume can be high (one metric sample every ~10 seconds per running task), so subscriptions are created on demand — only when a client is actively viewing a specific task.

### Publishers

| Service | Messages published                           |
| ------- | -------------------------------------------- |
| Worker  | `metric_update`, `log_update`, `command_ack` |

### Subscribers

| Service   | How                                                                                                                                                 |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dashboard | Creates an on-demand subscription filtered to `attributes.task_id = "<task_id>"` via its REST API; the subscription is handed to the browser client |

### Message attributes (all messages)

- `type` — message type string
- `task_id` — the task this message belongs to

---

### Message: `metric_update`

Published approximately every 10 seconds while a task is executing.

**Attributes:** `type=metric_update`, `task_id=<task_id>`

**Data:** JSON body

```json
{
  "type": "metric_update",
  "task_id": "<task_id>",
  "timestamp": "<RFC3339>",
  "process_count": 4,
  "volumes": [
    { "location": "/mnt/disk1", "total_gb": 100.0, "used_gb": 42.3 },
    { "location": "/", "total_gb": 50.0, "used_gb": 18.1 }
  ],
  "total_memory": 8589934592,
  "total_data": 5368709120,
  "total_shared": 52428800,
  "total_resident": 6442450944,
  "cpu_user": 72,
  "cpu_system": 8,
  "cpu_idle": 15,
  "cpu_iowait": 3,
  "mem_total": 8589934592,
  "mem_available": 2147483648,
  "mem_free": 1073741824,
  "mem_pressure_some_avg10": 18,
  "mem_pressure_full_avg10": 2
}
```

Field notes:

- All memory and byte fields are **bytes** (int64).
- `cpu_*` fields are percentages (integer 0–100); they do not need to sum to 100.
- `mem_pressure_*` fields are PSI (Pressure Stall Information) averages as a percentage (0–100).
- `volumes` is an array of mounted filesystem snapshots; may be empty or absent if the worker does not report disk usage.

---

### Message: `log_update`

Published at irregular intervals (roughly every 3–8 seconds) while a task is executing.

**Attributes:** `type=log_update`, `task_id=<task_id>`

**Data:** JSON body

```json
{
  "type": "log_update",
  "task_id": "<task_id>",
  "timestamp": "<RFC3339>",
  "content": "line of output\nanother line\n"
}
```

`content` is a raw UTF-8 string containing one or more lines of output (newline-terminated). Multiple chunks arrive in order; consumers should concatenate them to reconstruct the full log.

---

### Message: `command_ack`

Published once by the worker in response to a `start_publishing` command. The dashboard does not need to wait for this ack; it is provided only so that clients can confirm the command was received.

**Attributes:** `type=command_ack`, `task_id=<task_id>`

**Data:** JSON body

```json
{
  "type": "command_ack",
  "req_id": "<req_id>",
  "task_id": "<task_id>"
}
```

`req_id` echoes the value from the corresponding `start_publishing` message so the sender can correlate the acknowledgement.

---

## Topic: `sparkles-v6-task-in` — Worker Control Input

**Direction:** Dashboard → Worker

**Purpose:** Send control commands to a specific running task. Currently only one command type is defined. Messages carry a full JSON body; `task_id` and `type` are also present as attributes to support server-side filtering.

### Publishers

| Service   | Messages published                                                     |
| --------- | ---------------------------------------------------------------------- |
| Dashboard | `start_publishing` (when a browser client creates a task subscription) |

### Subscribers

| Service | How                                                                                                          |
| ------- | ------------------------------------------------------------------------------------------------------------ |
| Worker  | Listens on a filtered subscription while a task is running; processes commands directed at the tasks it owns |

---

### Message: `start_publishing`

Sent by the dashboard backend when a client creates a task subscription. Upon receiving this, the worker should begin publishing `metric_update` and `log_update` messages to `sparkles-v6-task-out`.

**Attributes:** `type=start_publishing`, `task_id=<task_id>`

**Data:** JSON body

```json
{
  "type": "start_publishing",
  "req_id": "<unique_request_id>",
  "task_id": "<task_id>"
}
```

`req_id` is a random hex string. The worker echoes it in the corresponding `command_ack` message.

---

## Subscription Lifecycle

### Lifecycle subscriptions (for the `sparkles-v6-events` topic)

1. Client calls `POST /api/v1/subscription` on the dashboard backend.
2. Backend creates a Pub/Sub subscription on `sparkles-v6-events` named `sparkles-{random_id}`, with a 24-hour message retention and TTL (so abandoned subscriptions self-clean), and an optional server-side filter built from the `types` query parameter.
3. Backend generates a short-lived GCP access token scoped to `pubsub` for the subscriber service account configured in `SparklesV6ProjectConfig`.
4. Backend returns `{ subscription_id, pull_url, ack_url, authorization_token }`.
5. Client pulls directly from the GCP Pub/Sub REST API using the returned credentials — the backend is not in the delivery path.
6. Client calls `POST /api/v1/subscription/{subscription_id}/unsubscribe` when done; backend deletes the Pub/Sub subscription.

### Task metric/log subscriptions (for the `sparkles-v6-task-out` topic)

1. Client calls `POST /api/v1/task/{task_id}/subscription`.
2. Backend creates a Pub/Sub subscription on `sparkles-v6-task-out` filtered to `attributes.task_id = "{task_id}"`.
3. Backend publishes a `start_publishing` control message to `sparkles-v6-task-in`.
4. Backend returns the same `{ subscription_id, pull_url, ack_url, authorization_token }` shape.
5. Client pulls directly from the GCP Pub/Sub REST API.
6. Client calls `POST /api/v1/task/{task_id}/subscription/{subscription_id}/unsubscribe` when done.
