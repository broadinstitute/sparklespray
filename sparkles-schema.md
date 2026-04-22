# Sparkles: Data Storage and Communication Schema

## Overview

Sparkles is a distributed batch-job system. A **cluster** of worker nodes pulls
tasks from a queue, executes them, and reports results. The dashboard observes
all of this in near-real-time through two channels:

1. **Datastore** — authoritative, persistent state for clusters, jobs, tasks, and
   lifecycle events. The dashboard backend reads from here to answer REST queries
   and to bootstrap fresh sessions.
2. **Pub/Sub** — ephemeral, push-style notifications used to avoid polling. There
   are three topics: one for lifecycle event notifications, one for per-task
   metrics/logs flowing out of workers, and one for control messages flowing into
   workers.

The data described here is written by **worker nodes** (tasks, job control) and
read by the **dashboard backend** (and, indirectly, the browser). The simulator
(`simulator/main.go`) is the reference implementation of the writer side and
should be treated as the normative contract for all field names, types, and values.

---

## GCP Project and Naming

All resources live in a single GCP project. Collection and topic names are
versioned to allow in-place schema migrations without disrupting existing data.

| Resource                                | Name                   |
| --------------------------------------- | ---------------------- |
| Datastore collection – events           | `SparklesV6Event`      |
| Datastore collection – clusters         | `SparklesV6Cluster`    |
| Datastore collection – jobs             | `SparklesV6Job`        |
| Datastore collection – tasks            | `SparklesV6Task`       |
| Pub/Sub topic – lifecycle notifications | `sparkles-v6-events`   |
| Pub/Sub topic – task metrics/log output | `sparkles-v6-task-out` |
| Pub/Sub topic – worker control input    | `sparkles-v6-task-in`  |

---

## Datastore Collections

### SparklesV6Cluster

One document per compute cluster. Written once when the cluster starts.

| Field          | Type      | Indexed | Description                                   |
| -------------- | --------- | ------- | --------------------------------------------- |
| `cluster_id`   | string    | yes     | Unique identifier (e.g. `cluster-a1b2c3d4`)   |
| `machine_type` | string    | yes     | GCE machine type label (e.g. `n1-standard-4`) |
| `created_at`   | timestamp | yes     | UTC time the cluster was registered           |

**Key**: `NameKey("SparklesV6Cluster", cluster_id)`

---

### SparklesV6Job

One document per submitted job. Written when the job is created and updated as
its status changes.

| Field                      | Type      | Indexed | Description                                                                  |
| -------------------------- | --------- | ------- | ---------------------------------------------------------------------------- |
| `job_id`                   | string    | yes     | Unique identifier (e.g. `job-20240101-120000-abc123`)                        |
| `cluster_id`               | string    | yes     | ID of the cluster running this job                                           |
| `status`                   | string    | yes     | Current status: `pending` \| `running` \| `complete` \| `failed` \| `killed` |
| `submit_time`              | timestamp | yes     | UTC time the job was submitted                                               |
| `task_count`               | int32     | yes     | Total number of tasks in this job                                            |
| `tasks`                    | []string  | no      | Ordered list of task IDs (noindex – large list)                              |
| `kube_job_spec`            | string    | no      | Serialized Kubernetes job spec (optional)                                    |
| `metadata`                 | string    | no      | Arbitrary job metadata string (optional)                                     |
| `max_preemptable_attempts` | int32     | yes     | Maximum times a task may be preempted before being failed                    |
| `target_node_count`        | int32     | yes     | Target number of worker nodes for this job                                   |

**Key**: `NameKey("SparklesV6Job", job_id)`

---

### SparklesV6Task

One document per task. Written when the task is created and updated at each
status transition.

| Field                | Type          | Indexed | Description                                                       |
| -------------------- | ------------- | ------- | ----------------------------------------------------------------- |
| `task_id`            | string        | yes     | Unique identifier, formatted as `{job_id}.{04d index}`            |
| `task_index`         | int64         | yes     | Zero-based position within the job                                |
| `job_id`             | string        | yes     | Parent job ID                                                     |
| `cluster_id`         | string        | yes     | ID of the cluster owning this task                                |
| `status`             | string        | yes     | Current status (Similar to those for SparklesV6Job)               |
| `owner`              | string        | yes     | Worker ID currently holding the task (empty if not claimed)       |
| `last_updated`       | timestamp     | yes     | UTC time of most recent status change                             |
| `failure_reason`     | string        | yes     | Human-readable failure message (present when status is `failed`)  |
| `exit_code`          | string        | yes     | Process exit code string (set on completion)                      |
| `version`            | int32         | yes     | Optimistic-concurrency version counter                            |
| `args`               | string        | no      | Task arguments / command input                                    |
| `command`            | string        | no      | Full command string                                               |
| `docker_image`       | string        | no      | Docker image used for execution                                   |
| `command_result_url` | string        | no      | GCS URL for the command result file                               |
| `monitor_address`    | string        | no      | Address of the worker's monitoring endpoint                       |
| `log_url`            | string        | no      | GCS URL for the task's stdout/stderr log                          |
| `history`            | []TaskHistory | no      | Ordered list of all status transitions (noindex – embedded slice) |

**Key**: `NameKey("SparklesV6Task", task_id)`

#### TaskHistory (embedded in Task.history)

| Field            | Type    | Description                                           |
| ---------------- | ------- | ----------------------------------------------------- |
| `timestamp`      | float64 | Unix seconds (nanosecond precision) of the transition |
| `status`         | string  | Status value at this point in time                    |
| `owner`          | string  | Worker that caused the transition (may be empty)      |
| `failure_reason` | string  | Reason if the transition was a failure (may be empty) |

---

### SparklesV6Event

Immutable event log. One document is appended for every significant lifecycle
event. Events are never updated; they accumulate and expire after 7 days.

**Key**: `NameKey("SparklesV6Event", event_id)` where `event_id` is a UUID.

#### Common fields on every event

| Field       | Type      | Indexed | Description                                |
| ----------- | --------- | ------- | ------------------------------------------ |
| `event_id`  | string    | yes     | UUID assigned at write time                |
| `type`      | string    | yes     | Event type string (see catalogue below)    |
| `timestamp` | timestamp | yes     | UTC write time                             |
| `expiry`    | timestamp | yes     | `timestamp + 7 days`; used for TTL cleanup |

#### Event type catalogue

All fields below are indexed unless marked **noindex**.

---

**`cluster_started`** — a new cluster came online

| Field        | Type   | Description       |
| ------------ | ------ | ----------------- |
| `cluster_id` | string | ID of the cluster |

---

**`worker_started`** — a worker node started within a cluster

| Field        | Type   | Description              |
| ------------ | ------ | ------------------------ |
| `cluster_id` | string |                          |
| `worker_id`  | string | Unique worker identifier |

---

**`worker_stopped`** — a worker node exited (normal or crash)

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `worker_id`  | string |             |

---

**`job_started`** — a new job was submitted

| Field        | Type   | Description                      |
| ------------ | ------ | -------------------------------- |
| `cluster_id` | string |                                  |
| `job_id`     | string |                                  |
| `task_count` | int64  | Total number of tasks in the job |

---

**`task_claimed`** — a worker picked up a task and started staging

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `job_id`     | string |             |
| `task_id`    | string |             |

---

**`task_exec_started`** — the task's process began executing

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `job_id`     | string |             |
| `task_id`    | string |             |

---

**`task_exec_complete`** — the task's process exited; upload phase begins

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `job_id`     | string |             |
| `task_id`    | string |             |

---

**`task_complete`** — the task fully succeeded (staging, exec, and upload done)

| Field                   | Type    | Indexed | Description                             |
| ----------------------- | ------- | ------- | --------------------------------------- |
| `cluster_id`            | string  | yes     |                                         |
| `job_id`                | string  | yes     |                                         |
| `task_id`               | string  | yes     |                                         |
| `exit_code`             | int64   | noindex | Process exit code (0 = success)         |
| `download_bytes`        | int64   | noindex | Bytes downloaded during localization    |
| `upload_bytes`          | int64   | noindex | Bytes uploaded after execution          |
| `max_mem_in_gb`         | float64 | noindex | Peak RSS in GB                          |
| `max_memory_bytes`      | int64   | noindex | Peak RSS in bytes                       |
| `shared_memory_bytes`   | int64   | noindex | Peak shared memory in bytes             |
| `unshared_memory_bytes` | int64   | noindex | Peak unshared (private) memory in bytes |
| `user_cpu_sec`          | float64 | noindex | Total user-mode CPU seconds             |
| `system_cpu_sec`        | float64 | noindex | Total system-mode CPU seconds           |
| `block_input_ops`       | int64   | noindex | Block input operations                  |
| `block_output_ops`      | int64   | noindex | Block output operations                 |

---

**`task_failed`** — the task permanently failed

| Field            | Type   | Indexed | Description                       |
| ---------------- | ------ | ------- | --------------------------------- |
| `cluster_id`     | string | yes     |                                   |
| `job_id`         | string | yes     |                                   |
| `task_id`        | string | yes     |                                   |
| `failure_reason` | string | noindex | Human-readable reason for failure |

---

**`task_orphaned`** — the worker holding the task died; task will be requeued

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `job_id`     | string |             |
| `task_id`    | string |             |

---

## Pub/Sub Topics

### `sparkles-v6-events` — Lifecycle Notifications

**Direction**: workers → dashboard backend (and any other consumers)

**Purpose**: Notify subscribers that a lifecycle event has been written to
Datastore. Subscribers fetch the actual event data from Datastore; the Pub/Sub
message itself carries no body — it is purely a wake-up signal.

**Message format**:

- **Data**: empty (zero bytes)
- **Attributes**: `type=<event_type>` (e.g. `type=task_complete`)

The `type` attribute enables server-side Pub/Sub filter expressions, allowing
subscribers to receive only the event types they care about (e.g.
`attributes.type = "job_started" OR attributes.type = "task_claimed"`).

One message is published for every event written to `SparklesV6Event`.

---

### `sparkles-v6-task-out` — Task Metric and Log Output

**Direction**: worker nodes → dashboard

**Purpose**: Real-time stream of resource metrics and stdout/stderr from an
actively running task. Unlike lifecycle events, these messages carry a full JSON
body. Because the volume can be very high (one metric sample every 10 seconds per
running task), subscribers are created on demand — only when a user is actively
viewing a specific task.

All messages carry:

- **Attributes**: `type=<message_type>`, `task_id=<task_id>`

#### `metric_update` — Resource usage sample

Published approximately every 10 seconds while a task is executing. JSON body:

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

- All memory/byte fields are in **bytes** (int64).
- `cpu_*` fields are percentages (integer 0–100); they do not need to sum to 100.
- `mem_pressure_*` fields are PSI (Pressure Stall Information) averages, as a
  percentage (0–100).
- `volumes` is an array of mounted filesystem snapshots; may be empty or absent
  if the worker does not report disk usage.

#### `log_update` — Stdout/stderr chunk

Published at irregular intervals (roughly every 3–8 seconds) while a task is
executing. JSON body:

```json
{
  "type": "log_update",
  "task_id": "<task_id>",
  "timestamp": "<RFC3339>",
  "content": "line of output\nanother line\n"
}
```

`content` is a raw UTF-8 string containing one or more lines of output
(newline-terminated). Multiple chunks arrive in order; consumers should
concatenate them to reconstruct the full log.

#### `command_ack` — Acknowledgement of a control command

Published once in response to a `start_publishing` control message (see below).
JSON body:

```json
{
  "type": "command_ack",
  "req_id": "<req_id>",
  "task_id": "<task_id>"
}
```

The dashboard backend does **not** need to wait for this ack; it is provided only
so that clients can confirm the command was received.

---

### `sparkles-v6-task-in` — Worker Control Input

**Direction**: dashboard backend → worker nodes

**Purpose**: Send control commands to a specific running task. Currently only one
command type is defined.

All messages carry a full JSON body; the `task_id` and `type` fields are also
present as message **attributes** to support server-side filtering.

#### `start_publishing` — Request metric/log streaming

Sent by the backend when a client creates a task subscription. The worker should
begin publishing `metric_update` and `log_update` messages to `sparkles-v6-task-out`
after receiving this.

```json
{
  "type": "start_publishing",
  "req_id": "<unique_request_id>",
  "task_id": "<task_id>"
}
```

`req_id` is a random hex string. The worker echoes it in the corresponding
`command_ack` message so the sender can correlate the acknowledgement.

---

## Subscription Lifecycle

### Lifecycle subscriptions (lifecycle events)

1. Client calls `POST /api/v1/subscription` on the dashboard backend.
2. Backend creates a Pub/Sub subscription on `sparkles-v6-events` with:
   - A `sparkles-{random_id}` subscription name.
   - A 24-hour message retention and TTL (so abandoned subscriptions self-clean).
   - An optional filter expression built from the `types` query parameter.
3. Backend generates a short-lived GCP access token scoped to `pubsub` for a
   dedicated subscriber service account (read-only, pull/ack only).
4. Backend returns `{ subscription_id, pull_url, ack_url, authorization_token }`.
5. Client pulls directly from the GCP Pub/Sub REST API using the returned URLs and
   token — the backend is not in the loop for message delivery.
6. Client calls `POST /api/v1/subscription/{subscription_id}/unsubscribe` when
   done; backend deletes the Pub/Sub subscription.

### Task metric/log subscriptions

1. Client calls `POST /api/v1/task/{task_id}/subscription`.
2. Backend creates a Pub/Sub subscription on `sparkles-v6-task-out` filtered to
   `attributes.task_id = "{task_id}"`.
3. Backend publishes a `start_publishing` control message to `sparkles-v6-task-in`.
4. Backend returns the same `{ subscription_id, pull_url, ack_url, authorization_token }` shape as above.
5. Client pulls directly from the GCP Pub/Sub REST API.
6. Client calls `POST /api/v1/task/{task_id}/subscription/{subscription_id}/unsubscribe`
   when done.

---

## Task ID Format

Task IDs are constructed as:

```
{job_id}.{zero_padded_index}
```

For example, task 3 of job `job-20240101-120000-abc123` has ID
`job-20240101-120000-abc123.0003`.

Job IDs are constructed as:

```
job-{YYYYMMDD-HHMMSS}-{6_char_uuid_prefix}
```

---

## Event Expiry

Events in `SparklesV6Event` are written with an `expiry` field set to 7 days
after the event timestamp. This field should be used as the basis for a Datastore
TTL policy so that old events are deleted automatically. No other collections have
automatic expiry.
