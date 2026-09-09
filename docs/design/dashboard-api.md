# Sparklespray Dashboard API

This document specifies the REST API for the Sparklespray v100 dashboard backend. It is adapted from the original `dashboard-backend` implementation, with names and structure updated to match the v100 data model described in `docs/design/datamodel.md`.

---

## Overview

- **Base URL prefix**: `/api/v1`
- **Default port**: `8080`
- **CORS**: All origins permitted for `GET`, `POST`, and `OPTIONS`
- **Auth**: Backend uses a GCP service account. The subscription endpoints issue short-lived tokens scoped to Pub/Sub for client-side event streaming.

### Error response format

All error responses use the same envelope:

```json
{
  "error": "human-readable message",
  "code": "NOT_FOUND | BAD_REQUEST | INTERNAL_ERROR"
}
```

HTTP status codes:
| Code | Meaning |
|------|---------|
| 200 | Success with body |
| 204 | Success, no body |
| 400 | Invalid query parameter |
| 404 | Entity not found |
| 500 | Backend or GCP API failure |

---

## Workpools

Workpools correspond to the `WorkPools` Firestore collection. They replace the old "cluster" concept.

### `GET /api/v1/workpools`

List all workpools.

**Response** `200 OK` — array of WorkpoolSummary:

```json
[
  {
    "workpool_id": "string",
    "machine_type": "string",
    "region": "string",
    "status": "string (idle | ok | unhealthy | halted)",
    "status_message": "string",
    "last_incident_at": "RFC3339 timestamp | null",
    "incident_count": "integer",
    "expiry": "RFC3339 timestamp"
  }
]
```

**Firestore**: `WorkPools` — full collection scan, no filter.

---

### `GET /api/v1/workpool/{workpool_id}`

Get a single workpool's configuration and current status.

**Path parameters**:

- `workpool_id` — required

**Response** `200 OK`:

```json
{
  "workpool_id": "string",
  "machine_type": "string",
  "region": "string",
  "zones": ["string"],
  "root_dir": "string",
  "sparkles_worker_gcs_path": "string",
  "resources": [{ "name": "string", "value": "float64" }],
  "empty_volumes": [
    { "mount_point": "string", "type": "string", "size_in_gb": "integer" }
  ],
  "max_worker_count": "integer",
  "status": "string (idle | ok | unhealthy | halted)",
  "status_message": "string",
  "last_incident_at": "RFC3339 timestamp | null",
  "incident_count": "integer",
  "expiry": "RFC3339 timestamp"
}
```

**Errors**: `404` if not found.

**Firestore**: `WorkPools/{workpool_id}` — key lookup.

---

### `GET /api/v1/workpool/{workpool_id}/batches`

List all GCP Batch submissions associated with this workpool.

**Path parameters**:

- `workpool_id` — required

**Response** `200 OK` — array of BatchRequest:

```json
[
  {
    "batch_id": "string",
    "job_id": "string (GCP Batch job resource name)",
    "workpool_id": "string",
    "expected_vm_count": "integer",
    "preemptible": "boolean",
    "submitted_at": "RFC3339 timestamp",
    "running_since": "RFC3339 timestamp | null",
    "registered_worker_count": "integer",
    "status": "string (pending | started | completed | failed)",
    "unhealthy": "boolean"
  }
]
```

**Firestore**: `BatchAPIRequests` — query `workpool_id == {workpool_id}`, ordered by `submitted_at` descending.

> **Note**: The old API exposed a `log-summary` endpoint that fetched GCP Cloud Logging entries filtered by a `sparkles-cluster` label on the Batch job. The v100 data model does not attach that label. Re-enabling log aggregation requires labelling batch jobs with `workpool_id` at creation time and updating this endpoint to query Cloud Logging accordingly.

---

## Jobs

Jobs correspond to the `Jobs` Firestore collection.

### `GET /api/v1/jobs`

List job summaries with optional time range filtering. Summaries are read from the `JobSummary` collection (not `Jobs`) because the summarised task counts live there.

**Query parameters**:

- `after` — optional RFC3339 timestamp; return only jobs where `submit_time > after` (based on `JobSummary.expiry` proxy, or a separate indexed field — see note)
- `before` — optional RFC3339 timestamp; return only jobs where `submit_time <= before`
- `workpool_id` — optional; filter to a specific workpool

**Response** `200 OK` — array of JobSummaryResponse:

```json
[
  {
    "job_id": "string",
    "workpool_id": "string",
    "created_at": "RFC3339 timestamp",
    "status": "string (pending | in_progress | in_progress_with_error | in_progress_with_failure | success | error | failed | killed)",
    "tasks": [{ "state": "string", "count": "integer" }],
    "labels": [{ "name": "string", "value": "string" }],
    "expiry": "RFC3339 timestamp"
  }
]
```

**Firestore**: `JobSummary` — filter by `workpool_id` if provided; filter by `created_at` range when `after`/`before` are supplied.

---

### `GET /api/v1/job/{job_id}`

Get a single job's definition.

**Path parameters**:

- `job_id` — required

**Response** `200 OK`:

```json
{
  "job_id": "string",
  "name": "string",
  "workpool_id": "string",
  "created_at": "RFC3339 timestamp",
  "task_count": "integer",
  "resources": [{ "name": "string", "value": "float64" }],
  "labels": [{ "name": "string", "value": "string" }]
}
```

**Errors**: `404` if not found.

**Firestore**: `Jobs/{job_id}` — key lookup.

---

### `GET /api/v1/job/{job_id}/summary-history`

Get the time-series of task-count snapshots for a job, useful for plotting a progress chart.

**Path parameters**:

- `job_id` — required

**Response** `200 OK` — array of JobSummaryHistoryResponse ordered by timestamp ascending:

```json
[
  {
    "job_id": "string",
    "workpool_id": "string",
    "timestamp": "RFC3339 timestamp",
    "status": "string",
    "tasks": [{ "state": "string", "count": "integer" }]
  }
]
```

**Firestore**: `JobSummaryHistory` — query `job_id == {job_id}`, order by `timestamp` asc.

---

## Tasks

Tasks correspond to the `Tasks` Firestore collection.

### `GET /api/v1/task/{task_id}`

Get a single task's current state and metadata.

**Path parameters**:

- `task_id` — required

**Response** `200 OK`:

```json
{
  "task_id": "string",
  "task_index": "integer",
  "job_id": "string",
  "workpool_id": "string",
  "status": "string (pending | claimed | running | writing | success | error | failed | killed)",
  "command": ["string"],
  "docker_image": "string",
  "result_path": "string (GCS path, e.g. gs://bucket/path) | null",
  "log_path": "string (GCS path) | null",
  "owning_worker_id": "string | null",
  "failure_reason": "string | null",
  "parameters": [{ "name": "string", "value": "string" }],
  "exit_code": "integer | null",
  "resource_usage": {
    "start_time": "RFC3339 timestamp",
    "end_time": "RFC3339 timestamp",
    "elapsed_seconds": "float64",
    "exit_code": "integer",
    "oom_killed": "boolean",
    "container_memory_oom_kill_count": "integer, omitted if unavailable (includes OOM-killed children)",
    "container_cpu_usage_usec": "integer, omitted if unavailable",
    "container_cpu_user_usec": "integer, omitted if unavailable",
    "container_cpu_system_usec": "integer, omitted if unavailable",
    "container_cpu_throttled_usec": "integer, omitted if unavailable",
    "container_cpu_throttled_periods": "integer, omitted if unavailable",
    "container_memory_peak_bytes": "integer, omitted if unavailable",
    "container_memory_limit_bytes": "integer, omitted if unavailable",
    "container_memory_major_faults": "integer, omitted if unavailable",
    "container_memory_workingset_refaults": "integer, omitted if unavailable",
    "container_cpu_stall_some_usec": "integer, omitted if unavailable",
    "container_cpu_stall_full_usec": "integer, omitted if unavailable",
    "container_memory_stall_some_usec": "integer, omitted if unavailable",
    "container_memory_stall_full_usec": "integer, omitted if unavailable",
    "container_io_stall_some_usec": "integer, omitted if unavailable",
    "container_io_stall_full_usec": "integer, omitted if unavailable",
    "container_io_read_bytes": "integer, omitted if unavailable",
    "container_io_write_bytes": "integer, omitted if unavailable",
    "container_io_read_ops": "integer, omitted if unavailable",
    "container_io_write_ops": "integer, omitted if unavailable",
    "container_pids_peak": "integer, omitted if unavailable"
  }
}
```

`resource_usage` is omitted when absent (task has not yet completed). Any individual field within it is omitted when that metric could not be collected — the same nil-is-absent convention as `MetricSample`, see the conventions under [`GET /api/v1/task/{task_id}/log`](#get-apiv1tasktask_idlog). Field semantics and cgroup sources are documented in [datamodel.md](datamodel.md).

**Errors**: `404` if not found.

**Firestore**: `Tasks/{task_id}` — key lookup.

---

### `GET /api/v1/task/{task_id}/log`

Get streaming log and metric entries for a task. Both `log_update` and `metric_update` entries are stored in the `TaskLog` collection and returned together, distinguished by their `type` field.

Entries are a discriminated union: `content` is populated for `log_update`, and the nested `metric` object for `metric_update`. The metric payload is nested rather than flattened so a client can reuse a single metric type instead of carrying ~40 optional fields on the log branch.

**Path parameters**:

- `task_id` — required

**Query parameters**:

- `after` — optional RFC3339 timestamp; return entries with `timestamp > after`
- `types` — optional comma-separated filter: `log_update`, `metric_update`, or both (default: both)

**Response** `200 OK`:

```json
{
  "entries": [
    {
      "task_id": "string",
      "type": "string (log_update | metric_update)",
      "timestamp": "RFC3339 timestamp",

      "content": "string (only present when type == log_update)",

      "metric": {
        "task_id": "string",
        "type": "metric_update",
        "timestamp": "RFC3339 timestamp",
        "metric_schema": "integer",
        "seq": "integer (0-based sample index within the task)",
        "final": "boolean (true for the single post-exit sample)",

        "host_cpu_user_pct": "float64, omitted if unavailable (% of total CPU across all cores)",
        "host_cpu_system_pct": "float64, omitted if unavailable",
        "host_cpu_idle_pct": "float64, omitted if unavailable",
        "host_cpu_iowait_pct": "float64, omitted if unavailable",
        "host_cpu_count": "integer, omitted if unavailable (number of logical cores backing the host_cpu_*_pct fields above)",
        "host_memory_total_bytes": "integer, omitted if unavailable",
        "host_memory_available_bytes": "integer, omitted if unavailable",
        "host_cpu_stall_some_usec": "integer, omitted if unavailable (cumulative microseconds)",
        "host_cpu_stall_full_usec": "integer, omitted if unavailable",
        "host_memory_stall_some_usec": "integer, omitted if unavailable",
        "host_memory_stall_full_usec": "integer, omitted if unavailable",
        "host_io_stall_some_usec": "integer, omitted if unavailable",
        "host_io_stall_full_usec": "integer, omitted if unavailable",
        "host_volumes": [
          {
            "location": "string",
            "total_bytes": "integer",
            "used_bytes": "integer"
          }
        ],

        "container_memory_current_bytes": "integer, omitted if unavailable",
        "container_memory_peak_bytes": "integer, omitted if unavailable",
        "container_memory_limit_bytes": "integer, omitted if unavailable",
        "container_cpu_usage_usec": "integer, omitted if unavailable (cumulative)",
        "container_cpu_user_usec": "integer, omitted if unavailable (cumulative)",
        "container_cpu_system_usec": "integer, omitted if unavailable (cumulative)",
        "container_cpu_throttled_usec": "integer, omitted if unavailable",
        "container_cpu_throttled_periods": "integer, omitted if unavailable",
        "container_memory_major_faults": "integer, omitted if unavailable",
        "container_memory_workingset_refaults": "integer, omitted if unavailable",
        "container_memory_oom_kill_count": "integer, omitted if unavailable",
        "container_pids_peak": "integer, omitted if unavailable",
        "container_cpu_stall_some_usec": "integer, omitted if unavailable",
        "container_cpu_stall_full_usec": "integer, omitted if unavailable",
        "container_memory_stall_some_usec": "integer, omitted if unavailable",
        "container_memory_stall_full_usec": "integer, omitted if unavailable",
        "container_io_stall_some_usec": "integer, omitted if unavailable",
        "container_io_stall_full_usec": "integer, omitted if unavailable",
        "container_io_read_bytes": "integer, omitted if unavailable",
        "container_io_write_bytes": "integer, omitted if unavailable",
        "container_io_read_ops": "integer, omitted if unavailable",
        "container_io_write_ops": "integer, omitted if unavailable"
      }
    }
  ],
  "next_after": "RFC3339 timestamp"
}
```

`next_after` is always present and is the timestamp of the last returned entry (or `now` when the result is empty). Use it as the `after` parameter of the next call to poll for new entries.

**Conventions clients must handle:**

- **A missing field means "unavailable", not zero.** A metric this kernel does not expose is omitted from the JSON entirely (Go's `*int64`/`*float64` fields encode as absent, not `null`, when nil), so it stays distinguishable from a genuine zero. Clients should render a missing field as a gap, **not** as `0` and not as `NaN`. Common causes: a kernel too old for a given cgroup file, a `cpu.pressure` file with no `full` line (widespread), an unlimited memory limit, cgroup v1, a container whose cgroup had already been torn down, or (for the `host_cpu_*_pct` fields specifically) the first sample of a task, which has no previous `/proc/stat` snapshot to difference against.
- **Stall counters are cumulative microseconds, not rates.** To chart a stall percentage or a CPU rate, difference consecutive samples and divide by the wall time between them; clamp negative deltas (counter reset) to a gap. Cumulative totals are stored deliberately: a decaying average has a ~10-second ramp and so is meaningless for a short task, whereas totals difference exactly over any interval and compose with the final post-exit sample without leaving a gap.
- **Sample spacing is non-uniform.** Sampling is adaptive — 1s after task start, doubling to a 60s ceiling — so charts need a time-scaled x-axis. A categorical axis would badly distort the early, most detailed part of every task.
- **Check `metric_schema`.** `TaskLog` entries live for 7 days, so after a worker rollout the collection contains both old and new layouts. A client that decodes an old sample into the current shape sees all zeros — a container that apparently used no CPU — so mismatched schemas must be skipped rather than displayed. The server already skips them; clients holding cached entries should too.
- **A very short task yields exactly one entry**, with `final: true` and `seq: 0`, since it finished before the first periodic sample. Charts should handle a single-point series.

**Errors**: `400` for malformed `after` timestamp.

**Firestore**: `TaskLog` — query `task_id == {task_id}` and optionally `type in {types}`, with `timestamp > after`, ordered by `timestamp` asc.

> **Change from old API**: The old API had two separate endpoints: `/task/{id}/log` (returning `content` entries) and `/task/{id}/metrics` (returning metric samples). In v100 both are stored in the same `TaskLog` collection with a `type` discriminator, so they are unified here. Clients that need only one type should pass `types=log_update` or `types=metric_update`.

---

### `GET /api/v1/metrics`

Metadata describing every metric a `metric_update` entry's `metric` object (the periodic per-task time series, `MetricSample`) and/or a task's `resource_usage` (the one-shot final summary, `ResourceUsage`) may carry, independent of any particular task's samples. A client uses this to build a metric picker (which metrics exist, human-readable names/descriptions, which units to label an axis with, whether to plot a value as-is, as a rate, or as a count-per-category) instead of hardcoding either struct's field list.

**Response** `200 OK`:

```json
{
  "metrics": [
    {
      "key": "host_cpu_user_pct",
      "name": "User CPU",
      "description": "Percentage of total CPU time across all host cores spent in user mode since the previous sample.",
      "units": "percent",
      "type": "gauge",
      "default_position": 1,
      "in_metric_sample": true,
      "in_resource_usage": false
    },
    {
      "key": "container_memory_peak_bytes",
      "name": "Container Memory (peak)",
      "description": "High-water mark of the container's memory usage over its lifetime so far.",
      "units": "bytes",
      "type": "gauge",
      "resource_usage_default_position": 2,
      "in_metric_sample": true,
      "in_resource_usage": true
    },
    {
      "key": "container_cpu_usage_usec",
      "name": "Container CPU",
      "description": "Cumulative CPU time consumed by the container.",
      "units": "usec",
      "type": "counter",
      "in_metric_sample": true,
      "in_resource_usage": true
    },
    {
      "key": "elapsed_seconds",
      "name": "Execution Time",
      "description": "Wall-clock duration of the task's container, end minus start.",
      "units": "seconds",
      "type": "gauge",
      "resource_usage_default_position": 1,
      "in_metric_sample": false,
      "in_resource_usage": true
    },
    {
      "key": "exit_code",
      "name": "Exit Code",
      "description": "The container's process exit code.",
      "units": "none",
      "type": "categorical",
      "in_metric_sample": false,
      "in_resource_usage": true
    }
  ]
}
```

Fields on each metric:

- `key` — matches the field's JSON name on `MetricSample` and/or `ResourceUsage` (see `in_metric_sample`/`in_resource_usage`), with one exception: `host_volumes` is an array, and each element yields two per-volume metric values at the sample level (`host_volume_total_bytes`, `host_volume_used_bytes`, each tagged with which volume by a `location` the client associates with that reading), rather than a single scalar field.
- `units` — `"percent" | "bytes" | "usec" | "count" | "seconds" | "none"`, the unit the _raw_ stored value is in. `"none"` is for `"categorical"` metrics, whose value is a label rather than a quantity.
- `type` —
  - `"gauge"`: the raw value is meaningful on its own at a single point in time — a percentage, a current byte count, a high-water mark like `container_pids_peak` or `container_memory_peak_bytes`.
  - `"counter"`: in a `MetricSample` time series, the raw value only ever grows, so a single reading says little on its own; clients should plot `Δvalue / Δtime` instead of the raw cumulative value, and label the rate `<units>/s`. In a `ResourceUsage` one-shot summary there's no second reading to difference against — it's just the cumulative total for that task's whole run, plotted as-is.
  - `"categorical"`: the value is one of a small set of discrete values (an exit code, a boolean) — plot a count of tasks per distinct value, not percentiles.
- `default_position` / `resource_usage_default_position` — each omitted entirely when this metric shouldn't be shown by default on that view (most metrics, most views). When present, a lower number means higher priority among the metrics a client shows without the user asking for more. This is a recommendation, not a requirement — clients may override it (e.g. an explicit user selection). Two separate fields, not one shared number space, because a metric can be `in_metric_sample` **and** `in_resource_usage` at once (most `container_*` counters are) with a different default-visibility answer on each view — e.g. `container_memory_peak_bytes` above is a default on the per-job distributions view (`resource_usage_default_position: 2`) but not on the per-task time series view (no `default_position` at all, which instead defaults to `container_memory_current_bytes`). `default_position` is the per-task time series (`MetricSample`) view's answer; `resource_usage_default_position` is the per-job distributions (`ResourceUsage`) view's.
- `in_metric_sample` / `in_resource_usage` — which of the two structs this key can appear on. A client filters this table by whichever one it's rendering: the per-task time series tab uses `in_metric_sample`, the per-job distributions tab uses `in_resource_usage`. Most `container_*` counters are true for both (both structs use the same field names for the ones they share).

**Firestore**: none — this is a static, hand-maintained table (`v100.MetricMetadataTable`), not read from a collection.

---

### `POST /api/v1/task/{task_id}/stream`

Request that the worker running this task begin streaming its log and metric entries to the `TaskLog` Firestore collection in real time. By default, workers buffer locally; this activates live streaming.

**Path parameters**:

- `task_id` — required

**Response** `204 No Content`

**Pub/Sub**: Publishes a `stream_task_updates` control message to the topic `sparkles-worker-in`. The message targets the specific worker identified by the task's `owning_worker_id`. The worker creates a per-worker subscription named `sparkles-worker-in-{worker_id}` at startup.

Message payload:

```json
{ "type": "stream_task_updates", "task_id": "string" }
```

> **Change from old API**: The old endpoint published to `sparkles-v6-task-in` with type `start_publishing`. The v100 equivalent topic is `sparkles-worker-in` with message type `stream_task_updates`. The routing changed: messages must be published to the per-worker subscription's topic, not a global task topic. The backend must look up `owning_worker_id` from the Task document before publishing, so it can route to the right worker's subscription.

---

## Events

Events correspond to the `Events` Firestore collection, which is the durable audit log for everything published to the `sparkles-events` Pub/Sub topic.

### `GET /api/v1/events`

Query the event log.

**Query parameters**:

- `after` — optional RFC3339 timestamp; return events with `timestamp > after`
- `before` — optional RFC3339 timestamp; return events with `timestamp <= before`
- `job_id` — optional; filter to events related to a specific job
- `workpool_id` — optional; filter to events related to a specific workpool
- `task_id` — optional; filter to events related to a specific task
- `types` — optional comma-separated list of event types: `worker_started`, `worker_stopped`, `task_state_update`, `job_created`, `job_terminated`
- `limit` — optional integer, default 1000, max 10000

**Response** `200 OK`:

```json
{
  "events": [
    {
      "event_id": "string",
      "type": "string",
      "timestamp": "RFC3339 timestamp",
      "expiry": "RFC3339 timestamp",
      "worker_id": "string | null",
      "workpool_id": "string | null",
      "task_id": "string | null",
      "job_id": "string | null",
      "old_state": "string | null",
      "new_state": "string | null"
    }
  ],
  "next_after": "RFC3339 timestamp (timestamp of last entry; omitted when result is empty)"
}
```

Fields not applicable to an event's `type` are omitted or null.

**Firestore**: `Events` — filter by provided parameters, order by `timestamp` asc, apply limit.

> **Change from old API**: The old event documents were untyped property maps and included a `cluster_id` field. The v100 `EventRecord` struct uses `workpool_id` instead of `cluster_id`. The `task_id` filter is new — the old model did not index events by task.

---

## Event Streaming (Pub/Sub)

For real-time event delivery, clients can create a short-lived Pub/Sub subscription and poll it directly using their own bearer token.

### `POST /api/v1/subscriptions`

Create a new subscription to the `sparkles-events` Pub/Sub topic.

**Query parameters**:

- `types` — optional comma-separated list of event types to filter on (e.g. `job_created,job_terminated`). If omitted, all event types are delivered.

**Response** `200 OK`:

```json
{
  "subscription_id": "string",
  "pull_url": "string (Google Pub/Sub pull endpoint)",
  "ack_url": "string (Google Pub/Sub acknowledge endpoint)",
  "authorization_token": "string (short-lived access token scoped to Pub/Sub)"
}
```

The subscription auto-expires after 24 hours. The `authorization_token` is a short-lived credential scoped to `https://www.googleapis.com/auth/pubsub`, generated by impersonating a dashboard service account.

**Pub/Sub**: Creates subscription under `sparkles-events` topic with a 24-hour TTL and 10-second ack deadline. If `types` is provided, applies a server-side filter: `attributes.type = "t1" OR attributes.type = "t2" ...`.

---

### `POST /api/v1/subscriptions/{subscription_id}/unsubscribe`

Delete a previously-created subscription.

**Path parameters**:

- `subscription_id` — required

**Response** `204 No Content`

---

## Workers

Workers correspond to the `Workers` Firestore collection.

### `GET /api/v1/workpool/{workpool_id}/workers`

List active worker records for a workpool.

**Path parameters**:

- `workpool_id` — required

**Query parameters**:

- `status` — optional; filter by `started` or `stopped` (default: `started`)

**Response** `200 OK` — array of WorkerRecord:

```json
[
  {
    "worker_id": "string",
    "workpool_id": "string",
    "batch_id": "string",
    "instance_name": "string",
    "status": "string (started | stopped)",
    "expiry": "RFC3339 timestamp",
    "heartbeat_expiry": "RFC3339 timestamp"
  }
]
```

**Firestore**: `Workers` — query `workpool_id == {workpool_id}` and optionally `status == {status}`.

> **New endpoint**: The old API had no direct worker listing. This endpoint is new and made possible by the `Workers` collection added in v100.

---

## Data Model Inconsistencies and Gaps

This section summarises capabilities present in the old dashboard that cannot be directly implemented from the v100 data model without additions.

### Missing: WorkPool-level VM status detail

The old `ClusterStatus` document tracked per-pool VM-level detail: `instanceInUseCount`, `idleInstanceCount`, `orphanedTaskCount`, `preemptableInstanceCount`, `nonPreemptableInstanceCount`, along with a classification of failed batch submissions. The v100 `WorkPools` document has only a string `status` and `status_message` (plus `last_incident_at` / `incident_count`).

**Workaround**: The `/workpool/{workpool_id}/batches` endpoint exposes `BatchAPIRequests` data, from which a client can compute submitted/started/failed batch counts. VM-level counts require querying the GCP Compute or Batch API at request time, as in the original backend. A `WorkpoolSummary` collection is defined in `datamodel.md` for when this is implemented.

### Missing: arbitrary Job metadata

The old `Job` document carried a `metadata` JSON blob and `kube_job_spec`. The v100 model does not have these. If they are needed, they must be added at submission time.

### Changed: log_path vs log_url

The old `Task.log_url` was a full HTTP URL. The v100 `Task.log_path` is a GCS path (`gs://bucket/key`). The backend should convert this to a signed URL or a GCS console URL before returning it to the dashboard.

### Changed: task metrics collection

The old backend had a separate `/task/{id}/metrics` endpoint backed by a `SparklesV6TaskMetric` collection with periodic samples. In v100, periodic metric samples are stored as `metric_update` entries inside the `TaskLog` collection, and a final summary is stored as `Task.resource_usage`. The new `/task/{id}/log-entries` endpoint unifies both. Clients that previously polled `/metrics` should now poll `/log-entries?types=metric_update`.

### Changed: stream task updates routing

The old `POST /task/{id}/subscription` published to a global `sparkles-v6-task-in` topic. In v100, the equivalent is a `stream_task_updates` message published to `sparkles-worker-in`, which is routed to the specific worker via per-worker subscriptions. The backend must look up the task's `owning_worker_id` to find the correct worker.

### New: BatchAPIRequests

The v100 data model adds a `BatchAPIRequests` collection that has no equivalent in the old model. The `/workpool/{workpool_id}/batches` endpoint exposes this data and can replace some of the status information previously computed by polling GCP APIs.

### New: ResourceUsage on Task

The v100 `Task.resource_usage` sub-document provides a single post-run resource summary (CPU time, peak memory, block I/O, elapsed time, OOM flag). This is new information with no equivalent in the old model.
