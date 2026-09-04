# Dashboard Frontend Update — Gap Analysis

This document compares the React frontend in `dashboard/` against the new Go backend in
`v100/dev/dashboard_backend.go` (API spec: `dashboard-api.md`). The goal is to determine
what changes are required to make the frontend work again, and to flag anything
non-trivial enough that dropping the feature is preferable to carrying the complexity.

---

## Decisions

| #   | Topic                       | Decision                                                                                 |
| --- | --------------------------- | ---------------------------------------------------------------------------------------- |
| Q1  | Live task log/metrics       | Poll `GET /api/v1/task/{id}/log?after={cursor}` instead of PubSub pull                   |
| Q2  | Worker pool sidebar         | Replace with minimal version using `GET /api/v1/workpools`                               |
| Q3  | PerfOverview page           | Keep; add `GET /api/v1/job/{id}/tasks` endpoint with `status` and `last_updated` filters |
| Q4  | ClusterDetail / ClusterLogs | Drop worker-count time series from ClusterDetail; remove ClusterLogs entirely            |
| Q5  | Task status vocabulary      | New vocabulary accepted (`running`/`writing`/`success`/`error` replacing old names)      |

---

## Summary

| Category                                | Items |
| --------------------------------------- | ----- |
| Trivial renames (< 5 lines each)        | 1–4   |
| Small field-level changes (10–50 lines) | 5–7   |
| Moderate rewrites (50–200 lines)        | 8–10  |
| Backend addition required               | 11    |
| Features to remove                      | 12–14 |

The largest single change is the **event type model rewrite** (item 8): the new backend emits
`task_state_update` (with `old_state`/`new_state`) instead of seven fine-grained event types,
which affects nearly every data file and several pages.

---

## Trivial Renames

These are safe to batch into a single PR since each change is mechanical.

### 1. Event `id` → `event_id`

`EventProvider.tsx` deduplicates events using `e.id`. The new backend returns `event_id`.

```ts
// Before
const knownIds = new Set(prev.map((e) => e.id));
// After
const knownIds = new Set(prev.map((e) => e.event_id));
```

Also update the `BaseEvent` interface in `types.ts`.

### 2. `cluster_id` query parameter → `workpool_id`

`ClusterDetail.tsx` calls `/api/v1/events?cluster_id={clusterId}`. The new backend parameter
is `workpool_id`.

### 3. `task.log_url` → `task.log_path`

`TaskDetail.tsx` reads `d.log_url` from the task response. The new backend returns `log_path`
(a GCS URI, e.g. `gs://bucket/key`). The frontend currently only passes this through to a
link — the value will display differently but the code change is one line.

### 4. Event query for cluster events: `types` values

`ClusterDetail` queries events with `types=worker_started,worker_stopped,cluster_started,cluster_stopped`.
In the new model `cluster_started`/`cluster_stopped` do not exist; remove them from the filter.

---

## Small Field-Level Changes

### 5. Jobs list: endpoint path and response shape

`EventProvider.tsx` polls `GET /api/v1/jobs/summary` and expects `BackendJobSummary[]`:

```ts
{
  jobID,
    submitTime,
    lastUpdated,
    clusterId,
    taskCount,
    successCount,
    failureCount;
}
```

The new endpoint is `GET /api/v1/jobs` and returns `JobSummary[]`:

```json
{ "job_id", "workpool_id", "created_at", "status", "tasks": [{"state", "count"}], "labels", "expiry" }
```

Changes needed:

- Update the fetch URL.
- Rename `jobID` → `job_id`, `submitTime` → `created_at`, `clusterId` → `workpool_id`.
- Compute `successCount` = sum of `tasks` entries where `state == "success"`.
- Compute `failureCount` = sum of `tasks` entries where `state` is `"error"`, `"failed"`, or `"killed"`.
- Compute `taskCount` = sum of all `tasks` entries.
- `lastUpdated` has no equivalent — remove or drop from the UI.
- Update `BackendJobSummary` in `types.ts`.

### 6. Job detail: field renames and missing fields

`GET /api/v1/job/{jobId}` response changes:

| Old field                  | New field        | Action                 |
| -------------------------- | ---------------- | ---------------------- |
| `cluster_id`               | `workpool_id`    | rename                 |
| `submit_time`              | `created_at`     | rename                 |
| `task_count`               | `task_count`     | no change              |
| `metadata` (object)        | `labels` (array) | see item 7             |
| `status`                   | —                | not in Job doc; drop   |
| `max_preemptable_attempts` | —                | remove; no replacement |
| `target_node_count`        | —                | remove; no replacement |

`JobDetail.status` was not used to drive the active/inactive check in `TaskDetail.tsx` (that
uses the derived event status), so dropping it is safe.

### 7. Labels: `metadata` object → `labels` array

`JobList.tsx` uses `metadata: Record<string, string>` for the faceted label search. The
new API returns `labels: Array<{name: string, value: string}>`. This affects:

- `JobDetail` type in `types.ts`
- `LabelChips` component reads `Object.entries(metadata)`
- `facetIndex` building loop reads `Object.keys(j.metadata)`
- Job list search: `Object.entries(j.metadata).some(...)`

Fix: convert `labels` to a `metadata` map at the fetch site:

```ts
const metadata = Object.fromEntries(job.labels.map((l) => [l.name, l.value]));
```

No changes needed to the UI components themselves.

---

## Moderate Rewrites

### 8. Event type system: task events

This is the most pervasive change. Every file in `src/data/` and several pages rely on
the seven fine-grained task event types. The new model emits a single `task_state_update`
event with `old_state` and `new_state` string fields. The accepted status vocabulary mapping is:

| Old event type                  | New equivalent                                     | New status label |
| ------------------------------- | -------------------------------------------------- | ---------------- |
| `task_claimed`                  | `task_state_update` where `new_state == "claimed"` | `claimed`        |
| `task_exec_started`             | `task_state_update` where `new_state == "running"` | `running`        |
| `task_exec_complete`            | `task_state_update` where `new_state == "writing"` | `writing`        |
| `task_complete` (exit 0)        | `task_state_update` where `new_state == "success"` | `success`        |
| `task_complete` (exit ≠ 0)      | `task_state_update` where `new_state == "error"`   | `error`          |
| `task_orphaned` / `task_failed` | `task_state_update` where `new_state == "failed"`  | `failed`         |
| `task_killed`                   | `task_state_update` where `new_state == "killed"`  | `killed`         |
| `job_started`                   | `job_created`                                      | —                |
| `job_killed`                    | `job_terminated`                                   | —                |

**Files to change:**

- `types.ts`: replace the 7 task event interfaces with a single `TaskStateUpdateEvent`
  (fields: `task_id`, `job_id`, `old_state`, `new_state`); rename `JobStartedEvent` →
  `JobCreatedEvent`, `JobKilledEvent` → `JobTerminatedEvent`.
- `events.ts`: rewrite `deriveStatus()` to read `new_state` directly from the last
  `task_state_update` (much simpler than the event-sequence inference). Rewrite
  `extractTimings()` using the new state names (`running` instead of `exec_started`, etc.).
  `getJobTasks()` is unaffected since `task_id` and `job_id` are still present.
- `EventLog.tsx`, `TaskDetail.tsx`: update rendered status labels and colors to the new
  vocabulary.

The `task_state_update` event does **not** carry `failure_reason`. If the task detail
displays a failure reason it must be fetched from `GET /api/v1/task/{id}` rather than
read from the event stream.

### 9. Live task log and metrics

**Decision: poll `GET /api/v1/task/{id}/log?after={cursor}` every 2 s.**

`useTaskPubsub.ts` currently creates a per-task PubSub subscription and polls a Pub/Sub
pull URL. The new approach:

1. When the task becomes active, call `POST /api/v1/task/{id}/stream` once to activate
   live streaming on the worker.
2. Poll `GET /api/v1/task/{id}/log?after={cursor}&types=log_update,metric_update` every 2 s.
3. Advance `cursor` from the returned `next_after` field.
4. Parse `entries[]` — each entry has `type`, `timestamp`, and either `content` (log) or
   metric fields. The metric field names are identical to the old PubSub payload so
   `toResourceDataPoint` in `useTaskPubsub.ts` can be reused with minor argument changes.

Rename the hook to `useTaskLog.ts`. The PubSub subscription/unsubscribe calls are removed
entirely (no cleanup needed on unmount beyond cancelling the poll loop).

### 10. ClusterDetail page

**Decision: drop the worker-count time series; retain the page as a workpool status view.**

`ClusterDetail.tsx` currently shows a time-series chart of worker counts derived from
`worker_started`/`worker_stopped` events (`clusterTimeSeries.ts`), and a status summary
from `GET /api/v1/clusters/summary`.

Changes:

- Remove the time-series chart and `clusterTimeSeries.ts` entirely.
- Replace the status summary fetch with `GET /api/v1/workpool/{workpool_id}`, which returns
  the workpool's `status`, `status_message`, `last_incident_at`, and `incident_count`.
- Rename the route parameter from `clusterId` to `workpoolId`.
- Keep the event log section (item 2 and item 4 handle the query param rename and type
  filter cleanup).

---

## Backend Addition Required

### 11. `GET /api/v1/job/{job_id}/tasks` — new endpoint

Required for the PerfOverview page. Add to `dashboard_backend.go`.

**Query parameters:**

- `status` — optional comma-separated list; return only tasks in these states (e.g.
  `status=success,error,failed,killed`). This avoids fetching tasks still in progress,
  which don't yet have `resource_usage`.
- `updated_after` — optional RFC3339 timestamp; return only tasks whose status changed
  after this time. Enables incremental polling: cache task results client-side and only
  re-fetch tasks that may have changed.

**Response** `200 OK`:

```json
[
  {
    "task_id": "string",
    "task_index": "integer",
    "status": "string",
    "exit_code": "integer | null",
    "resource_usage": {
      "elapsed_seconds": "float64",
      "max_memory_bytes": "integer",
      "cpu_user_usec": "integer",
      "cpu_system_usec": "integer",
      "block_read_bytes": "integer",
      "block_write_bytes": "integer",
      "oom_killed": "boolean"
    }
  }
]
```

`resource_usage` is omitted when null (task has no recorded usage).

**Firestore**: `Tasks` — query `job_id == {job_id}`, optionally `status in {statuses}`,
optionally filtered client-side by a task's `status` change time. Note: Firestore does not
have a `last_updated` field on Tasks by default; to support `updated_after` filtering the
backend can either add a `last_updated` field written on every state transition, or perform
the filter client-side after fetching all tasks for the job (acceptable for jobs with ≤ a
few thousand tasks).

The simplest correct implementation is to store a `last_updated` timestamp on the Task
document in `FirestoreTaskQueue.UpdateState` (and `RecordError`, `RecordFailed`,
`RecordKilled`), then query `last_updated > updated_after` in addition to the status filter.

---

## Features to Remove

### 12. Worker pool sidebar: replace with minimal version

**Decision: replace, not remove.**

`WorkerPoolCard` and `useWorkerPools` in `JobList.tsx` will be rewritten to use
`GET /api/v1/workpools`. The new card shows:

- Workpool ID, machine type, region
- Status string and status message
- Last incident timestamp and incident count (if non-zero)
- Active worker count from `GET /api/v1/workpool/{id}/workers` (count of `status=started`
  workers)

VM-level breakdown counts (`instanceInUseCount`, `idleInstanceCount`, `orphanedTaskCount`,
`preemptableInstanceCount`, `nonPreemptableInstanceCount`) are removed from the card.
The `WorkerPoolCard` component and `ProportionBar`/`BreakdownRow` helpers can be simplified
substantially. `ClusterStatus` and `ClusterInfo` types in `types.ts` are removed and
replaced with a simpler `WorkpoolSummary` type derived from the workpool response.

### 13. PerfOverview page: keep but rewrite data source

**Decision: keep; rework to fetch tasks via item 11.**

`PerfOverview.tsx` currently derives histograms from `task_complete` events (embedded
resource metrics). Replace the event-based data source with a fetch to
`GET /api/v1/job/{id}/tasks?status=success,error,failed,killed`, then read `resource_usage`
from each task. The histogram computations (`jobPerf.ts`) can remain largely intact — field
names map directly:

| Old event field    | New `resource_usage` field                                      |
| ------------------ | --------------------------------------------------------------- |
| `user_cpu_sec`     | `cpu_user_usec / 1e6`                                           |
| `system_cpu_sec`   | `cpu_system_usec / 1e6`                                         |
| `max_mem_in_gb`    | `max_memory_bytes / 1e9`                                        |
| `block_input_ops`  | `block_read_bytes` (bytes, not ops — axis label needs updating) |
| `block_output_ops` | `block_write_bytes`                                             |

`download_bytes` / `upload_bytes` are gone; remove those histogram panels.

### 14. ClusterLogs page: remove

`ClusterLogs.tsx` calls `GET /api/v1/cluster/{clusterId}/log-summary`, which is not
implemented in the new backend (Cloud Logging integration requires attaching workpool labels
to Batch jobs — see `dashboard-api.md` §"Note" under `/workpool/{id}/batches`). Remove the
page and its route from `App.tsx`.

---

## Non-Issues

- **Task detail fields**: The new `GET /api/v1/task/{id}` response has the same `task_id`,
  `command`, `docker_image` fields the frontend reads. `resource_usage` and `parameters` are
  new additions that the current frontend ignores.
- **Event pagination** (`next_after` cursor): `EventProvider.tsx` already reads
  `data.next_after` correctly.
- **CORS**: The new backend adds CORS headers; no frontend change needed.

---

## Recommended Implementation Order

1. Trivial renames (items 1–4) — mechanical, unblock everything else.
2. Jobs list endpoint + field changes (item 5) — unblocks the main page.
3. Job detail + labels adapter (items 6–7) — unblocks job/task navigation.
4. Event type system rewrite (item 8) — unblocks task timeline and status display.
5. Replace `useTaskPubsub` with `useTaskLog` polling (item 9) — unblocks task metrics/log tabs.
6. Backend: add `GET /api/v1/job/{id}/tasks` with `status` + `updated_after` filters (item 11).
7. Rewrite worker pool sidebar (item 12) and ClusterDetail (item 10).
8. Rewrite PerfOverview to use tasks endpoint (item 13).
9. Remove ClusterLogs (item 14).
