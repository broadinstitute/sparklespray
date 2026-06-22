# datamodel.md Issues

Issues found by comparing `datamodel.md` against the code under `v100/`. Each issue notes whether the doc or the code is likely wrong.

---

## Issue 1 — `metric_update` entries are fully implemented (doc says "not yet implemented")

**Location:** `datamodel.md` line 195

**Doc says:**

> `metric_update` entries _(not yet implemented)_ — the schema below is planned but not written by any current code path

**Code reality:**
`task_metrics.go` and `task_event_log.go` fully implement metric polling. `OpenTaskEventLog` starts a goroutine that fires every minute, calls `collectMetrics`, and writes a `ResourceUsageEvent` via `WriteMetric`. `WriteMetric` follows the same buffered/streaming pattern as `WriteOutput`: JSON to the local events file when not streaming, Firestore `TaskLog.Add()` when streaming.

**Fix:** Remove the "not yet implemented" disclaimer and the note that says these are "not written by any current code path".

---

## Issue 2 — `metric_update` table omits the four common fields

**Location:** `datamodel.md` lines 197–213

The `metric_update` table only shows metric-specific fields. But `ResourceUsageEvent` (`task_metrics.go:20–40`) has four additional fields that every entry carries:

| Field       | Type      | Notes                         |
| ----------- | --------- | ----------------------------- |
| `task_id`   | string    | Same as `log_update`          |
| `type`      | string    | Always `"metric_update"`      |
| `timestamp` | timestamp | When the sample was collected |
| `expiry`    | timestamp | 7 days after `timestamp`      |

These are identical in purpose to the `log_update` common fields but are not listed in the metric_update table. The introductory paragraph for `TaskLog` (line 185) also describes the collection as holding "output entries", which is now inaccurate.

**Fix:** Either add the four common fields to the metric_update table, or restructure the section to document them once as shared fields for all TaskLog entries.

---

## Issue 3 — `Jobs` collection missing `name` field

**Location:** `datamodel.md` lines 13–17

**Code:**

```go
// task_queue.go:97-99
type Job struct {
    JobID      string          `firestore:"job_id"`
    Name       string          `firestore:"name"`
    WorkpoolID string          `firestore:"workpool_id"`
    Resources  []ResourceEntry `firestore:"resources"`
}
```

The `name` field is present in the struct and written to Firestore, but the `Jobs` table in the doc only lists `job_id`, `workpool_id`, and `resources`.

**Fix:** Add `name` (string — human-readable label for the job) to the Jobs collection table. Decide whether the doc or the code is wrong: if `name` is intentional, add it to the doc; if it's vestigial, remove it from the struct.

---

## Issue 4 — `WorkPools` collection missing 10 provisioning/watchdog parameter fields

**Location:** `datamodel.md` lines 86–95

**Code:** `task_queue.go:76–87` defines these fields on the `WorkPool` struct with full `firestore:` tags, but none appear in the documentation:

| Firestore field                   | Go field                       |
| --------------------------------- | ------------------------------ |
| `max_worker_count`                | `MaxWorkerCount`               |
| `max_preemptible_worker_attempts` | `MaxPreemptibleWorkerAttempts` |
| `max_workers_per_request`         | `MaxWorkersPerRequest`         |
| `min_time_between_polls_sec`      | `MinTimeBetweenPollsSec`       |
| `max_time_between_polls_sec`      | `MaxTimeBetweenPollsSec`       |
| `max_time_to_start_worker_sec`    | `MaxTimeToStartWorkerSec`      |
| `max_time_in_queue_sec`           | `MaxTimeInQueueSec`            |
| `vm_shutdown_grace_period_sec`    | `VMShutdownGracePeriodSec`     |
| `max_zombies_before_abort`        | `MaxZombiesBeforeAbort`        |
| `max_consecutive_failed_batches`  | `MaxConsecutiveFailedBatches`  |

These are set when a workpool is provisioned and read by the monitor to control autoscaling and watchdog behavior.

**Fix:** Add a third table (or extend the first) in the `WorkPools` section documenting these provisioning/watchdog parameters. If some of these fields are unimplemented stubs, document that.

---

## Issue 5 — `TaskLog` introductory paragraph is `log_update`-only after metric_update is added

**Location:** `datamodel.md` lines 183–185

**Doc says:**

> Each document represents one chunk of docker stdout/stderr output.

This is accurate only for `log_update` entries. With `metric_update` now implemented, `TaskLog` also holds periodic resource samples unrelated to stdout/stderr.

**Fix:** Broaden the introductory paragraph to say the collection holds two entry types: `log_update` (stdout/stderr chunks) and `metric_update` (periodic resource samples).
