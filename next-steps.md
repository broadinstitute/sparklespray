# Next Steps

Items that are currently stubbed or deferred, with notes on what real implementation would require.

## 1. Task log endpoint (`GET /api/v1/task/{id}/log`)

Currently returns stub text ("not yet implemented") on the first poll and blank content on subsequent polls.

Real implementation requires:

- Worker captures stdout/stderr and writes it to GCS or streams it to Datastore as append-only records.
- Backend endpoint reads the log records for the given task and returns them incrementally, advancing the `next_after` cursor.

## 2. Task metrics endpoint (`GET /api/v1/task/{id}/metrics`)

Currently always returns an empty array.

Real implementation requires:

- Worker emits per-task time-series telemetry (CPU %, memory bytes) at regular intervals (e.g., every 10s).
- Telemetry is stored in Datastore or a time-series store.
- Backend endpoint returns the stored metrics, paginated by `next_after`.
- Until this is real, the TaskDetail metrics tab falls back to simulated charts via `simulate.ts`.

## 3. Command and docker image fields on Task

Currently hardcoded to `"missing"` in the backend when the fields are empty.

Real implementation requires:

- The simulator and real worker write `command` and `docker_image` to the Task Datastore entity when the task is created or claimed.

## 4. System-level OS metrics (iowait, process count, system memory breakdown)

Not captured anywhere. The simulated metrics charts in TaskDetail show these as placeholders.

Real implementation requires:

- A node-level monitoring agent (or worker-side collection) that samples `/proc` and emits events.
- These would feed into the metrics endpoint above.

## 5. Task detail simulated charts

CPU breakdown, memory curves, and process count in TaskDetail use `simulateResourceUsage` from `simulate.ts` as long as the metrics endpoint returns an empty array. Once endpoint #2 is real, the simulated fallback should be removed.
