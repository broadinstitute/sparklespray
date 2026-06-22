# Sparklespray v100 Data Model

This document describes the Firestore collections, Pub/Sub topics, and task lifecycle for the v100 rewrite of Sparklespray.

---

## Firestore Collections

### `Jobs`

One document per submitted job. The document ID is the `job_id`.

| Field         | Type            | Description                                                                                                            |
| ------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `job_id`      | string          | Unique identifier for the job                                                                                          |
| `name`        | string          | Human-readable label for the job (set at submission time)                                                              |
| `workpool_id` | string          | The workpool this job's tasks should be executed in                                                                    |
| `resources`   | []ResourceEntry | Per-task resource requirements (e.g. `slots=1,mem=8`). Workers verify they can satisfy these before claiming any task. |

**ResourceEntry** (embedded object):

| Field   | Type    | Description                         |
| ------- | ------- | ----------------------------------- |
| `name`  | string  | Resource name (e.g. `slots`, `mem`) |
| `value` | float64 | Required quantity of that resource  |

---

### `Tasks`

One document per task. The document ID is the `task_id`.

| Field               | Type             | Description                                                                                |
| ------------------- | ---------------- | ------------------------------------------------------------------------------------------ |
| `task_id`           | string           | Unique identifier for the task                                                             |
| `task_index`        | int              | Zero-based index of this task within its job                                               |
| `job_id`            | string           | ID of the parent job                                                                       |
| `workpool_id`       | string           | The workpool this task belongs to (denormalized from the job for efficient querying)       |
| `status`            | string           | Current status — see table below                                                           |
| `command`           | []string         | The command to execute, as an argv array (e.g. `["python", "train.py", "--epochs", "10"]`) |
| `docker_image`      | string           | Docker image used to run the command                                                       |
| `result_path`       | string           | GCS path (e.g. `gs://bucket/path`) where results are uploaded after the command completes  |
| `log_path`          | string           | GCS path where the command's stdout/stderr is uploaded after the command completes         |
| `files_to_localize` | []FileToLocalize | Files to download from GCS into the working directory before the command runs              |
| `owning_worker_id`  | string           | ID of the worker that has claimed this task; empty when not claimed                        |
| `failure_reason`    | string           | Human-readable reason for failure; populated when `status` is `failed`                     |
| `exit_code`         | int              | Process exit code; populated when `status` is `error`                                      |

**FileToLocalize** (embedded object):

| Field           | Type   | Description                                                                                  |
| --------------- | ------ | -------------------------------------------------------------------------------------------- |
| `source`        | string | GCS path of the file to download (e.g. `gs://bucket/path/file.txt`)                          |
| `destination`   | string | Relative path under the working directory where the file is written (e.g. `inputs/file.txt`) |
| `is_executable` | bool   | If true, the file is made executable after download. Defaults to false if omitted.           |

**Status values:**

Active (non-terminal) states — a task in any of these can be orphaned back to `pending`:

| Status    | Description                                                                                   |
| --------- | --------------------------------------------------------------------------------------------- |
| `pending` | Task is waiting to be claimed by a worker                                                     |
| `claimed` | Worker has claimed the task and is staging it (downloading files, pulling docker image, etc.) |
| `running` | Task process is actively executing                                                            |
| `writing` | Execution finished; worker is uploading results to cloud storage                              |

Terminal states — no further transitions except an administrative kill:

| Status    | Description                                                               |
| --------- | ------------------------------------------------------------------------- |
| `success` | Task ran to completion and the process exited with code 0                 |
| `error`   | Task ran to completion but the process returned a non-zero exit code      |
| `failed`  | Task did not run to completion due to an infrastructure or system failure |
| `killed`  | Task was administratively terminated _(not yet implemented)_              |

**Three-way terminal split:** `success`/`error`/`failed` model two distinct failure modes. `error` means the task executed fully and the _program itself_ reported a problem — look at the task's output. `failed` means execution did not complete — look at the worker or infrastructure logs. `success` means exit code 0.

**Claiming is atomic.** `ClaimTask` uses a Firestore transaction to flip `status` from `pending` → `claimed` and record the `owning_worker_id`. To reduce contention when many workers compete for the same job's tasks, the worker fetches up to 100 pending candidates and shuffles them before attempting the transaction.

---

### `WorkPools`

One document per workpool. The document ID is the `workpool_id`. A workpool defines the VM configuration used to create workers that process tasks associated with that workpool.

| Field           | Type          | Description                                                        |
| --------------- | ------------- | ------------------------------------------------------------------ |
| `workpool_id`   | string        | Unique identifier for the workpool                                 |
| `machine_type`  | string        | GCP machine type for worker VMs (e.g. `n2-standard-4`)             |
| `region`        | string        | GCP region for Batch jobs (e.g. `us-central1`)                     |
| `zones`         | []string      | GCP zones to query for running VMs (e.g. `["us-central1-a"]`)      |
| `root_dir`      | string        | Working directory on the VM where tasks are executed               |
| `resources`     | []Resource    | Resource capacity advertised by workers created from this workpool |
| `empty_volumes` | []EmptyVolume | Ephemeral volumes to attach to each VM                             |
| `expiry`        | timestamp     | When this document may be garbage-collected                        |

The following provisioning and watchdog parameters are set once at workpool creation and read by the monitor to govern autoscaling behaviour:

| Field                             | Type | Description                                                                                  |
| --------------------------------- | ---- | -------------------------------------------------------------------------------------------- |
| `max_worker_count`                | int  | Maximum number of VMs the monitor may have running concurrently for this workpool            |
| `max_preemptible_worker_attempts` | int  | How many times the monitor may submit a preemptible batch before falling back to on-demand   |
| `max_workers_per_request`         | int  | Maximum number of VMs in a single GCP Batch job submission                                   |
| `min_time_between_polls_sec`      | int  | Minimum seconds between provisioning poll iterations                                         |
| `max_time_between_polls_sec`      | int  | Maximum seconds between provisioning poll iterations (when idle)                             |
| `max_time_to_start_worker_sec`    | int  | Seconds after batch submission before a VM that never registered is considered a zombie      |
| `max_time_in_queue_sec`           | int  | Maximum seconds a task may wait in `pending` before the monitor considers the pool unhealthy |
| `vm_shutdown_grace_period_sec`    | int  | Seconds the monitor waits after asking a VM to shut down before treating it as gone          |
| `max_zombies_before_abort`        | int  | Number of zombie VMs tolerated in one batch before the monitor marks the batch failed        |
| `max_consecutive_failed_batches`  | int  | Number of consecutive failed batches before the monitor halts the workpool                   |

The following fields are written exclusively by the monitor process and must not be set at submission time:

| Field              | Type      | Description                                                                                |
| ------------------ | --------- | ------------------------------------------------------------------------------------------ |
| `status`           | string    | Operational health: `idle`, `ok`, `unhealthy`, or `halted`                                 |
| `status_message`   | string    | Human-readable description of the current status or last incident                          |
| `last_incident_at` | timestamp | Time of the most recent watchdog incident                                                  |
| `incident_count`   | int       | Cumulative number of incidents; the monitor halts the workpool if this exceeds a threshold |

**Resource** (embedded object) — mirrors the resource entries on `Jobs`; workers created from this workpool will advertise this capacity:

| Field   | Type    | Description                         |
| ------- | ------- | ----------------------------------- |
| `name`  | string  | Resource name (e.g. `slots`, `mem`) |
| `value` | float64 | Quantity of that resource available |

**EmptyVolume** (embedded object) — ephemeral disk volume created fresh for each VM:

| Field         | Type   | Description                                                   |
| ------------- | ------ | ------------------------------------------------------------- |
| `mount_point` | string | Filesystem path where the volume is mounted (e.g. `/scratch`) |
| `type`        | string | Volume type (e.g. `pd-ssd`, `local-ssd`)                      |
| `size_in_gb`  | int    | Volume size in gibibytes                                      |

---

### `Workers`

One document per active worker process. The document ID is the `worker_id` (a UUID generated at startup).

| Field              | Type      | Description                                                                                                 |
| ------------------ | --------- | ----------------------------------------------------------------------------------------------------------- |
| `worker_id`        | string    | UUID assigned at worker startup                                                                             |
| `workpool_id`      | string    | Workpool this worker serves                                                                                 |
| `batch_id`         | string    | ID of the `BatchAPIRequests` document that spawned this worker                                              |
| `instance_name`    | string    | GCP VM instance name; used by the monitor for surgical VM termination                                       |
| `status`           | string    | `started` or `stopped`                                                                                      |
| `expiry`           | timestamp | Time after which this record can be garbage-collected (set 7 days out at startup; zeroed on clean shutdown) |
| `heartbeat_expiry` | timestamp | Rolling deadline updated every heartbeat period; used to detect crashed workers                             |

The worker updates `heartbeat_expiry` every minute while running. On a clean shutdown the worker sets both `expiry` and `heartbeat_expiry` to the current time and flips `status` to `stopped`.

---

### `Events`

An append-only log of every event published to `sparkles-events`. The document ID is a UUID assigned at write time.

Each event document contains the same fields as the corresponding Pub/Sub message, plus an `expiry` field for TTL-based garbage collection:

| Field       | Type      | Description                                                                                              |
| ----------- | --------- | -------------------------------------------------------------------------------------------------------- |
| `event_id`  | string    | UUID uniquely identifying this event                                                                     |
| `type`      | string    | Event type — `worker_started`, `worker_stopped`, `task_state_update`, `job_created`, or `job_terminated` |
| `timestamp` | timestamp | When the event was recorded                                                                              |
| `expiry`    | timestamp | When this document may be deleted (7-day TTL)                                                            |

Additional fields present on **worker events** (`worker_started`, `worker_stopped`):

| Field         | Type   | Description                |
| ------------- | ------ | -------------------------- |
| `worker_id`   | string | ID of the worker           |
| `workpool_id` | string | Workpool the worker serves |

Additional fields present on **task state update events** (`task_state_update`):

| Field       | Type   | Description                             |
| ----------- | ------ | --------------------------------------- |
| `task_id`   | string | ID of the task that changed state       |
| `job_id`    | string | ID of the parent job                    |
| `old_state` | string | The task's status before the transition |
| `new_state` | string | The task's status after the transition  |

Additional fields present on **job events** (`job_created`, `job_terminated`):

| Field         | Type   | Description                    |
| ------------- | ------ | ------------------------------ |
| `job_id`      | string | ID of the job                  |
| `workpool_id` | string | Workpool the job is running in |

Every write to `sparkles-events` is mirrored to this collection atomically before (or as part of) the publish, so the `Events` collection is the durable record and Pub/Sub is the real-time delivery mechanism.

---

### `TaskLog`

An append-only collection of entries written by workers for in-flight tasks. It holds two entry types: `log_update` (stdout/stderr output chunks) and `metric_update` (periodic resource usage samples). By default tasks buffer entries locally; streaming to this collection is activated per-task by sending a `stream_task_updates` control message to the worker (see [`sparkles-worker-in`](#sparkles-worker-in-control-plane--worker)). Once activated, the worker replays any buffered entries accumulated since the task started, then writes each subsequent entry directly to this collection until the task completes.

The document ID is auto-assigned by Firestore. The TTL is 7 days from the time the entry is written.

All entries share these common fields:

| Field       | Type      | Description                                     |
| ----------- | --------- | ----------------------------------------------- |
| `task_id`   | string    | ID of the task that produced this entry         |
| `type`      | string    | Entry type — `log_update` or `metric_update`    |
| `timestamp` | timestamp | When this entry was captured by the worker      |
| `expiry`    | timestamp | 7 days after `timestamp`; used for TTL-based GC |

**`log_update` entries** — one chunk of docker stdout/stderr output:

| Field     | Type   | Description                                             |
| --------- | ------ | ------------------------------------------------------- |
| `content` | string | Raw stdout/stderr text for this chunk (arbitrary bytes) |

**`metric_update` entries** — a periodic resource usage sample collected once per minute while the task is running:

| Field                     | Type          | Description                                                                                                       |
| ------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------- |
| `process_count`           | int32         | Number of processes visible in `/proc`                                                                            |
| `total_memory`            | int64         | Total virtual memory size across all processes (bytes)                                                            |
| `total_data`              | int64         | Total data-segment size across all processes (bytes)                                                              |
| `total_shared`            | int64         | Total shared memory across all processes (bytes)                                                                  |
| `total_resident`          | int64         | Total resident set size across all processes (bytes)                                                              |
| `cpu_user`                | int64         | Cumulative user-mode CPU time (jiffies) from `/proc/stat`                                                         |
| `cpu_system`              | int64         | Cumulative kernel-mode CPU time (jiffies) from `/proc/stat`                                                       |
| `cpu_idle`                | int64         | Cumulative idle CPU time (jiffies) from `/proc/stat`                                                              |
| `cpu_iowait`              | int64         | Cumulative I/O-wait CPU time (jiffies) from `/proc/stat`                                                          |
| `mem_total`               | int64         | System total memory (bytes) from `/proc/meminfo`                                                                  |
| `mem_available`           | int64         | System available memory (bytes) from `/proc/meminfo`                                                              |
| `mem_free`                | int64         | System free memory (bytes) from `/proc/meminfo`                                                                   |
| `mem_pressure_some_avg10` | int32         | Memory pressure "some" 10-second average × 100 (e.g. 150 = 1.50%) from `/proc/pressure/memory`; -1 if unavailable |
| `mem_pressure_full_avg10` | int32         | Memory pressure "full" 10-second average × 100; -1 if unavailable                                                 |
| `volumes`                 | []VolumeUsage | Disk volume usage snapshots at the time of the update                                                             |

**VolumeUsage** (embedded object):

| Field      | Type    | Description                                     |
| ---------- | ------- | ----------------------------------------------- |
| `location` | string  | Mount path of the volume (e.g. `/`, `/scratch`) |
| `total_gb` | float64 | Total capacity of the volume (GiB)              |
| `used_gb`  | float64 | Space currently used on the volume (GiB)        |

---

### `JobSummary`

One document per job, keyed by `job_id`. This is the **mutable** counterpart to the immutable `Jobs` document: a `Job` is written once at submission and never updated; all evolving state lives here. `JobSummary` is owned exclusively by the **monitor** process, which recomputes it whenever task states change. No other process should write to this collection.

Any question about job progress — "is this job still running?", "how many tasks failed?" — should be answered by reading `JobSummary`, not by scanning `Tasks` or adding derived fields to `Jobs`.

| Field         | Type        | Description                                                  |
| ------------- | ----------- | ------------------------------------------------------------ |
| `job_id`      | string      | ID of the job this summary describes                         |
| `workpool_id` | string      | Workpool the job is running in                               |
| `expiry`      | timestamp   | When this document may be garbage-collected                  |
| `status`      | string      | Rolled-up job status — see table below                       |
| `tasks`       | []TaskCount | Task counts grouped by status; one entry per non-zero status |

**TaskCount** (embedded object):

| Field   | Type   | Description                                                       |
| ------- | ------ | ----------------------------------------------------------------- |
| `state` | string | Task status value (same allowed values as the `Tasks` collection) |
| `count` | int    | Number of tasks currently in that state                           |

**Status values:**

| Status                     | Description                                                                               |
| -------------------------- | ----------------------------------------------------------------------------------------- |
| `pending`                  | All tasks are `pending`; no work has started                                              |
| `in_progress`              | At least one task is active (`claimed`/`running`/`writing`); no `failed` or `error` tasks |
| `in_progress_with_error`   | At least one task is active; at least one task is in `error` state                        |
| `in_progress_with_failure` | At least one task is active; at least one task is in `failed` state                       |
| `killed`                   | At least one task was `killed` _(not yet implemented)_                                    |
| `success`                  | All tasks complete; every terminal task reached `success`                                 |
| `error`                    | All tasks complete; at least one task is in `error` state, none in `failed`               |
| `failed`                   | All tasks complete; at least one task is in `failed` state                                |

---

### `JobSummaryHistory`

An append-only log of `JobSummary` snapshots. Each document is a point-in-time copy written by the monitor process whenever it updates `JobSummary`. The document ID is a UUID assigned at write time.

| Field         | Type        | Description                                                          |
| ------------- | ----------- | -------------------------------------------------------------------- |
| `job_id`      | string      | ID of the job this snapshot describes                                |
| `workpool_id` | string      | Workpool the job is running in                                       |
| `timestamp`   | timestamp   | When this snapshot was recorded                                      |
| `expiry`      | timestamp   | When this document may be garbage-collected                          |
| `status`      | string      | Job status at the time of the snapshot (same values as `JobSummary`) |
| `tasks`       | []TaskCount | Task counts at the time of the snapshot                              |

**TaskCount** is the same embedded object as in `JobSummary`.

---

### `BatchAPIRequests`

One document per GCP Batch job submitted by the monitor. The document ID is the internal `batch_id` (a UUID). This collection is written and read exclusively by the monitor; no other process should modify it.

| Field                     | Type      | Description                                                                              |
| ------------------------- | --------- | ---------------------------------------------------------------------------------------- |
| `batch_id`                | string    | Internal UUID for this batch request (document ID)                                       |
| `job_id`                  | string    | GCP Batch job resource name returned by the Batch API                                    |
| `workpool_id`             | string    | Workpool this batch request belongs to                                                   |
| `expected_vm_count`       | int       | Number of VMs requested in this batch job                                                |
| `preemptible`             | bool      | Whether the batch was submitted as preemptible                                           |
| `submitted_at`            | timestamp | When the batch job was submitted to GCP                                                  |
| `running_since`           | timestamp | When the batch job first reached RUNNING state; absent until then                        |
| `registered_worker_count` | int       | Number of worker processes that have registered for this batch; monotonically increasing |
| `status`                  | string    | Monitor's classification of this batch — `pending`, `started`, `completed`, or `failed`  |
| `unhealthy`               | bool      | Sticky flag set when the monitor detects a problem with this batch; never cleared        |

---

## Pub/Sub Topics

### `sparkles-events` _(Worker → Control plane)_

Published by workers to report lifecycle events. Messages are JSON-encoded. Every message published here is also written to the `Events` Firestore collection.

**WorkerEvent** — published on worker start and stop:

```json
{
  "type": "worker_started" | "worker_stopped",
  "worker_id": "...",
  "workpool_id": "..."
}
```

**JobCreatedEvent** — published when a new job is submitted:

```json
{
  "type": "job_created",
  "job_id": "...",
  "workpool_id": "..."
}
```

The monitor subscribes to this topic via the `monitor-events-in` subscription and triggers an immediate provisioning poll on `job_created` receipt, rather than waiting for the next timer tick. The `dev submit` command creates a short-lived ephemeral subscription (`devsubmit-monitor-<id>`) to log events as they arrive, and deletes it on exit.

**JobTerminatedEvent** — published by the monitor when all tasks in a job have reached a terminal state (`success`, `error`, `failed`, or `killed`). This event signals that the job is done; it does not indicate whether the job succeeded or failed — consumers should query `JobSummary` for that.

```json
{
  "type": "job_terminated",
  "job_id": "...",
  "workpool_id": "..."
}
```

**TaskStateUpdate** — published on every task state transition:

```json
{
  "type": "task_state_update",
  "task_id": "...",
  "job_id": "...",
  "old_state": "...",
  "new_state": "..."
}
```

| `new_state` | `old_state`                         | Meaning                                                  |
| ----------- | ----------------------------------- | -------------------------------------------------------- |
| `claimed`   | `pending`                           | Worker successfully claimed the task; staging begins     |
| `running`   | `claimed`                           | Staging complete; task process launched                  |
| `writing`   | `running`                           | Process exited; uploading results to cloud storage       |
| `success`   | `writing`                           | Upload complete; process exited with code 0              |
| `error`     | `writing`                           | Upload complete; process exited with non-zero code       |
| `failed`    | `claimed` \| `running` \| `writing` | Infrastructure failure; task did not complete            |
| `pending`   | `claimed` \| `running` \| `writing` | Task orphaned back to pending (worker crash detected)    |
| `killed`    | _(any)_                             | Task administratively terminated _(not yet implemented)_ |

---

### `batch-api-notifications` _(GCP Batch API → Monitor)_

Published by GCP Batch API (or the batch API emulator) to notify the monitor when a batch job changes state. The monitor subscribes to this topic under the `batch-api-notifications` subscription. When a notification arrives the monitor immediately runs its tier-2 reconciliation loop (checking job status, reconciling VMs) for the affected batch rather than waiting for the next periodic tick.

Messages are JSON-encoded GCP Batch API state-change notifications:

```json
{
  "jobName": "projects/<project>/locations/<region>/jobs/<job-id>"
}
```

The `jobName` field is a fully-qualified GCP Batch job resource name. The monitor reverse-looks up the internal `batch_id` from the `BatchAPIRequests` Firestore collection using this value.

The topic is configured as the Pub/Sub notification target when the monitor creates each GCP Batch job (via the `PubsubTopic` field on the job spec). The monitor creates this topic and its subscription at startup if they do not already exist.

---

### `sparkles-worker-in` _(Control plane → Worker)_

Used to send control messages to a specific worker. Each worker creates a **per-worker subscription** named `sparkles-worker-in-<worker_id>` at startup and deletes it on clean shutdown.

All messages share a common JSON envelope:

| Field     | Type   | Description                            |
| --------- | ------ | -------------------------------------- |
| `type`    | string | Message type (see below)               |
| `task_id` | string | ID of the task this message applies to |

#### `stream_task_updates`

Activates live streaming of a task's stdout/stderr to the `TaskLog` Firestore collection. The worker:

1. Closes the local events buffer file for the identified task.
2. Replays all buffered `log_update` entries accumulated since the task started by reading the buffer file and writing each entry to `TaskLog`.
3. Sets a flag so that all subsequent output chunks for that task are written directly to `TaskLog` instead of the local buffer.

If no task with the given `task_id` is currently running on the worker, the message is acknowledged and ignored.

---

## Resource Model

Workers and jobs both express resources as a set of named floating-point values (e.g. `slots=1`, `mem=8.0`).

- **Worker capacity** is provided at launch via `--resources` (e.g. `--resources slots=4,mem=32`). Defaults to `slots=1`.
- **Job requirements** are stored in the `Jobs` document and checked before any task from that job is claimed.

Before claiming tasks for a job the worker verifies that `worker_capacity - job_requirements >= 0` for every resource. If the job's requirements exceed the worker's _total_ capacity (not just what's currently free), the worker immediately fails every pending task in that job with the message _"Task requires more resources than allowed by worker pool"_ and moves on.

While tasks are running the worker tracks available capacity and waits for a running task to complete before claiming another if resources are exhausted.

---

## Task Lifecycle

```
                 ┌─────────┐
                 │ pending │ ◄──────────────────────────────────────┐
                 └────┬────┘                                        │
                      │ → claimed                                   │ → pending
                      ▼                                             │ (orphaned)
                 ┌─────────┐                                        │
                 │ claimed │ ───────────────────────────────────────┤
                 └────┬────┘  (staging: downloading files,          │
                      │        pulling image, etc.)                 │
                      │ → running                                   │
                      ▼                                             │
                 ┌─────────┐                                        │
                 │ running │ ───────────────────────────────────────┤
                 └────┬────┘  (process actively executing)          │
                      │ → writing                                   │
                      ▼                                             │
                 ┌─────────┐                                        │
                 │ writing │ ───────────────────────────────────────┘
                 └────┬────┘  (uploading results to cloud storage)
          ┌───────────┼───────────┐
          │           │           │ → failed (from any active state)
       → success   → error        ▼
          │           │      ┌────────┐
          ▼           ▼      │ failed │
     ┌─────────┐ ┌───────┐   └────────┘
     │ success │ │ error │
     └─────────┘ └───────┘
      (terminal)  (terminal)  (terminal)

     ┌────────┐
     │ killed │  (administrative kill, from any state) (not yet implemented)
     └────────┘
      (terminal)
```

### States

**`pending`** — Initial state for every task. The task is waiting to be picked up by a worker.

**`claimed`** — A worker has taken ownership and is staging the task: downloading any required input files, pulling the Docker image, and doing any other preparation before execution starts.

**`running`** — The task process is actively executing.

**`writing`** — The task process has exited and the worker is uploading results to cloud storage.

**`success`** — The task ran to completion and the process exited with code 0. Terminal.

**`error`** — The task ran to completion but the process returned a non-zero exit code. The process finished; look at the task's own output to understand the failure. Terminal.

**`failed`** — The task did not run to completion due to an infrastructure or system failure (worker crash, resource mismatch, timeout, etc.). Look at the worker or infrastructure logs. Terminal.

**`killed`** — The task was administratively terminated. Terminal. _(not yet implemented)_

**Three-way terminal split:** `success`/`error`/`failed` distinguish two fundamentally different kinds of failure. `error` means _the program_ reported a problem; `failed` means _the infrastructure_ prevented the program from completing.

### Transitions

Every state transition publishes a `task_state_update` event to `sparkles-events` and appends a corresponding document to the `Events` collection.

1. **`pending` → `claimed`**  
   A worker atomically claims the task via a Firestore transaction. The transaction re-reads the document and only commits if the task is still `pending`, so only one worker can succeed under concurrent competition. Staging begins immediately after.

2. **`claimed` → `running`**  
   The worker has finished staging (downloads, image pull) and launches the task process.

3. **`running` → `writing`**  
   The task process has exited. The worker begins uploading results to cloud storage.

4. **`writing` → `success`**  
   Upload complete; the process exited with code 0.

5. **`writing` → `error`**  
   Upload complete; the process exited with a non-zero code. `exit_code` is recorded in Firestore.

6. **Any active state → `failed`**  
   The worker calls `RecordFailed` when an infrastructure or system error prevents the task from completing. `failure_reason` is populated and `owning_worker_id` is cleared. Can occur from `claimed`, `running`, or `writing`.

7. **Any active state → `pending`** _(planned)_  
   A watchdog process periodically scans for worker records whose `heartbeat_expiry` has passed. For each crashed worker, any task in an active state (`claimed`, `running`, or `writing`) is reset to `pending` so it can be retried. Not yet implemented in v100.

8. **Any state → `killed`** _(not yet implemented)_  
   An external administrative action. `owning_worker_id` is cleared.

### Worker perspective

```
for each job that has pending tasks:
    if job.resources > worker.total_capacity:
        fail all pending tasks in job          # they can never run here
        continue

    while job has pending tasks:
        wait until worker has enough free capacity
        claim a task  →  stage → run → write (async)
        on completion: free resources, record success/error/failed

drain all in-flight tasks
exit
```
