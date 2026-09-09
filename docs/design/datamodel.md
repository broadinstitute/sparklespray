# Sparklespray v100 Data Model

This document describes the Firestore collections, Pub/Sub topics, and task lifecycle for the v100 rewrite of Sparklespray.

**Naming convention:** primary collection documents (`Tasks`, `Workers`, `BatchAPIRequests`) use a field named **`status`** for their lifecycle field. Rolled-up views and events (`JobSummary`, `WorkPoolSummary`, `StateCount`, `task_state_update`) use **`state`** / `old_state` / `new_state`. The split is intentional: `status` = raw per-document state; `state` = derived or aggregated state.

---

## Firestore Collections

### `Jobs`

One document per submitted job. The document ID is the `job_id`.

| Field         | Type            | Description                                                                                                            |
| ------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `job_id`      | string          | Unique identifier for the job                                                                                          |
| `name`        | string          | Human-readable label for the job (set at submission time)                                                              |
| `workpool_id` | string          | The workpool this job's tasks should be executed in                                                                    |
| `created_at`  | timestamp       | When the job was submitted                                                                                             |
| `expiry`      | timestamp       | When this document may be garbage-collected (set 7 days out at submission time)                                        |
| `task_count`  | int             | Number of tasks in this job (denormalized at submission time)                                                          |
| `resources`   | []ResourceEntry | Per-task resource requirements (e.g. `slots=1,mem=8`). Workers verify they can satisfy these before claiming any task. |
| `labels`      | []Label         | User-defined key/value tags attached at submission time (e.g. `experiment=v3`, `owner=alice`)                          |

**ResourceEntry** (embedded object):

| Field   | Type    | Description                         |
| ------- | ------- | ----------------------------------- |
| `name`  | string  | Resource name (e.g. `slots`, `mem`) |
| `value` | float64 | Required quantity of that resource  |

**Label** (embedded object) — user-defined tag; used on `Jobs`, `JobSummary`, `JobSummaryHistory`, `Tasks`, and `WorkPools`:

| Field   | Type   | Description |
| ------- | ------ | ----------- |
| `name`  | string | Tag name    |
| `value` | string | Tag value   |

---

### `Tasks`

One document per task. The document ID is the `task_id`.

| Field                        | Type             | Description                                                                                                                                                 |
| ---------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `task_id`                    | string           | Unique identifier for the task                                                                                                                              |
| `task_index`                 | int              | Zero-based index of this task within its job                                                                                                                |
| `job_id`                     | string           | ID of the parent job                                                                                                                                        |
| `workpool_id`                | string           | The workpool this task belongs to (denormalized from the job for efficient querying)                                                                        |
| `status`                     | string           | Current status — see table below                                                                                                                            |
| `command`                    | []string         | The command to execute, as an argv array (e.g. `["python", "train.py", "--epochs", "10"]`)                                                                  |
| `docker_image`               | string           | Docker image used to run the command                                                                                                                        |
| `result_path`                | string           | GCS path (e.g. `gs://bucket/path`) where results are uploaded after the command completes                                                                   |
| `log_path`                   | string           | GCS path where the command's stdout/stderr is uploaded after the command completes                                                                          |
| `files_to_localize`          | []FileToLocalize | Files to download from GCS into the working directory before the command runs. Mutually exclusive with `files_to_localize_manifest`.                        |
| `files_to_localize_manifest` | string           | GCS path of a manifest file listing files to localize; used instead of inline `files_to_localize` when the file list is too large to embed in the document. |
| `labels`                     | []Label          | User-defined key/value pairs passed to the task at submission time                                                                                          |
| `owning_worker_id`           | string           | ID of the worker that has claimed this task; empty when not claimed                                                                                         |
| `failure_reason`             | string           | Human-readable reason for failure; populated when `status` is `failed`                                                                                      |
| `exit_code`                  | int              | Process exit code; populated when `status` is `error`                                                                                                       |
| `resource_usage`             | ResourceUsage    | Summary of resources consumed by this task's container; written by the worker after the container exits (best-effort; absent if collection failed)          |
| `last_updated`               | timestamp        | Updated on every status transition; used by tooling to detect stale documents                                                                               |
| `expiry`                     | timestamp        | When this document may be garbage-collected (set 7 days out at submission time)                                                                             |

**FileToLocalize** (embedded object):

| Field           | Type   | Description                                                                                  |
| --------------- | ------ | -------------------------------------------------------------------------------------------- |
| `source`        | string | GCS path of the file to download (e.g. `gs://bucket/path/file.txt`)                          |
| `destination`   | string | Relative path under the working directory where the file is written (e.g. `inputs/file.txt`) |
| `is_executable` | bool   | If true, the file is made executable after download. Defaults to false if omitted.           |

**ResourceUsage** (embedded object) — written by the worker once per task. It is assembled from `docker inspect` (timing and exit) plus the metrics poller's **final sample**, taken after the container exits but before `docker rm` is called, while its cgroup still exists.

Because the cgroup counters are cumulative, this summary covers the whole run — including whatever happened between the last periodic `metric_update` and exit. The `container_*` fields carry exactly the same names as in `metric_update` entries, so the summary and the last periodic sample are directly comparable.

Requires **cgroup v2**; see the note on unavailable values below the `metric_update` table.

| Field                                  | Type      | Description                                                                                                                             |
| -------------------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `start_time`                           | timestamp | Container start time (`docker inspect .State.StartedAt`)                                                                                |
| `end_time`                             | timestamp | Container finish time (`docker inspect .State.FinishedAt`)                                                                              |
| `elapsed_seconds`                      | float64   | Wall-clock duration in seconds (`end_time - start_time`); the denominator that makes the counters below interpretable                   |
| `exit_code`                            | int       | Container exit code (`docker inspect .State.ExitCode`)                                                                                  |
| `oom_killed`                           | bool      | True if the container's **main process** was OOM-killed (`docker inspect .State.OOMKilled`)                                             |
| `container_memory_oom_kill_count`      | int64     | Count of OOM-killed processes in the cgroup, **including children** (`memory.events oom_kill`) — catches kills that `oom_killed` misses |
| `container_cpu_usage_usec`             | int64     | Total CPU time, user + system (`cpu.stat usage_usec`)                                                                                   |
| `container_cpu_user_usec`              | int64     | User-mode CPU time (`cpu.stat user_usec`)                                                                                               |
| `container_cpu_system_usec`            | int64     | Kernel-mode CPU time (`cpu.stat system_usec`)                                                                                           |
| `container_cpu_throttled_usec`         | int64     | Time runnable but denied CPU by a quota (`cpu.stat throttled_usec`); non-zero only when a CPU limit is set                              |
| `container_cpu_throttled_periods`      | int64     | Number of throttled scheduling periods (`cpu.stat nr_throttled`)                                                                        |
| `container_memory_peak_bytes`          | int64     | Peak memory high-water mark (`memory.peak`; needs kernel ≥ 5.19)                                                                        |
| `container_memory_limit_bytes`         | int64     | Memory limit (`memory.max`); unavailable when unlimited                                                                                 |
| `container_memory_major_faults`        | int64     | Major (disk-backed) page faults (`memory.stat pgmajfault`)                                                                              |
| `container_memory_workingset_refaults` | int64     | Pages evicted then faulted back in (`memory.stat workingset_refault_file` + `_anon`) — distinguishes real thrashing from cold reads     |
| `container_cpu_stall_some_usec`        | int64     | Cumulative µs at least one task was stalled waiting for CPU (`cpu.pressure`) — reveals a task starved by an oversubscribed host         |
| `container_cpu_stall_full_usec`        | int64     | Cumulative µs _all_ tasks were CPU-stalled; frequently unavailable, as many kernels emit no `full` line for `cpu.pressure`              |
| `container_memory_stall_some_usec`     | int64     | Cumulative µs stalled on memory (`memory.pressure`) — separates thrashing near the limit from a slow disk                               |
| `container_memory_stall_full_usec`     | int64     | Cumulative µs all tasks were memory-stalled                                                                                             |
| `container_io_stall_some_usec`         | int64     | Cumulative µs stalled on I/O (`io.pressure`) — **this is "how long was the task blocked on I/O"**                                       |
| `container_io_stall_full_usec`         | int64     | Cumulative µs all tasks were I/O-stalled                                                                                                |
| `container_io_read_bytes`              | int64     | Bytes read from block devices (`io.stat rbytes`)                                                                                        |
| `container_io_write_bytes`             | int64     | Bytes written to block devices (`io.stat wbytes`)                                                                                       |
| `container_io_read_ops`                | int64     | Read operations (`io.stat rios`); with bytes, distinguishes many-small-random from few-big-sequential I/O                               |
| `container_io_write_ops`               | int64     | Write operations (`io.stat wios`)                                                                                                       |
| `container_pids_peak`                  | int64     | Peak process/thread count (`pids.peak`, kernel ≥ 6.1; else `pids.current`) — reveals unexpected fan-out                                 |

Quantities deliberately **not** stored, because they are derivable and belong at display time: CPU efficiency (`container_cpu_usage_usec / (elapsed_seconds × cores)`), I/O stall fraction (`container_io_stall_full_usec / elapsed_seconds`), memory headroom (`container_memory_peak_bytes / container_memory_limit_bytes`), and **unaccounted wait** (`elapsed − cpu_usage − all stalls`), which is how time blocked on the network is inferred — a socket wait is a voluntary sleep, so it appears in neither `iowait` nor `io.pressure`.

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
| `killed`  | Task was administratively terminated                                      |

**Three-way terminal split:** `success`/`error`/`failed` model two distinct failure modes. `error` means the task executed fully and the _program itself_ reported a problem — look at the task's output. `failed` means execution did not complete — look at the worker or infrastructure logs. `success` means exit code 0.

**Claiming is atomic.** `ClaimTask` uses a Firestore transaction to flip `status` from `pending` → `claimed` and record the `owning_worker_id`. To reduce contention when many workers compete for the same job's tasks, the worker fetches up to 100 pending candidates and shuffles them before attempting the transaction.

---

### `WorkPools`

One document per workpool. The document ID is the `workpool_id`. `WorkPool` is written once at creation and never updated; all evolving state lives in `WorkPoolSummary`. A workpool defines the VM configuration used to create workers that process tasks associated with that workpool.

| Field                             | Type            | Description                                                                                                    |
| --------------------------------- | --------------- | -------------------------------------------------------------------------------------------------------------- |
| `workpool_id`                     | string          | Unique identifier for the workpool                                                                             |
| `project_id`                      | string          | GCP project to create this workpool's Batch jobs/VMs in; empty means the monitor's own project                 |
| `machine_type`                    | string          | GCP machine type for worker VMs (e.g. `n2-standard-4`)                                                         |
| `region`                          | string          | GCP region for Batch jobs (e.g. `us-central1`)                                                                 |
| `zones`                           | []string        | GCP zones to query for running VMs (e.g. `["us-central1-a"]`)                                                  |
| `root_dir`                        | string          | Directory on the VM that the worker uses as its working root; also where the `sparkles` binary is staged       |
| `sparkles_worker_gcs_path`        | string          | GCS path (e.g. `gs://bucket/sparkles`) of the worker binary; downloaded to `{root_dir}/sparkles` at VM startup |
| `service_account`                 | string          | GCP service account email assigned to worker VMs; governs what GCP resources each worker can access            |
| `resources`                       | []ResourceEntry | Resource capacity advertised by workers created from this workpool                                             |
| `empty_volumes`                   | []EmptyVolume   | Ephemeral volumes to attach to each VM                                                                         |
| `labels`                          | []Label         | User-defined key/value tags attached at creation time (e.g. `team=ml`, `env=prod`)                             |
| `expiry`                          | timestamp       | When this document may be garbage-collected                                                                    |
| `max_worker_count`                | int             | Maximum number of VMs the monitor may have running concurrently for this workpool                              |
| `max_preemptible_worker_attempts` | int             | How many times the monitor may submit a preemptible batch before falling back to on-demand                     |
| `max_workers_per_request`         | int             | Maximum number of VMs in a single GCP Batch job submission                                                     |
| `vm_shutdown_grace_period_sec`    | int             | Seconds the monitor waits after asking a VM to shut down before treating it as gone                            |
| `max_zombies_before_abort`        | int             | Number of zombie VMs tolerated in one batch before the monitor marks the batch failed                          |
| `max_consecutive_failed_batches`  | int             | Number of consecutive failed batches before the monitor halts the workpool                                     |

**ResourceEntry** (embedded object) — same type as `ResourceEntry` on `Jobs`; workers created from this workpool will advertise this capacity:

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
| `status`           | string    | `started`, `stopped` (clean shutdown), or `zombie` (heartbeat expired without a clean shutdown)             |
| `expiry`           | timestamp | Time after which this record can be garbage-collected (set 7 days out at startup; zeroed on clean shutdown) |
| `heartbeat_expiry` | timestamp | Rolling deadline updated every heartbeat period; used to detect crashed workers                             |

The worker updates `heartbeat_expiry` every minute while running. On a clean shutdown the worker sets both `expiry` and `heartbeat_expiry` to the current time and flips `status` to `stopped`. If `heartbeat_expiry` passes without a clean shutdown, task recovery (the monitor's `runRequeueOrphanedTasks` poller) flips `status` to `zombie` instead — distinguishing "crashed or preempted" from "shut down cleanly" — and records a `workpool_incident` event.

---

### `WorkPoolSummary`

One document per workpool, keyed by `workpool_id`. `WorkPoolSummary` is the mutable counterpart to the immutable `WorkPool` document: a `WorkPool` is written once at creation and never updated; all evolving state lives here. `WorkPoolSummary` is written exclusively by the **monitor** process, which recomputes it on each provisioning poll. No other process should write to this collection. Even more strict: the "worker pool summary poll" process is the only one who should insert/update objects in this collection. If another process needs to record information which can not be determined by querying a snapshot of the db, an event should be written which the polling process can use.

(Concrete example: The work pool summary poller skips pools idle pools to reduce work. However, we need a way to transition from idle -> ok, which we want to do when a new job is submitted. Instead of having the job submission directly update the workpool, we insert a new_job event, which the poller sees and uses to determine the pool should be switched to "ok")

Any question about workpool health — "how many VMs are expected?", "are there unhealthy batches?" — should be answered by reading `WorkPoolSummary`, not by scanning `BatchAPIRequests`, `Workers`, or `Tasks` directly.

The following fields are copied from `WorkPool` at first write and not updated thereafter:

| Field                             | Type   | Description                                                                                 |
| --------------------------------- | ------ | ------------------------------------------------------------------------------------------- |
| `workpool_id`                     | string | Workpool this summary describes (copied from `WorkPool`)                                    |
| `machine_type`                    | string | GCP machine type for worker VMs (copied from `WorkPool`)                                    |
| `max_preemptible_worker_attempts` | int    | Max preemptible batch submissions before falling back to on-demand (copied from `WorkPool`) |

The following fields are written exclusively by the monitor process:

| Field                             | Type         | Description                                                                                                 |
| --------------------------------- | ------------ | ----------------------------------------------------------------------------------------------------------- |
| `expiry`                          | timestamp    | When this document may be garbage-collected                                                                 |
| `last_updated`                    | timestamp    | When these metrics were last computed by the monitor                                                        |
| `state`                           | string       | Operational health: `idle`, `ok`, `unhealthy`, or `halted`                                                  |
| `state_message`                   | string       | Human-readable description of the current state or last incident                                            |
| `last_incident_at`                | timestamp    | Time of the most recent watchdog incident                                                                   |
| `incident_count`                  | int          | Cumulative number of incidents; the monitor halts the workpool if this exceeds a threshold                  |
| `expected_preemptible_workers`    | int          | Sum of `expected_vm_count` across active (`pending`/`started`) `BatchAPIRequests` where `preemptible=true`  |
| `expected_nonpreemptible_workers` | int          | Sum of `expected_vm_count` across active (`pending`/`started`) `BatchAPIRequests` where `preemptible=false` |
| `unhealthy_batch_count`           | int          | Number of `BatchAPIRequests` documents with `unhealthy=true`                                                |
| `batch_api_request_counts`        | []StateCount | Per-state counts of `BatchAPIRequests` documents; one entry per non-zero state (`pending`, `started`, etc.) |
| `preemptible_workers`             | []StateCount | Per-state counts of preemptible `Workers` documents; one entry per non-zero state (`started`, `stopped`)    |
| `nonpreemptible_workers`          | []StateCount | Per-state counts of non-preemptible `Workers` documents; one entry per non-zero state                       |
| `tasks`                           | []StateCount | Per-state counts of `Tasks` documents belonging to this workpool; one entry per non-zero state              |

**StateCount** (embedded object):

| Field   | Type   | Description                       |
| ------- | ------ | --------------------------------- |
| `state` | string | State value                       |
| `count` | int    | Number of documents in that state |

---

### `WorkPoolSummaryHistory`

An append-only log of `WorkPoolSummary` snapshots. Each document is a point-in-time copy written by the monitor process whenever it updates `WorkPoolSummary`. The document ID is a UUID assigned at write time.

| Field                             | Type         | Description                                                                        |
| --------------------------------- | ------------ | ---------------------------------------------------------------------------------- |
| `workpool_id`                     | string       | Workpool this snapshot describes                                                   |
| `timestamp`                       | timestamp    | When this snapshot was recorded                                                    |
| `expiry`                          | timestamp    | When this document may be garbage-collected                                        |
| `state`                           | string       | Copied from `WorkPoolSummary.state` at snapshot time                               |
| `state_message`                   | string       | Copied from `WorkPoolSummary.state_message` at snapshot time                       |
| `last_incident_at`                | timestamp    | Copied from `WorkPoolSummary.last_incident_at` at snapshot time                    |
| `incident_count`                  | int          | Copied from `WorkPoolSummary.incident_count` at snapshot time                      |
| `expected_preemptible_workers`    | int          | Copied from `WorkPoolSummary` at snapshot time                                     |
| `expected_nonpreemptible_workers` | int          | Copied from `WorkPoolSummary` at snapshot time                                     |
| `unhealthy_batch_count`           | int          | Copied from `WorkPoolSummary` at snapshot time                                     |
| `batch_api_request_counts`        | []StateCount | Per-state counts of `BatchAPIRequests` at the time of the snapshot                 |
| `preemptible_workers`             | []StateCount | Per-state counts of preemptible `Workers` at the time of the snapshot              |
| `nonpreemptible_workers`          | []StateCount | Per-state counts of non-preemptible `Workers` at the time of the snapshot          |
| `tasks`                           | []StateCount | Per-state counts of `Tasks` belonging to this workpool at the time of the snapshot |

**StateCount** is the same embedded object as in `WorkPoolSummary`.

---

### `Events`

An append-only log of every event published to `sparkles-events`. The document ID is a UUID assigned at write time.

Each event document contains the same fields as the corresponding Pub/Sub message, plus an `expiry` field for TTL-based garbage collection:

| Field       | Type      | Description                                                                                                                                                                               |
| ----------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `event_id`  | string    | UUID uniquely identifying this event                                                                                                                                                      |
| `type`      | string    | Event type — `worker_started`, `worker_stopped`, `task_state_update`, `job_created`, `job_terminated`, `workpool_state_change`, `batch_failed`, `batch_succeeded`, or `workpool_incident` |
| `timestamp` | timestamp | When the event was recorded                                                                                                                                                               |
| `expiry`    | timestamp | When this document may be deleted (7-day TTL)                                                                                                                                             |

Additional fields present on **worker events** (`worker_started`, `worker_stopped`):

| Field                | Type   | Description                                                                                                                           |
| -------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| `worker_id`          | string | ID of the worker                                                                                                                      |
| `workpool_id`        | string | Workpool the worker serves                                                                                                            |
| `cleanly_terminated` | bool   | (`worker_stopped` only) `true` when the worker self-reported a normal shutdown; `false` when task recovery marked it a zombie instead |

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

Additional fields present on **workpool state change events** (`workpool_state_change`):

| Field           | Type   | Description                                                                                     |
| --------------- | ------ | ----------------------------------------------------------------------------------------------- |
| `workpool_id`   | string | ID of the workpool whose state changed                                                          |
| `new_state`     | string | The workpool's new state — `idle`, `ok`, `unhealthy`, or `halted`                               |
| `state_message` | string | Human-readable description of the state or incident; empty when transitioning to `idle` or `ok` |

Additional fields present on **batch outcome events** (`batch_failed`, `batch_succeeded`):

| Field           | Type   | Description                                                                                       |
| --------------- | ------ | ------------------------------------------------------------------------------------------------- |
| `workpool_id`   | string | ID of the workpool the batch attempt belongs to                                                   |
| `state_message` | string | (`batch_failed` only) Human-readable failure reason, e.g. the error returned by the GCP Batch API |

One `batch_failed`/`batch_succeeded` event is published per **batch attempt
outcome**, not per worker — a batch that registers several workers still
counts as a single success. `batch_failed` is published both when an
already-created batch is later judged to have failed (no workers ever
registered, GCP reported a job failure, too many zombie workers, etc. — see
`markBatchFailed` in `v100/monitor/monitor.go`) and when the initial
`CreateJob` call to the GCP Batch API itself fails synchronously, before any
`BatchAPIRequest` document exists (`submitBatch` in
`v100/monitor/provision.go`) — the latter case has no corresponding
`BatchAPIRequest` at all, since no batch was ever created.

`batch_succeeded` is published once per batch, the first time it's confirmed
to have at least one worker registered (`checkBatchStartup` in
`v100/monitor/batch_startup_monitor.go`).

The monitor's `checkHaltThreshold` (`v100/monitor/monitor.go`) queries this
collection — filtered to `type in [batch_failed, batch_succeeded]` for a
workpool, within the last hour — to decide whether to halt a workpool: if the
most recent `MaxConsecutiveFailedBatches` outcomes in that window are all
`batch_failed`, the workpool transitions to `halted`. This replaced an
earlier version of the check that queried `BatchAPIRequests` directly, which
could never see synchronous `CreateJob` failures since those never produce a
`BatchAPIRequest` document.

Additional fields present on **workpool incident events** (`workpool_incident`):

| Field           | Type   | Description                                    |
| --------------- | ------ | ---------------------------------------------- |
| `workpool_id`   | string | ID of the workpool the anomaly was detected on |
| `state_message` | string | Human-readable description of the anomaly      |

`workpool_incident` is published by `recordIncident` (`v100/monitor/monitor.go`)
every time the watchdog (task recovery/cluster reconciler/batch startup
monitor) detects a batch/worker anomaly — a batch failing outright, a subset
of VMs failing to register within the grace period, a zombie VM being
terminated by the cluster reconciler (heartbeat expired but the VM is still
running per GCP), or a worker's own heartbeat expiring without a clean
shutdown (task recovery marks it `zombie` — see the `Workers` collection
above; a distinct, Worker-`status`-level concept from the cluster
reconciler's VM-level zombie check). `recordIncident` is purely a
log-to-Events operation; it does not mutate `WorkPoolState` itself. Unlike
`batch_failed`/`batch_succeeded`, halting itself is **not** published as a
`workpool_incident` — it's reported only via `workpool_state_change`.

`WorkPoolSummary`'s `state_message`/`last_incident_at`/`incident_count`
fields, _and_ the `ok`↔`unhealthy` portion of `WorkPoolState.State` itself,
are all derived together by querying this collection for `workpool_incident`
events within the last hour each time the WorkPool summary poll runs
(`updateWorkPoolSummary` in `v100/monitor/workpool_summary_poll.go`) — a
workpool with one or more recent incidents is `unhealthy`; with none, it's
`ok`. None of this is carried as persisted mutable state between polls; it's
a **windowed** view recomputed fresh each time, so it self-heals as old
incidents age out of the one-hour window rather than accumulating as an
all-time count. (`idle`/`halted`, the other two `WorkPoolState.State` values,
are decided independently — see `cluster-health.md`.)

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

**`metric_update` entries** — one resource usage sample.

Sampling is **adaptive**: the first sample is taken 1 second after the task starts and the delay then doubles up to a 60-second ceiling (1, 2, 4, 8, 16, 32, 60, 60, …). This replaced a fixed one-minute interval, under which any task shorter than a minute produced _no samples at all_. One further sample is taken after the container exits, flagged `final`.

Each sample carries both **host** metrics (the environment the task ran in — relevant because one worker runs several containers per VM) and **container** metrics (what this task itself did).

| Field                                              | Type         | Description                                                                                     |
| -------------------------------------------------- | ------------ | ----------------------------------------------------------------------------------------------- |
| `metric_schema`                                    | int32        | Metric layout version. Readers **must** skip samples with an unexpected value — see below       |
| `seq`                                              | int32        | 0-based sample index within the task, so a consumer can detect a lost sample                    |
| `final`                                            | bool         | True for the single post-exit sample; that one also becomes the task's `resource_usage` summary |
| `host_cpu_user_pct`                                | float64      | Host user-mode CPU, % of total across all cores since the previous sample (`/proc/stat`)        |
| `host_cpu_system_pct`                              | float64      | Host kernel-mode CPU %                                                                          |
| `host_cpu_idle_pct`                                | float64      | Host idle %                                                                                     |
| `host_cpu_iowait_pct`                              | float64      | Host I/O-wait %; retained so the four CPU percentages sum to 100                                |
| `host_cpu_count`                                   | int64        | Number of logical cores on the host, for interpreting the CPU percentages above                 |
| `host_memory_total_bytes`                          | int64        | Host `MemTotal` (`/proc/meminfo`)                                                               |
| `host_memory_available_bytes`                      | int64        | Host `MemAvailable`                                                                             |
| `host_cpu_stall_{some,full}_usec`                  | int64        | Cumulative host CPU stall µs (`/proc/pressure/cpu`)                                             |
| `host_memory_stall_{some,full}_usec`               | int64        | Cumulative host memory stall µs (`/proc/pressure/memory`)                                       |
| `host_io_stall_{some,full}_usec`                   | int64        | Cumulative host I/O stall µs (`/proc/pressure/io`)                                              |
| `host_volumes`                                     | []HostVolume | Disk usage per mount point at sample time                                                       |
| `container_memory_current_bytes`                   | int64        | Current container memory (`memory.current`)                                                     |
| `container_cpu_{usage,user,system}_usec`           | int64        | Cumulative container CPU time (`cpu.stat`)                                                      |
| `container_{cpu,memory,io}_stall_{some,full}_usec` | int64        | Cumulative container stall µs (`{cpu,memory,io}.pressure`)                                      |
| `container_io_{read,write}_{bytes,ops}`            | int64        | Cumulative container block I/O (`io.stat`)                                                      |

Samples also repeat the high-water and cumulative container fields documented under **ResourceUsage** above (`container_memory_peak_bytes`, `container_memory_limit_bytes`, `container_cpu_throttled_usec`, `container_cpu_throttled_periods`, `container_memory_major_faults`, `container_memory_workingset_refaults`, `container_memory_oom_kill_count`, `container_pids_peak`). Carrying them in every sample shows _when_ throttling or an OOM kill occurred rather than only that it did, and it lets the final summary be a pure projection of the last sample with no second pass over the cgroup.

**Two conventions worth understanding:**

- **A missing field means "unavailable", not zero.** A metric the kernel does not expose is a nil pointer, omitted from both the JSON and Firestore encodings, so it stays distinguishable from a genuine zero: "did no I/O" and "this kernel has no I/O accounting" are different facts. Common causes are a kernel too old for a given file (`memory.peak` needs ≥ 5.19, `pids.peak` ≥ 6.1), a `cpu.pressure` file with no `full` line (widespread), an unlimited `memory.max`, or cgroup v1 (unsupported — all container fields are unavailable). There is no separate "container present" flag: a task that never had a container, or whose cgroup could not be located, simply has every `container_*` field unavailable, the same convention as any other missing metric. The one sample where this is handled specially is the final one: any field it could not read itself (e.g. because the container's cgroup was torn down between the read and the sample being taken) is backfilled from the last periodic sample that did read it, so the final summary doesn't lose real data to that race.
- **PSI stalls are stored as cumulative `total=` microseconds, not `avg10`.** An average has a ~10-second ramp, which makes it meaningless for a task that runs for three seconds — precisely the case this design set out to fix. Totals difference exactly over any interval (`total[t] − total[t−1]`) and the post-exit read is the _same counter_, so the periodic samples and the final summary compose into one gap-free series. Averages are derivable from totals; totals are not recoverable from averages. Consumers wanting a rate should difference consecutive samples and divide by the elapsed wall time between them.

Because sample spacing is deliberately non-uniform, any chart must use a time-scaled x-axis; a categorical axis would badly distort the early, most detailed part of every task.

**Schema versioning.** `metric_schema` exists because `TaskLog` documents live for 7 days, so after a rollout the collection holds samples in both the old and new layouts. A reader that decodes an old document into the current shape gets **all zeros** — indistinguishable from a container that used no CPU and had no memory pressure. Readers must therefore compare `metric_schema` and skip mismatches rather than trusting the decode.

**HostVolume** (embedded object):

| Field         | Type   | Description                                     |
| ------------- | ------ | ----------------------------------------------- |
| `location`    | string | Mount path of the volume (e.g. `/`, `/scratch`) |
| `total_bytes` | int64  | Total capacity of the volume                    |
| `used_bytes`  | int64  | Space currently used on the volume              |

Bytes rather than gigabytes so the fields stay integers: Firestore's `DataTo` refuses to decode a stored double into an `int64` field, so a float unit here would permanently fix the wire type. Unit conversion belongs at display.

---

### `JobSummary`

One document per job, keyed by `job_id`. This is the **mutable** counterpart to the immutable `Jobs` document: a `Job` is written once at submission and never updated; all evolving state lives here. The **submit** path creates the initial `JobSummary` (with `state=pending`) at job submission time. After that, the `JobSummary` is maintained exclusively by the **monitor** process, which recomputes it whenever task states change. No process other than submit (at creation) and the monitor (thereafter) should write to this collection.

Any question about job progress — "is this job still running?", "how many tasks failed?" — should be answered by reading `JobSummary`, not by scanning `Tasks` or adding derived fields to `Jobs`.

| Field          | Type         | Description                                                                              |
| -------------- | ------------ | ---------------------------------------------------------------------------------------- |
| `job_id`       | string       | ID of the job this summary describes                                                     |
| `workpool_id`  | string       | Workpool the job is running in                                                           |
| `created_at`   | timestamp    | When the job was submitted (copied from `Jobs.created_at` at submission time)            |
| `expiry`       | timestamp    | When this document may be garbage-collected                                              |
| `last_updated` | timestamp    | When these fields were last recomputed by the monitor                                    |
| `state`        | string       | Rolled-up job state — see table below                                                    |
| `tasks`        | []StateCount | Task counts grouped by state; one entry per non-zero state                               |
| `labels`       | []Label      | User-defined tags (copied from `Jobs.labels` at submission time; not updated thereafter) |

**StateCount** (embedded object):

| Field   | Type   | Description                                                 |
| ------- | ------ | ----------------------------------------------------------- |
| `state` | string | State value (same allowed values as the `Tasks` collection) |
| `count` | int    | Number of tasks currently in that state                     |

**State values:**

| State                      | Description                                                                               |
| -------------------------- | ----------------------------------------------------------------------------------------- |
| `pending`                  | All tasks are `pending`; no work has started                                              |
| `in_progress`              | At least one task is active (`claimed`/`running`/`writing`); no `failed` or `error` tasks |
| `in_progress_with_error`   | At least one task is active; at least one task is in `error` state                        |
| `in_progress_with_failure` | At least one task is active; at least one task is in `failed` state                       |
| `killed`                   | At least one task was `killed`                                                            |
| `success`                  | All tasks complete; every terminal task reached `success`                                 |
| `error`                    | All tasks complete; at least one task is in `error` state, none in `failed`               |
| `failed`                   | All tasks complete; at least one task is in `failed` state                                |

---

### `JobSummaryHistory`

An append-only log of `JobSummary` snapshots. Each document is a point-in-time copy written by the monitor process whenever it updates `JobSummary`. The document ID is a UUID assigned at write time.

| Field         | Type         | Description                                                              |
| ------------- | ------------ | ------------------------------------------------------------------------ |
| `job_id`      | string       | ID of the job this snapshot describes                                    |
| `workpool_id` | string       | Workpool the job is running in                                           |
| `created_at`  | timestamp    | When the job was originally submitted (copied from `JobSummary`)         |
| `timestamp`   | timestamp    | When this snapshot was recorded                                          |
| `expiry`      | timestamp    | When this document may be garbage-collected                              |
| `state`       | string       | Job state at the time of the snapshot (same values as `JobSummary`)      |
| `tasks`       | []StateCount | Task counts at the time of the snapshot                                  |
| `labels`      | []Label      | User-defined tags at the time of the snapshot (copied from `JobSummary`) |

**StateCount** and **Label** are the same embedded objects as in `JobSummary`.

---

### `BatchAPIRequests`

One document per GCP Batch job submitted by the monitor. The document ID is the internal `batch_id` (a UUID). This collection is written and read exclusively by the monitor; no other process should modify it.

| Field                     | Type      | Description                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `batch_id`                | string    | Internal UUID for this batch request (document ID)                                                                                                                                                                                                                                                                                                               |
| `job_id`                  | string    | GCP Batch job resource name returned by the Batch API                                                                                                                                                                                                                                                                                                            |
| `project_id`              | string    | GCP project this batch's job and VMs live in, copied from `WorkPool.project_id` at submission. Pinned per batch so respeccing a workpool onto another project can't strand in-flight VMs; empty means the monitor's own project                                                                                                                                  |
| `workpool_id`             | string    | Workpool this batch request belongs to                                                                                                                                                                                                                                                                                                                           |
| `expected_vm_count`       | int       | Number of VMs requested in this batch job                                                                                                                                                                                                                                                                                                                        |
| `preemptible`             | bool      | Whether the batch was submitted as preemptible                                                                                                                                                                                                                                                                                                                   |
| `submitted_at`            | timestamp | When the batch job was submitted to GCP                                                                                                                                                                                                                                                                                                                          |
| `expiry`                  | timestamp | When this document may be garbage-collected (set 7 days out at submission time)                                                                                                                                                                                                                                                                                  |
| `running_since`           | timestamp | When the batch job first reached RUNNING state; absent until then                                                                                                                                                                                                                                                                                                |
| `registered_worker_count` | int       | Number of worker processes that have registered for this batch; monotonically increasing                                                                                                                                                                                                                                                                         |
| `status`                  | string    | Monitor's classification of this batch — `pending`, `started`, `completed`, `failed` (GCP itself reported the job as failed), `terminated` (the monitor's own bookkeeping — VM counts, worker registrations, heartbeats — found a problem and killed an otherwise-live job), or `deleted` (set when the GCP Batch job no longer exists, i.e. a 404 from the API) |
| `unhealthy`               | bool      | Sticky flag set when the monitor detects a problem with this batch; never cleared                                                                                                                                                                                                                                                                                |
| `termination_reason`      | string    | Populated when `status` is `terminated`; human-readable explanation of why the monitor killed the job (e.g. over-provisioning, no worker registered within the startup grace period, too many zombie workers)                                                                                                                                                    |

---

## Pub/Sub Topics

### `sparkles-events` _(Worker → Control plane)_

Published by workers to report lifecycle events. Messages are JSON-encoded. Every message published here is also written to the `Events` Firestore collection.

**WorkerEvent** — published on worker start and stop:

```json
{
  "type": "worker_started" | "worker_stopped",
  "worker_id": "...",
  "workpool_id": "...",
  "cleanly_terminated": true
}
```

`cleanly_terminated` is only meaningful on `worker_stopped`: `true` when the
worker itself published the event as part of a normal shutdown; `false` when
task recovery published it after marking the worker a zombie (its heartbeat
expired without a clean shutdown — see the `Workers` collection and the
`workpool_incident` description below).

**JobCreatedEvent** — published when a new job is submitted:

```json
{
  "type": "job_created",
  "job_id": "...",
  "workpool_id": "..."
}
```

The monitor subscribes to this topic via the `monitor-events-in` subscription and triggers an immediate provisioning poll on `job_created` receipt, rather than waiting for the next timer tick.

**JobTerminatedEvent** — published by the monitor when all tasks in a job have reached a terminal state (`success`, `error`, `failed`, or `killed`). This event signals that the job is done; it does not indicate whether the job succeeded or failed — consumers should query `JobSummary` for that.

```json
{
  "type": "job_terminated",
  "job_id": "...",
  "workpool_id": "..."
}
```

**WorkpoolStateChangeEvent** — published by the monitor whenever workpool state is saved (on every transition between `idle`, `ok`, `unhealthy`, and `halted`):

```json
{
  "type": "workpool_state_change",
  "workpool_id": "...",
  "state": "idle" | "ok" | "unhealthy" | "halted",
  "state_message": "..."
}
```

`state_message` is empty when transitioning to `idle` or `ok`; it contains a human-readable incident description when transitioning to `unhealthy` or `halted`.

**BatchFailedEvent** — published once per failed batch attempt, including
synchronous `CreateJob` failures that never produced a `BatchAPIRequest`:

```json
{
  "type": "batch_failed",
  "workpool_id": "...",
  "reason": "..."
}
```

**BatchSucceededEvent** — published once per batch, the first time it's confirmed to have at least one worker registered:

```json
{
  "type": "batch_succeeded",
  "workpool_id": "..."
}
```

**WorkpoolIncidentEvent** — published by `recordIncident` every time the watchdog detects a batch/worker anomaly:

```json
{
  "type": "workpool_incident",
  "workpool_id": "...",
  "reason": "..."
}
```

`WorkPoolSummary.state_message`/`last_incident_at`/`incident_count` are computed from these events (windowed to the last hour) rather than from persisted state — see the `Events` collection section above.

Both are consumed by `checkHaltThreshold` (see the `Events` collection section above) to decide whether to halt a workpool.

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

| `new_state` | `old_state`                         | Meaning                                               |
| ----------- | ----------------------------------- | ----------------------------------------------------- |
| `claimed`   | `pending`                           | Worker successfully claimed the task; staging begins  |
| `running`   | `claimed`                           | Staging complete; task process launched               |
| `writing`   | `running`                           | Process exited; uploading results to cloud storage    |
| `success`   | `writing`                           | Upload complete; process exited with code 0           |
| `error`     | `writing`                           | Upload complete; process exited with non-zero code    |
| `failed`    | `claimed` \| `running` \| `writing` | Infrastructure failure; task did not complete         |
| `pending`   | `claimed` \| `running` \| `writing` | Task orphaned back to pending (worker crash detected) |
| `killed`    | _(any)_                             | Task administratively terminated                      |

---

### `batch-api-notifications` _(GCP Batch API → Monitor)_

Published by GCP Batch API (or the batch API emulator) to notify the monitor when a batch job changes state. The monitor subscribes to this topic under the `batch-api-notifications` subscription. When a notification arrives the monitor immediately runs its cluster reconciler loop (checking job status, reconciling VMs) for the affected batch rather than waiting for the next periodic tick.

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

| Field  | Type   | Description              |
| ------ | ------ | ------------------------ |
| `type` | string | Message type (see below) |

#### `kill_job`

Additional fields

| Field    | Type   | Description                           |
| -------- | ------ | ------------------------------------- |
| `job_id` | string | ID of the job this message applies to |

Notifies all workers that the job `job_id` has been marked as killed, and so if the worker is currently running a task associated with that job, it should abort that work and mark the task as killed. This message is a best-effort message.

#### `stream_task_updates`

Additional fields

| Field     | Type   | Description                            |
| --------- | ------ | -------------------------------------- |
| `task_id` | string | ID of the task this message applies to |

Activates live streaming of a task's stdout/stderr to the `TaskLog` Firestore collection. The worker:

1. Closes the local events buffer file for the identified task.
2. Replays all buffered `log_update` entries accumulated since the task started by reading the buffer file and writing each entry to `TaskLog`.
3. Sets a flag so that all subsequent output chunks for that task are written directly to `TaskLog` instead of the local buffer.

If no task with the given `task_id` is currently running on the worker, the message is ignored

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
     │ killed │  (administrative kill, from any state)
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

**`killed`** — The task was administratively terminated. Terminal.

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

7. **Any active state → `pending`**  
   The monitor's task recovery loop runs periodically and scans for worker records whose `heartbeat_expiry` has passed. For each crashed or preempted worker, the worker's `status` is flipped to `zombie` (recording a `workpool_incident` event), and any task in an active state (`claimed`, `running`, or `writing`) is reset to `pending` so it can be picked up by a healthy worker.

8. **Any state → `killed`**  
   An external administrative action via the `sparkles kill` command. `owning_worker_id` is cleared. A best-effort `kill_job` control message is sent to all workers via `sparkles-worker-in` so any in-flight task for that job is aborted promptly.

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
