# Sparklespray: Firestore Data Model

All collections live in a single GCP project and use the `SparklesV6` prefix for versioning.

**Services referenced:**

- **CLI** — Python CLI (`cli/sparklespray/`)
- **Worker** — Go worker binary (`src/sparklesworker/`) and its simulator (`simulator/main.go`)
- **Dashboard** — Go dashboard backend (`dashboard-backend/main.go`)

---

## SparklesV6Job

One document per submitted job.

**Key:** `NameKey("SparklesV6Job", job_id)`

| Field                      | Type      | Indexed | Description                                                                |
| -------------------------- | --------- | ------- | -------------------------------------------------------------------------- |
| `job_id`                   | string    | yes     | Unique identifier (e.g. `job-20240101-120000-abc123`)                      |
| `cluster_id`               | string    | yes     | ID of the cluster running this job                                         |
| `status`                   | string    | yes     | `pending` or `killed`                                                      |
| `submit_time`              | timestamp | yes     | UTC time the job was submitted                                             |
| `task_count`               | int32     | yes     | Total number of tasks in the job                                           |
| `tasks`                    | []string  | no      | Ordered list of task IDs                                                   |
| `kube_job_spec`            | string    | no      | Serialized job spec (legacy field name, now used for Cloud Batch job spec) |
| `metadata`                 | string    | no      | JSON-encoded metadata dictionary                                           |
| `max_preemptable_attempts` | int32     | yes     | Maximum times a task may be preempted before being failed                  |
| `target_node_count`        | int32     | yes     | Target number of worker nodes for this job                                 |

**Writers:** CLI (`job_store.py` — on job submission)

**Readers:** CLI (status queries and monitoring), Dashboard (job list and detail APIs)

---

## SparklesV6Task

One document per task. Updated at every status transition.

**Key:** `NameKey("SparklesV6Task", task_id)`

| Field                | Type          | Indexed | Description                                                 |
| -------------------- | ------------- | ------- | ----------------------------------------------------------- |
| `task_id`            | string        | yes     | Unique identifier, formatted as `{job_id}.{04d index}`      |
| `task_index`         | int64         | yes     | Zero-based position within the job                          |
| `job_id`             | string        | yes     | Parent job ID                                               |
| `cluster_id`         | string        | yes     | ID of the cluster owning this task                          |
| `status`             | string        | yes     | `pending`, `claimed`, `complete`, `failed`, or `killed`     |
| `owner`              | string        | yes     | Worker ID currently holding the task (empty if not claimed) |
| `last_updated`       | timestamp     | yes     | UTC time of most recent status change                       |
| `failure_reason`     | string        | yes     | Human-readable failure message (set when `status = failed`) |
| `exit_code`          | string        | yes     | Process exit code string (set on completion)                |
| `version`            | int32         | yes     | Optimistic-concurrency version counter                      |
| `args`               | string        | no      | Task arguments / command input                              |
| `command`            | string        | no      | Full command string                                         |
| `docker_image`       | string        | no      | Docker image used for execution                             |
| `command_result_url` | string        | no      | GCS URL for the command result file                         |
| `monitor_address`    | string        | no      | Address of the worker's gRPC monitoring endpoint            |
| `log_url`            | string        | no      | GCS URL for the task's stdout/stderr log                    |
| `history`            | []TaskHistory | no      | Ordered list of all status transitions (embedded slice)     |

#### TaskHistory (embedded in `history`)

| Field            | Type    | Description                                           |
| ---------------- | ------- | ----------------------------------------------------- |
| `timestamp`      | float64 | Unix seconds (nanosecond precision) of the transition |
| `status`         | string  | Status value at this point in time                    |
| `owner`          | string  | Worker that caused the transition (may be empty)      |
| `failure_reason` | string  | Reason if the transition was a failure (may be empty) |

**Writers:** CLI (`task_store.py` — on task creation); Worker (`sparklesworker` — status updates via Datastore transactions)

**Readers:** CLI (monitoring), Dashboard (task list and detail APIs), Worker (task claiming — reads and updates atomically)

---

## SparklesV6Event

Immutable event log. One document appended per lifecycle event; documents expire after 7 days.

**Key:** `NameKey("SparklesV6Event", event_id)` where `event_id` is a UUID.

#### Common fields on every event

| Field       | Type      | Indexed | Description                                             |
| ----------- | --------- | ------- | ------------------------------------------------------- |
| `event_id`  | string    | yes     | UUID assigned at write time                             |
| `type`      | string    | yes     | Event type string (see catalogue below)                 |
| `timestamp` | timestamp | yes     | UTC write time                                          |
| `expiry`    | timestamp | yes     | `timestamp + 7 days`; used for the Datastore TTL policy |

#### Event type catalogue

Fields below are indexed unless marked **noindex**.

---

**`cluster_started`** — a new cluster came online

| Field        | Type   | Description       |
| ------------ | ------ | ----------------- |
| `cluster_id` | string | ID of the cluster |

**`worker_started`** — a worker node started within a cluster

| Field        | Type   | Description              |
| ------------ | ------ | ------------------------ |
| `cluster_id` | string |                          |
| `worker_id`  | string | Unique worker identifier |

**`worker_stopped`** — a worker node exited (normal or crash)

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `worker_id`  | string |             |

**`job_started`** — a new job was submitted

| Field        | Type   | Description                      |
| ------------ | ------ | -------------------------------- |
| `cluster_id` | string |                                  |
| `job_id`     | string |                                  |
| `task_count` | int64  | Total number of tasks in the job |

**`task_claimed`** — a worker picked up a task and started staging

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `job_id`     | string |             |
| `task_id`    | string |             |

**`task_exec_started`** — the task's process began executing

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `job_id`     | string |             |
| `task_id`    | string |             |

**`task_exec_complete`** — the task's process exited; upload phase begins

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `job_id`     | string |             |
| `task_id`    | string |             |

**`task_complete`** — the task fully succeeded (staging, exec, and upload done)

| Field                   | Type    | Indexed | Description                             |
| ----------------------- | ------- | ------- | --------------------------------------- |
| `cluster_id`            | string  | yes     |                                         |
| `job_id`                | string  | yes     |                                         |
| `task_id`               | string  | yes     |                                         |
| `exit_code`             | int64   | no      | Process exit code (0 = success)         |
| `download_bytes`        | int64   | no      | Bytes downloaded during localization    |
| `upload_bytes`          | int64   | no      | Bytes uploaded after execution          |
| `max_mem_in_gb`         | float64 | no      | Peak RSS in GB                          |
| `max_memory_bytes`      | int64   | no      | Peak RSS in bytes                       |
| `shared_memory_bytes`   | int64   | no      | Peak shared memory in bytes             |
| `unshared_memory_bytes` | int64   | no      | Peak unshared (private) memory in bytes |
| `user_cpu_sec`          | float64 | no      | Total user-mode CPU seconds             |
| `system_cpu_sec`        | float64 | no      | Total system-mode CPU seconds           |
| `block_input_ops`       | int64   | no      | Block input operations                  |
| `block_output_ops`      | int64   | no      | Block output operations                 |

**`task_failed`** — the task permanently failed

| Field            | Type   | Indexed | Description                       |
| ---------------- | ------ | ------- | --------------------------------- |
| `cluster_id`     | string | yes     |                                   |
| `job_id`         | string | yes     |                                   |
| `task_id`        | string | yes     |                                   |
| `failure_reason` | string | no      | Human-readable reason for failure |

**`task_orphaned`** — the worker holding the task died; task will be requeued

| Field        | Type   | Description |
| ------------ | ------ | ----------- |
| `cluster_id` | string |             |
| `job_id`     | string |             |
| `task_id`    | string |             |

---

**Writers:** CLI (`job_queue.py` — `job_started` events only); Worker (all other lifecycle events)

**Readers:** Dashboard (event queries for the REST API and for computing `SparklesV6JobSummary` and `SparklesV6ClusterSummary`); CLI (monitoring)

**TTL:** The `expiry` field should be configured as a Datastore TTL policy. Events are automatically deleted 7 days after they are written.

---

## SparklesV6Cluster

One document per compute cluster. Written by the worker on startup.

**Key:** `NameKey("SparklesV6Cluster", cluster_id)`

| Field          | Type      | Indexed | Description                                             |
| -------------- | --------- | ------- | ------------------------------------------------------- |
| `cluster_id`   | string    | yes     | Unique identifier (e.g. `cluster-a1b2c3d4`)             |
| `machine_type` | string    | yes     | GCE machine type label (e.g. `n1-standard-4`)           |
| `created_at`   | timestamp | yes     | UTC time the cluster was registered                     |
| `region`       | string    | yes     | GCP region extracted from the zone (e.g. `us-central1`) |
| `last_updated` | timestamp | yes     | UTC time of the most recent update                      |
| `expiry`       | timestamp | yes     | TTL expiry time                                         |

**Writers:** Worker (on cluster startup)

**Readers:** Dashboard (cluster list and detail APIs)

---

## SparklesV6ClusterSummary

One document per cluster, maintained by the dashboard backend. Computed from Batch API calls and event data; not written by the worker or CLI.

**Key:** `NameKey("SparklesV6ClusterSummary", cluster_id)`

| Field                            | Type      | Indexed | Description                                 |
| -------------------------------- | --------- | ------- | ------------------------------------------- |
| `cluster_id`                     | string    | yes     |                                             |
| `last_update`                    | timestamp | yes     | Time this status was last recomputed        |
| `submitted_worker_requests`      | int       | yes     | Node requests in `submitted` state          |
| `short_failed_worker_requests`   | int       | yes     | Node requests that failed quickly           |
| `other_failed_worker_requests`   | int       | yes     | Node requests that failed for other reasons |
| `completed_worker_requests`      | int       | yes     | Node requests that completed successfully   |
| `seen_completions`               | []string  | no      | IDs of completion events already processed  |
| `instance_in_use_count`          | int       | yes     | Instances currently running a task          |
| `orphaned_task_count`            | int       | yes     | Tasks whose worker disappeared              |
| `idle_instance_count`            | int       | yes     | Instances running but not assigned a task   |
| `running_task_count`             | int       | yes     | Tasks currently in the `claimed` state      |
| `preemptable_instance_count`     | int       | yes     | Number of preemptable instances             |
| `non_preemptable_instance_count` | int       | yes     | Number of non-preemptable instances         |
| `expiry`                         | timestamp | yes     | TTL expiry time                             |

**Writers:** Dashboard (cluster health monitor loop)

**Readers:** Dashboard (cluster status API endpoint)

---

## SparklesV6JobSummary

One document per job, maintained by the dashboard backend. Computed from events as they arrive; used to answer summary queries without scanning all tasks.

**Key:** `NameKey("SparklesV6JobSummary", job_id)`

| Field           | Type      | Indexed | Description                       |
| --------------- | --------- | ------- | --------------------------------- |
| `job_id`        | string    | yes     |                                   |
| `submit_time`   | timestamp | yes     | When the job was submitted        |
| `cluster_id`    | string    | yes     |                                   |
| `task_count`    | int       | yes     | Total tasks in the job            |
| `success_count` | int       | yes     | Tasks that completed successfully |
| `failure_count` | int       | yes     | Tasks that permanently failed     |
| `expiry`        | timestamp | yes     | TTL expiry time                   |

**Writers:** Dashboard (updated as `task_complete` and `task_failed` events are processed)

**Readers:** Dashboard (job list API)

---

## SparklesV6LastJobUpdate

One document per job. Written (upserted) by the CLI on job submission and by the worker on every task status change. Its sole purpose is to let the dashboard cheaply identify which jobs have changed recently so it can recompute their `SparklesV6JobSummary` without scanning every job.

Because multiple workers may write this document concurrently, last-writer-wins is explicitly acceptable: the dashboard only needs to know that _something_ changed, not _what_ changed.

**Key:** `NameKey("SparklesV6LastJobUpdate", job_id)`

| Field        | Type      | Indexed | Description                                                                                                                                                     |
| ------------ | --------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `timestamp`  | timestamp | yes     | Client-side UTC time of the write; used by the dashboard to filter for recently changed jobs                                                                    |
| `event_uuid` | string    | no      | UUID of the event that triggered this write; the dashboard can compare against the last value it processed to skip recomputes when nothing has actually changed |

**Writers:** CLI (`job_queue.py` — on job submission, alongside the initial task batch write); Worker (`sparklesworker` and `simulator/main.go` — inside `writeEvent`, for any event that carries a `job_id`)

**Readers:** Dashboard (polling loop — queries `WHERE timestamp > lastPollTime - padding` to collect the set of job IDs that need their summary recomputed)

---

## SparklesV6ClusterHeartbeat

One document per cluster. Written by the CLI watch process to prevent multiple watch processes from managing the same cluster simultaneously.

**Key:** `NameKey("SparklesV6ClusterHeartbeat", cluster_id)`

| Field            | Type    | Description                                   |
| ---------------- | ------- | --------------------------------------------- |
| `watch_run_uuid` | string  | UUID of the watch process that holds the lock |
| `timestamp`      | float64 | Unix timestamp of the most recent heartbeat   |

A heartbeat older than 10 minutes is considered stale and another watch process may take over.

**Writers:** CLI (`cluster_service.py` — watch process)

**Readers:** CLI (`cluster_service.py` — before starting a watch to check for an existing holder)

---

## SparklesV6ClusterKeys

Singleton document holding TLS certificates and a shared secret used for encrypted gRPC communication between the CLI and workers.

**Key:** `NameKey("SparklesV6ClusterKeys", "sparklespray")`

| Field           | Type   | Indexed | Description                                    |
| --------------- | ------ | ------- | ---------------------------------------------- |
| `cert`          | bytes  | no      | TLS certificate (PEM)                          |
| `private_key`   | bytes  | no      | TLS private key (PEM)                          |
| `shared_secret` | string | no      | 20-character random alphanumeric shared secret |

**Writers:** CLI (`key_store.py` — during initial cluster setup)

**Readers:** CLI (`job_queue.py` — to pass credentials to workers); Worker (to authenticate incoming gRPC connections)

---

## SparklesV6ProjectConfig

Singleton document storing project-level configuration as a flat JSON-encoded key-value map. The exact set of keys is not fixed by schema; new configuration keys are added as features require them.

**Key:** `NameKey("SparklesV6ProjectConfig", "SparklesV6ProjectConfig")`

Known keys:

| Key                              | Type   | Description                                                                                               |
| -------------------------------- | ------ | --------------------------------------------------------------------------------------------------------- |
| `dashboard_user_service_account` | string | Service account email used by the dashboard backend to generate short-lived subscriber tokens for Pub/Sub |

**Writers:** CLI admin tooling (`datastore_helper.py`)

**Readers:** CLI (job submission, to resolve configuration); Dashboard backend (reads `dashboard_user_service_account` on startup)

---

## SparklesV6NodeReq

One document per Google Cloud Batch API node request. Tracks the lifecycle of each request to provision worker instances for a job.

**Key:** `NameKey("SparklesV6NodeReq", operation_id)`

| Field           | Type   | Description                                                 |
| --------------- | ------ | ----------------------------------------------------------- |
| `operation_id`  | string | Operation ID returned by the Batch API (used as entity key) |
| `cluster_id`    | string | Cluster this node request belongs to                        |
| `job_id`        | string | Job that triggered the request                              |
| `status`        | string | `submitted`, `staging`, `running`, `complete`, or `failed`  |
| `node_class`    | string | `preemptable` or `normal`                                   |
| `sequence`      | string | Sequence identifier for ordering requests                   |
| `instance_name` | string | GCE instance name once assigned (may be empty)              |

**Writers:** CLI (`batch_api.py` — when submitting node requests to the Batch API)

**Readers:** CLI (`cluster_service.py` — to track and reconcile outstanding requests)
