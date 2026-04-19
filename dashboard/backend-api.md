# Sparkles Dashboard — Backend API Specification

## Overview

The backend exposes a single endpoint for fetching events. All business logic — deriving job/cluster/task status, computing time series, aggregating statistics — runs in the frontend.

All timestamps are ISO 8601 strings with UTC timezone (e.g. `2026-04-14T20:22:42.937000+00:00`). All responses are JSON.

---

## Event Model

Every event has a `type` and a `timestamp`. Events fall into three categories: cluster/infrastructure events, job events, and task events.

### Base fields (all events)

| Field       | Type              | Description                                                          |
| ----------- | ----------------- | -------------------------------------------------------------------- |
| `id`        | string            | Unique ID for this event which can be used for de-duplicating events |
| `type`      | string            | Event type identifier (see below)                                    |
| `timestamp` | string (ISO 8601) | When the event occurred                                              |
| `expiry`    | string (ISO 8601) | When this event will be deleted from the event history               |

### Cluster events

#### `cluster_started`

Emitted when a compute cluster is initialised.

| Field        | Type   | Description                       |
| ------------ | ------ | --------------------------------- |
| `cluster_id` | string | Unique identifier for the cluster |

#### `cluster_stopped`

Emitted when a cluster has fully shut down.

| Field        | Type   | Description                       |
| ------------ | ------ | --------------------------------- |
| `cluster_id` | string | Unique identifier for the cluster |

### Worker events

Workers are individual compute nodes within a cluster. A worker may stop and restart during a cluster's lifetime (e.g. due to preemption).

#### `worker_started`

| Field        | Type   | Description                               |
| ------------ | ------ | ----------------------------------------- |
| `cluster_id` | string | The cluster this worker belongs to        |
| `worker_id`  | string | Unique identifier for the worker instance |

#### `worker_stopped`

| Field        | Type   | Description                               |
| ------------ | ------ | ----------------------------------------- |
| `cluster_id` | string | The cluster this worker belongs to        |
| `worker_id`  | string | Unique identifier for the worker instance |

### Job events

#### `job_started`

Emitted when a job is submitted and accepted by the system.

| Field        | Type    | Description                                                                              |
| ------------ | ------- | ---------------------------------------------------------------------------------------- |
| `job_id`     | string  | Unique identifier for the job                                                            |
| `cluster_id` | string  | The cluster assigned to execute this job                                                 |
| `task_count` | integer | Total number of tasks in this job (authoritative count, including tasks not yet claimed) |

#### `job_killed`

The job has been marked as killed and no tasks will be started _(No additional fields.)_

### Task events

Tasks are the unit of work within a job. A task progresses through a lifecycle of stages and may be orphaned (lease expired, re-queued) and reclaimed multiple times before completing.

All task events share these fields:

| Field        | Type   | Description                              |
| ------------ | ------ | ---------------------------------------- |
| `task_id`    | string | Unique identifier for the task           |
| `job_id`     | string | The job this task belongs to             |
| `cluster_id` | string | The cluster assigned to execute this job |

#### `task_claimed`

(In UI we should probably label the status of such a task as "preparing")

A worker has claimed the task and will begin staging inputs. _(No additional fields.)_

#### `task_exec_started`

(Was previously labeled "task_staged" and "task_started")

Input data has been localised to the worker; Task execution has begun (process launched). _(No additional fields.)_

#### `task_exec_complete`

(In UI we should probably label the status of such a task as "finalizing")

Task execution has completed. Uploading results started.

#### `task_complete`

Outputs have been uploaded; task is fully done. Includes resource usage from the execution environment.

| Field                   | Type    | Description                                              |
| ----------------------- | ------- | -------------------------------------------------------- |
| `download_bytes`        | integer | Bytes downloaded from remote storage during localisation |
| `upload_bytes`          | integer | Bytes uploaded to remote storage                         |
| `exit_code`             | integer | Process exit code (0 = success)                          |
| `max_mem_in_gb`         | float   | Peak RSS memory in GiB                                   |
| `user_cpu_sec`          | float   | User-mode CPU time in seconds                            |
| `system_cpu_sec`        | float   | Kernel-mode CPU time in seconds                          |
| `max_memory_bytes`      | integer | Peak RSS in bytes                                        |
| `shared_memory_bytes`   | integer | Peak shared memory in bytes                              |
| `unshared_memory_bytes` | integer | Peak unshared (private) memory in bytes                  |
| `block_input_ops`       | integer | Block device read operations                             |
| `block_output_ops`      | integer | Block device write operations                            |

#### `task_orphaned`

The worker unexpectedly terminated. The task is returned to the queue. _(No additional fields.)_

#### `task_failed`

The task has permanently failed and will not be retried.

| Field            | Type   | Description                                     |
| ---------------- | ------ | ----------------------------------------------- |
| `failure_reason` | string | Human-readable description of the failure cause |

#### `task_killed`

The task discovered the job had been killed and so this task that was running has been aborted.

---

## Endpoint

### `GET /api/v1/task/{task_id}`

Return the details of the given task

### `GET /api/v1/cluster/{cluster_id}`

Return the details of the given cluster

### `GET /api/v1/job/{job_id}`

Return the details of the given job

### `GET /api/v1/events`

Returns a page of events ordered by `timestamp` ascending.

**Query parameters**

| Parameter | Type              | Required | Description                                                                                                           |
| --------- | ----------------- | -------- | --------------------------------------------------------------------------------------------------------------------- |
| `after`   | string (ISO 8601) | No       | Return only events with `timestamp > after`. Omit to start from the beginning.                                        |
| `before`  | string (ISO 8601) | No       | Return only events with `timestamp <= before`. Omit for no upper bound.                                               |
| `types`   | string            | No       | Comma-separated list of event types to include. Omit for all types. Example: `job_started,task_claimed,task_complete` |
| `limit`   | integer           | No       | Maximum number of events to return. Default: `1000`. Max: `10000`.                                                    |

**Response**

```json
{
  "events": [
    {
      "type": "job_started",
      "timestamp": "2026-04-14T20:17:54.793379+00:00",
      "job_id": "daintree-fit-v1-1acdceb51f5d5c0301de-2",
      "cluster_id": "36428935-b50b-4f48-a3bd-7351ea83a0be",
      "task_count": 1000
    },
    {
      "type": "task_claimed",
      "timestamp": "2026-04-14T20:22:42.937000+00:00",
      "task_id": "daintree-fit-v1-1acdceb51f5d5c0301de-2.0001",
      "job_id": "daintree-fit-v1-1acdceb51f5d5c0301de-2",
      "expiry": "2026-04-15T20:22:42.937000+00:00"
    }
  ],
  "next_after": "2026-04-14T20:22:42.937000+00:00"
}
```

`next_after` is the timestamp of the last returned event. Pass it as `after` in the next request to poll for new events without gaps or duplicates. It is omitted when the response contains no events.

**Error responses**

```json
{ "error": "invalid timestamp: 'not-a-date'", "code": "BAD_REQUEST" }
```

| HTTP status | `code`           | Meaning                              |
| ----------- | ---------------- | ------------------------------------ |
| 400         | `BAD_REQUEST`    | Invalid or malformed query parameter |
| 500         | `INTERNAL_ERROR` | Unexpected server error              |
