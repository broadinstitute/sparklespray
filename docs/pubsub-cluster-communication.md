# Pub/Sub Cluster Communication

Sparklespray uses Google Cloud Pub/Sub for real-time communication between the CLI and worker nodes. This document describes the message types, payload structures, and communication patterns.

## Overview

Each cluster uses two Pub/Sub topics:

| Topic            | Direction      | Purpose                                  |
| ---------------- | -------------- | ---------------------------------------- |
| `incoming_topic` | CLI -> Workers | CLI sends requests to workers            |
| `response_topic` | Workers -> CLI | Workers send responses and status events |

Topic names depend on the `pubsub_topics` configuration:

- **Shared mode** (default): `sparkles-inbound` and `sparkles-outbound`
- **Per-cluster mode**: `sparkles-{cluster_id}-incoming` and `sparkles-{cluster_id}-response`

## Message Envelope Format

### Request Messages (CLI -> Workers)

```json
{
  "type": "<message-type>",
  "request_id": "<uuid>",
  "payload": { ... }
}
```

### Response Messages (Workers -> CLI)

```json
{
  "type": "<message-type>",
  "request_id": "<uuid>",
  "payload": { ... },
  "error": "<error-message-if-failed>"
}
```

### Event Messages (Workers -> CLI)

```json
{
  "type": "<event-type>",
  "worker_id": "<worker-id>",
  "timestamp": <milliseconds-since-epoch>,
  "task_id": "<optional>",
  "exit_code": "<optional>",
  "error": "<optional>"
}
```

## Request/Response Message Types

These messages follow a request/response pattern where the CLI sends a request and waits for a matching response (up to 20 seconds timeout).

### `read_output`

Read task output logs from a running task.

**Request Payload:**

| Field       | Type   | Description                                 |
| ----------- | ------ | ------------------------------------------- |
| `task_id`   | string | The task ID to read output from             |
| `size`      | int32  | Number of bytes to read                     |
| `offset`    | int64  | File offset to start reading from           |
| `worker_id` | string | Target specific worker (empty to broadcast) |

**Response Payload:**

| Field         | Type  | Description                            |
| ------------- | ----- | -------------------------------------- |
| `data`        | bytes | Base64-encoded output data             |
| `end_of_file` | bool  | True if offset has reached end of file |

**Usage:** Called continuously by `LogMonitor` to stream task output logs in real-time. The CLI calls with increasing offsets to poll for new log data.

**Source Files:**

- CLI: `cli/sparklespray/livelog/pubsub_client.py`
- Worker: `go/src/github.com/broadinstitute/sparklesworker/pubsub.go`

### `get_process_status`

Request system resource usage and process statistics from a worker.

**Request Payload:**

| Field       | Type   | Description                                 |
| ----------- | ------ | ------------------------------------------- |
| `worker_id` | string | Target specific worker (empty to broadcast) |

**Response Payload:**

| Field                     | Type  | Description                              |
| ------------------------- | ----- | ---------------------------------------- |
| `process_count`           | int32 | Number of processes in container         |
| `total_memory`            | int64 | Total memory usage (bytes)               |
| `total_data`              | int64 | Data segment memory (bytes)              |
| `total_shared`            | int64 | Shared memory (bytes)                    |
| `total_resident`          | int64 | Resident set size (bytes)                |
| `cpu_user`                | int64 | User CPU time                            |
| `cpu_system`              | int64 | System CPU time                          |
| `cpu_idle`                | int64 | Idle CPU time                            |
| `cpu_iowait`              | int64 | I/O wait CPU time                        |
| `mem_total`               | int64 | System total memory (bytes)              |
| `mem_available`           | int64 | System available memory (bytes)          |
| `mem_free`                | int64 | System free memory (bytes)               |
| `mem_pressure_some_avg10` | int32 | Memory pressure (some) 10-second average |
| `mem_pressure_full_avg10` | int32 | Memory pressure (full) 10-second average |

**Usage:** Called periodically during log streaming to monitor container resource usage.

## Event Message Types

These are one-way notifications from workers to CLI. They do not expect a response.

### `worker_started`

Notification that a worker has started and is ready to accept tasks.

| Field       | Type   | Description                |
| ----------- | ------ | -------------------------- |
| `type`      | string | `"worker_started"`         |
| `worker_id` | string | Worker instance identifier |
| `timestamp` | int64  | Milliseconds since epoch   |

**Sent:** When worker completes initialization in `main.go`.

### `worker_stopping`

Notification that a worker is shutting down.

| Field       | Type   | Description                |
| ----------- | ------ | -------------------------- |
| `type`      | string | `"worker_stopping"`        |
| `worker_id` | string | Worker instance identifier |
| `timestamp` | int64  | Milliseconds since epoch   |

**Sent:** When worker begins shutdown sequence.

### `task_started`

Notification that a worker has claimed and started executing a task.

| Field       | Type   | Description                |
| ----------- | ------ | -------------------------- |
| `type`      | string | `"task_started"`           |
| `worker_id` | string | Worker instance identifier |
| `task_id`   | string | The task being executed    |
| `timestamp` | int64  | Milliseconds since epoch   |

**Sent:** After task is claimed, before execution begins.

### `task_completed`

Notification that a task execution has completed.

| Field       | Type   | Description                               |
| ----------- | ------ | ----------------------------------------- |
| `type`      | string | `"task_completed"`                        |
| `worker_id` | string | Worker instance identifier                |
| `task_id`   | string | The task that completed                   |
| `timestamp` | int64  | Milliseconds since epoch                  |
| `exit_code` | string | Process exit code (empty if error/killed) |
| `error`     | string | Error message (empty if successful)       |

**Sent:** After task execution completes, with one of three scenarios:

1. **Success:** `exit_code` contains the exit code, `error` is empty
2. **Execution error:** `exit_code` is empty, `error` contains error message
3. **Task killed:** `exit_code` is empty, `error` is `"killed"`

## Subscription Management

### CLI Subscriptions

The CLI creates temporary subscriptions to receive responses:

- **Name:** `sparkles-cli-{random-8-hex-chars}`
- **Expiration:** 1 hour of inactivity
- **Lifecycle:** Created on first request, deleted when client closes

### Worker Subscriptions

Each worker creates its own subscription to receive requests:

- **Name:** `{incoming_topic}-{worker_id}`
- **Expiration:** 24 hours of inactivity (fallback)
- **Lifecycle:** Created on startup, deleted on clean shutdown

## Worker-Specific Filtering

Requests can target a specific worker by including `worker_id` in the payload:

- If `worker_id` is **empty**: All workers process the request
- If `worker_id` is **set**: Only the matching worker responds; others ignore the message

This allows the CLI to query specific workers on multi-worker clusters while all workers subscribe to the same topic.

## Communication Flow Example

### Log Streaming Flow

```
CLI                                Worker
 |                                    |
 |-- read_output(task_id, offset) --> |
 |                                    |-- reads stdout file
 |<-- {data, end_of_file} ----------- |
 |                                    |
 |-- get_process_status() ----------> |
 |                                    |-- collects system stats
 |<-- {process_count, memory, ...} -- |
 |                                    |
 | (repeat with increasing offset)    |
```

### Task Lifecycle Events

```
Worker                              CLI (listening)
 |                                    |
 |-- worker_started --------------->  |
 |                                    |
 | (claims task from Datastore)       |
 |                                    |
 |-- task_started(task_id) -------->  |
 |                                    |
 | (executes task)                    |
 |                                    |
 |-- task_completed(task_id) ------>  |
 |                                    |
 | (idle or claims next task)         |
 |                                    |
 |-- worker_stopping --------------> |
```

## Configuration Storage

Topic names are stored in Google Cloud Datastore:

- **Collection:** `SparklesV6Cluster`
- **Key:** `cluster_id`
- **Fields:** `incoming_topic`, `response_topic`

Both CLI and workers retrieve this configuration to establish pub/sub communication.
