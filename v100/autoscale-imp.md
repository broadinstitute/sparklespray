# Implementation Plan: `autoscale` Subcommand

## Goal

Add an `autoscale` subcommand to the `sparkles` CLI that starts the autoscaler loop:

```
sparkles autoscale --project <gcp-project> [--db <firestore-db>]
```

---

## What Exists

- **`autoscaler.New(clock, batchAPI, pools, batches, workers, tasks, pubsub)`** — fully implemented.
- **`RunAutoscalerLoop(ctx)`** — fully implemented; runs indefinitely.
- **All seven interfaces** — defined in `autoscaler/interfaces.go`.
- **Test fakes** — `autoscaler/testfakes_test.go` (in-memory implementations).
- **No production adapters exist** — none of the interfaces have Firestore/GCP implementations yet.

---

## Schema Changes

### `WorkpoolSpec` (in `cli_main.go`)

Add the autoscaler-specific fields so `dev submit` can persist them when creating a workpool:

```go
type WorkpoolSpec struct {
    ID           string          `json:"id"`
    MachineType  string          `json:"machineType"`
    RootDir      string          `json:"rootDir"`
    Resources    []ResourceEntry `json:"resources"`
    EmptyVolumes []EmptyVolume   `json:"emptyVolumes"`
    Region       string          `json:"region"`    // NEW: GCP region for Batch jobs, e.g. "us-central1"

    // Provisioning parameters
    MaxWorkerCount               int `json:"maxWorkerCount"`
    MaxPreemptibleWorkerAttempts int `json:"maxPreemptibleWorkerAttempts"`
    MaxWorkersPerRequest         int `json:"maxWorkersPerRequest"`   // default 100

    // Watchdog parameters (zero value → autoscaler uses its own defaults)
    MinTimeBetweenPollsSec         int `json:"minTimeBetweenPollsSec"`
    MaxTimeBetweenPollsSec         int `json:"maxTimeBetweenPollsSec"`
    MaxTimeToStartWorkerSec        int `json:"maxTimeToStartWorkerSec"`
    MaxTimeInQueueSec              int `json:"maxTimeInQueueSec"`
    VMShutdownGracePeriodSec       int `json:"vmShutdownGracePeriodSec"`
    MaxZombiesBeforeAbort          int `json:"maxZombiesBeforeAbort"`
    MaxConsecutiveFailedBatches    int `json:"maxConsecutiveFailedBatches"`
}
```

The `devSubmit` function maps these onto the `WorkPool` struct before writing to Firestore.
The existing `WorkPool` struct in `cli_main.go` must gain matching fields with firestore tags.

### `WorkPool` (in `cli_main.go`)

Add fields to match `autoscaler.WorkPool`:

```go
type WorkPool struct {
    WorkpoolID   string          `firestore:"workpool_id"`
    MachineType  string          `firestore:"machine_type"`
    RootDir      string          `firestore:"root_dir"`
    Resources    []ResourceEntry `firestore:"resources"`
    EmptyVolumes []EmptyVolume   `firestore:"empty_volumes"`
    Expiry       time.Time       `firestore:"expiry"`
    Region       string          `firestore:"region"`    // NEW

    // Provisioning
    MaxWorkerCount               int `firestore:"max_worker_count"`
    MaxPreemptibleWorkerAttempts int `firestore:"max_preemptible_worker_attempts"`
    MaxWorkersPerRequest         int `firestore:"max_workers_per_request"`

    // Watchdog (stored as seconds; zero → autoscaler uses defaults)
    MinTimeBetweenPollsSec         int `firestore:"min_time_between_polls_sec"`
    MaxTimeBetweenPollsSec         int `firestore:"max_time_between_polls_sec"`
    MaxTimeToStartWorkerSec        int `firestore:"max_time_to_start_worker_sec"`
    MaxTimeInQueueSec              int `firestore:"max_time_in_queue_sec"`
    VMShutdownGracePeriodSec       int `firestore:"vm_shutdown_grace_period_sec"`
    MaxZombiesBeforeAbort          int `firestore:"max_zombies_before_abort"`
    MaxConsecutiveFailedBatches    int `firestore:"max_consecutive_failed_batches"`

    // Status (written by autoscaler)
    Status         string    `firestore:"status"`
    StatusMessage  string    `firestore:"status_message"`
    LastIncidentAt time.Time `firestore:"last_incident_at"`
    IncidentCount  int       `firestore:"incident_count"`
}
```

---

## New Files

### 1. `v100/autoscaler/adapters.go` — Firestore stores

Five structs implementing the five store interfaces, all sharing a `*firestore.Client`.

#### `FirestoreWorkPoolStore`

Collection: `WorkPools` (constant `workpoolCollection` from `task_queue.go`).

Maps between the Firestore `WorkPool` document (with int-seconds duration fields) and
`autoscaler.WorkPool` (with `time.Duration` fields). For each duration field, if the stored
value is 0, the autoscaler defaults are used when reading.

#### `FirestoreBatchRequestStore`

Collection: `BatchAPIRequests`.

Straightforward mapping; `running_since` stored as a Firestore Timestamp (nil → null).

#### `FirestoreWorkerStore`

Collection: `Workers` (constant `workerCollection`).

- `ListExpired`: query `where heartbeat_expiry < now`
- `ListByBatch`: query `where batch_id == batchID`
- `CountActive`: query `where workpool_id == id AND heartbeat_expiry > now`, return count

#### `FirestoreTaskStore`

Collection: `Tasks` (constant `taskCollection`).

- `ListByWorker`: query `where owning_worker_id == id AND status in statuses`
- `CountPending`: query `where workpool_id == id AND status == "pending"`, return count
- `ResetToPending`: update `{status: "pending", owning_worker_id: ""}` by task ID

#### `FirestoreBatchStore` (implements `BatchRequestStore`)

Already described above.

---

### 2. `v100/autoscaler/batch_api.go` — GCP Batch API client

Uses **`google.golang.org/api/batch/v1`** (already pulled in transitively via `google.golang.org/api v0.274.0` in `go.mod`; no new direct dependency needed).

```go
type GCPBatchAPIClient struct {
    svc     *batch.Service   // google.golang.org/api/batch/v1
    project string
}
```

#### `CreateJob`

- Constructs a `batch.Job` with:
  - A single `TaskGroup` containing `expected_vm_count` tasks (one per VM).
  - `AllocationPolicy` specifying `machine_type` (from workpool) and whether preemptible.
  - Labels: `"sparkles-worker-batch": batchID`, `"sparkles-worker-workpool": workpoolID`.
  - The Pub/Sub notification config set to publish to the `autoscaler-in` subscription topic
    so the autoscaler's `PubSubReceiver` fires on state changes.
- Region/zone comes from the `WorkPool.Region` field passed into the client or the job request.
- Returns the GCP job name as `jobID`.

**Open question on `CreateJob` signature:** the interface is `CreateJob(ctx, workpoolID, batchID string, vmCount int, preemptible bool) (jobID string, err error)`. The GCP Batch API needs the machine type and region to create a job, but these aren't in the interface — they come from the workpool. The adapter will need to look up the workpool (via `WorkPoolStore`) or receive the workpool config at construction time. **Recommended approach:** pass a `WorkPoolStore` reference into `GCPBatchAPIClient` so `CreateJob` can fetch the workpool config by `workpoolID`.

#### `GetJobStatus`

- Calls `projects.locations.jobs.get(jobID)`.
- Maps GCP `JobStatus.State` → `autoscaler.BatchJobStatus`.

#### `ListRunningVMs`

**Interface change required:** update signature from `ListRunningVMs(ctx, labelFilter string)` to
`ListRunningVMs(ctx context.Context, filterLabelName, filterLabelValue string)`.
Update `interfaces.go`, all call sites in the autoscaler, and the test fake accordingly.

- Uses **Compute Engine Instances API** (`google.golang.org/api/compute/v1`).
- Callers pass `"sparkles-worker-batch"` + batchID, or `"sparkles-worker-workpool"` + workpoolID.
- Builds the GCE label filter expression `labels.<filterLabelName>=<filterLabelValue>`.
- Calls `instances.aggregatedList` with that filter and returns a `map[string]VMInfo`.

#### `TerminateVM`

- Calls `instances.delete(project, zone, instanceName)`.
- The zone must be parsed from the instance's `selfLink` or stored in `VMInfo`. Add `Zone string` to `VMInfo`.

#### `TerminateJob`

- Calls `projects.locations.jobs.cancel(jobID)`.

---

### 3. `v100/autoscaler/pubsub_receiver.go` — PubSub adapter

```go
type GCPPubSubReceiver struct {
    ch   chan string
}
```

- Subscription name: `autoscaler-in`
- Uses `cloud.google.com/go/pubsub` (already in `go.mod`).
- Background goroutine pulls messages and sends the `BatchID` (extracted from the message data or an attribute) to `ch`.
- `Notifications()` returns `<-chan string`.

**Open question:** What is the message format? The GCP Batch API publishes structured notifications. The adapter needs to extract the GCP job ID from the notification, then look up the `BatchAPIRequest` by `job_id` to get the `batch_id`. This requires a `BatchRequestStore` reference in the receiver, or the message must directly contain the `batch_id`. **Recommended:** pass a `BatchRequestStore` into the receiver so it can do the lookup.

---

### 4. Wire-up in `cli_main.go`

Add the `autoscale` command:

```go
{
    Name:  "autoscale",
    Usage: "Start the autoscaler loop",
    Flags: []cli.Flag{
        cli.StringFlag{Name: "project", Usage: "GCP project ID (required)"},
        cli.StringFlag{Name: "db",      Usage: "Firestore database (optional, uses default if empty)"},
    },
    Action: runAutoscale,
}
```

`runAutoscale`:

1. Validate `--project`.
2. Create `*firestore.Client` (with optional `--db`).
3. Create `*compute.Service` and `*batch.Service` from `google.golang.org/api`.
4. Instantiate all adapters.
5. Call `autoscaler.New(scheduler.RealClock{}, batchAPI, pools, batches, workers, tasks, pubsub)`.
6. Wire `context.WithCancel` + `signal.NotifyContext` for `SIGINT`/`SIGTERM`.
7. Call `as.RunAutoscalerLoop(ctx)` — blocks until signal.

---

## File Layout

```
v100/
  cli_main.go                     — add autoscale command, update WorkPool + WorkpoolSpec structs
  autoscaler/
    adapters.go                   — NEW: FirestoreWorkPoolStore, FirestoreBatchRequestStore,
                                          FirestoreWorkerStore, FirestoreTaskStore
    batch_api.go                  — NEW: GCPBatchAPIClient (Batch API + Compute Engine)
    pubsub_receiver.go            — NEW: GCPPubSubReceiver
```

---

## Remaining Ambiguities

1. ~~**`CreateJob` workpool config access**~~ — **Resolved.** Pass `WorkPoolStore` into `GCPBatchAPIClient`.

2. ~~**`ListRunningVMs` label filter format**~~ — **Resolved.** Signature becomes `(ctx, filterLabelName, filterLabelValue string)`. Callers use `"sparkles-worker-batch"` or `"sparkles-worker-workpool"` as the label name.

3. ~~**`TerminateVM` zone**~~ — **Resolved.** Add `Zone string` to `VMInfo`; populate from the Compute Engine aggregated list response. `TerminateVM` parses the zone from `VMInfo` passed by the caller.

4. ~~**PubSub message format**~~ — **Resolved.** Pass a `BatchRequestStore` into `GCPPubSubReceiver`; look up `batch_id` by job name on each notification.
