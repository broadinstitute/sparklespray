# Batch API Emulator Plan

## Overview

The goal is an external-process emulator for `BatchAPIClient` that the autoscaler can talk to
instead of GCP. This enables integration testing without a real GCP project.

Three deliverables:

1. **Emulator server** — an HTTP server that speaks a simple REST protocol mirroring the five
   `BatchAPIClient` methods. When `CreateJob` is called it actually spawns `vmCount` Docker
   containers; those containers are the "VMs" returned by `ListRunningVMs`. Job status is driven
   by container lifecycle, not manual control.
2. **`RemoteBatchAPIClient`** — a Go struct implementing `BatchAPIClient` that proxies calls to
   the emulator over HTTP.
3. **Wiring** — check `SPARKLES_BATCH_API_EMULATOR` in `runMonitor`; if set, use the remote
   client instead of the real GCP one.

---

## 1. Emulator server

### Delivery

Add a new subcommand to the existing `sparkles` binary:

```
sparkles dev batchapi-emulator --addr :8742 --queueTime 0s
```

This keeps everything in one binary, avoids separate build targets, and groups it with the
existing `sparkles dev submit` subcommand under `dev`.  
Source: `v100/monitor/emulator/server.go` (package `emulator`), wired into
`v100/cli_main.go`.

### In-memory state

```go
type emulatorVM struct {
    InstanceName string
    Zone         string
    Cmd          *exec.Cmd   // nil after process exits
    ExitCode     int         // set when process exits
    Done         bool
}

type emulatorJob struct {
    Spec   WorkerJobSpec
    JobID  string
    Status BatchJobStatus     // QUEUED → RUNNING → SUCCEEDED | FAILED
    VMs    []*emulatorVM
    cancel chan struct{}       // closed by TerminateJob to interrupt the queue-wait sleep
}
```

State is protected by a mutex. No persistence — the emulator is ephemeral by design.

### Job lifecycle

`CreateJob` immediately assigns a `jobID`, records the job as `QUEUED`, then launches a
goroutine that:

1. Sleeps for `--queueTime` (default `0s`) using a cancellable timer (a `select` on
   `time.After(queueTime)` and a per-job cancel channel).
2. If cancelled during the sleep (see `TerminateJob` below), marks the job `FAILED` and exits
   without spawning any containers.
3. Spawns `vmCount` Docker containers in parallel using `docker run -d`:
   ```
   docker run -d --name <instanceName> <dockerImage> <command>
   ```
   (`-d` returns immediately once the container is created, before the entrypoint runs.)
4. Transitions the job to `RUNNING` once all `docker run -d` calls have returned successfully.
5. Polls each container's status (e.g. via `docker inspect` or `docker wait`) in a goroutine;
   when the last one exits, transitions the job to `SUCCEEDED` (all exit 0) or `FAILED` (any
   non-zero exit).

The emulator always uses region `emulator` and the fixed zone list:

```
emulator-zone-a
emulator-zone-b
emulator-zone-c
```

`CreateJob` rejects any spec whose `region` is not `"emulator"`. Each container is assigned
instance name `vm-{jobID}-{n}` and a zone from the fixed list, round-robin by index.

`TerminateVM` sends `docker stop <instanceName>` to kill one container without affecting the
rest of the job. The watching goroutine notices the exit normally; if it was the last VM, the
job transitions to `FAILED`.

`TerminateJob` closes the job's cancel channel (unblocking the queue-wait goroutine if the
job is still `QUEUED`), then sends `docker stop` to every still-running container, and marks
the job `FAILED` immediately without waiting for the stops to complete.

### REST API

All request and response bodies are JSON.

#### BatchAPIClient operations

| Method                              | Path                                                                                                                                                                                                                                                                     | Maps to                                                                                                                                |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `POST /jobs`                        | body: `WorkerJobSpec` (`region` must be `"emulator"`, plus `machineType, vmCount, preemptible, dockerImage, command, emptyVolumes`); `batchID` and `workpoolID` are passed as `labels: [{name, value}]` with keys `sparkles-worker-batch` and `sparkles-worker-workpool` | `CreateJob` → responds `{jobID}`; returns 400 if region ≠ `"emulator"`                                                                 |
| `GET /jobs/{jobID}/status`          | —                                                                                                                                                                                                                                                                        | `GetJobStatus` → responds `{status}`                                                                                                   |
| `GET /region/{region}/zones`        | —                                                                                                                                                                                                                                                                        | Returns `{zones: ["emulator-zone-a","emulator-zone-b","emulator-zone-c"]}`; 404 if region ≠ `"emulator"`                               |
| `GET /vms/{zone}`                   | query: `filterLabelName`, `filterLabelValue`                                                                                                                                                                                                                             | `ListRunningVMs` → only returns VMs in the given zone whose container is still running; responds `{vms: {name: {instanceName, zone}}}` |
| `DELETE /vms/{zone}/{instanceName}` | —                                                                                                                                                                                                                                                                        | `TerminateVM` → `docker stop <instanceName>`                                                                                           |
| `POST /jobs/{jobID}/cancel`         | —                                                                                                                                                                                                                                                                        | `TerminateJob` → stops all containers, marks job FAILED                                                                                |

#### Control endpoints (debugging only)

| Method                | Path | Purpose                                             |
| --------------------- | ---- | --------------------------------------------------- |
| `GET /jobs`           | —    | List all jobs, their status, and per-VM exit codes. |
| `POST /control/reset` | —    | Stop all running containers and clear all state.    |

### Graceful shutdown

On `SIGINT` (^C) or `SIGTERM` the server:

1. Stops accepting new HTTP requests.
2. Sends `docker stop` to every container that is still running, across all jobs, in parallel.
3. Waits for all `docker stop` calls to return before the process exits.

No timeout is imposed on the wait — the process will not exit until every container has
confirmed it has stopped. This ensures no orphaned containers are left behind after the
emulator is killed.

---

## 2. `RemoteBatchAPIClient`

**File:** `v100/monitor/remote_batch_client.go`  
**Package:** `autoscaler`

```go
type RemoteBatchAPIClient struct {
    baseURL    string
    httpClient *http.Client
}

func NewRemoteBatchAPIClient(baseURL string) *RemoteBatchAPIClient
```

Each method on `RemoteBatchAPIClient` makes an HTTP call to the emulator, encodes/decodes JSON,
and maps HTTP errors to Go errors. No retries — let the caller (autoscaler) handle errors as it
would with real GCP transient failures.

`ListRunningVMs` receives a `[]string` zones slice from the interface. The implementation fans
out one `GET /vms/{zone}` request per zone (in parallel), then merges all results into the
single `map[string]VMInfo` the interface requires. This mirrors the per-zone pagination
required by the real GCP Compute API.

`CreateJob` receives `WorkerJobSpec`, which keeps `BatchID` and `WorkpoolID` as named Go
fields. Before sending to the emulator, `RemoteBatchAPIClient` packs them into the `labels`
list (`sparkles-worker-batch` → `BatchID`, `sparkles-worker-workpool` → `WorkpoolID`), matching
how `GCPBatchAPIClient` attaches them as GCP job labels. The emulator's `ListRunningVMs` then
filters on those labels, so the autoscaler's label-based queries work identically against both
backends.

---

## 3. Wiring into `runMonitor`

In `v100/cli_main.go`, `runMonitor` currently unconditionally creates a `GCPBatchAPIClient`.
Change it to:

```go
var batchAPI autoscaler.BatchAPIClient
if emulatorURL := os.Getenv("SPARKLES_BATCH_API_EMULATOR"); emulatorURL != "" {
    batchAPI = autoscaler.NewRemoteBatchAPIClient(emulatorURL)
} else {
    batchAPI, err = autoscaler.NewGCPBatchAPIClient(ctx, project, pools)
    if err != nil {
        return fmt.Errorf("creating batch API client: %w", err)
    }
}
```

No other changes to the autoscaler core.

---

## File summary

| File                                  | Role                                                                |
| ------------------------------------- | ------------------------------------------------------------------- |
| `v100/monitor/emulator/server.go`     | HTTP emulator server (new)                                          |
| `v100/monitor/remote_batch_client.go` | `RemoteBatchAPIClient` impl (new)                                   |
| `v100/cli_main.go`                    | Wire env-var switch + add `dev batchapi-emulator` subcommand (edit) |

---

## Design notes

- Job state is driven entirely by container lifecycle — no manual override endpoint. This means
  the emulator behaves like real GCP Batch: you can't fake a RUNNING job without something
  actually running.
- `TerminateVM` and `TerminateJob` map directly to `docker stop`, so they test real signal
  handling in the worker image.
- There is a known race: if `TerminateJob` arrives after the queue-wait sleep ends but before
  all `docker run -d` calls complete, some containers may still be spawned. This is acceptable
  for an emulator — don't add complexity to handle it.
- `ListRunningVMs` only returns VMs whose container process is still alive. A container that
  has exited (even if the job hasn't been cleaned up yet) disappears from the VM list, matching
  GCP Batch semantics.
- `RemoteBatchAPIClient` has no dependency on the emulator's internals. Any server that speaks
  the same HTTP protocol (e.g. a future recording proxy) could be used instead.
- The emulator lives under `sparkles dev batchapi-emulator`, consistent with the existing
  `sparkles dev submit` subcommand. The same binary used in production can spin up the emulator
  in CI, reducing build complexity.
- `SPARKLES_BATCH_API_EMULATOR` intentionally does not require `--project` to be a real GCP
  project when the env var is set, since no GCP calls are made.
