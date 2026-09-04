# Functional Test Plan

## Overview

Two functional tests exercise the full submission→execution and submission→kill flows without
real GCP services. Each test starts the necessary infrastructure, runs through the scenario,
and asserts on Firestore state.

Key insight: **workers claim tasks directly from Firestore**; the monitor is not required for
these tests. This means we only need Firestore, PubSub, and GCS emulators, plus the worker
processes themselves.

---

## Infrastructure

| Service   | Approach                                                                                                                            |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Firestore | `gcloud beta emulators firestore start` subprocess; `FIRESTORE_EMULATOR_HOST` auto-routes the Go client                             |
| PubSub    | `gcloud beta emulators pubsub start` subprocess; `PUBSUB_EMULATOR_HOST` auto-routes the Go client                                   |
| GCS       | [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) subprocess; `GCS_EMULATOR_ENDPOINT` routes the worker's storage client |
| Batch API | Not needed; workers are started directly from test code                                                                             |
| Monitor   | Not needed; workers claim tasks without it                                                                                          |
| Docker    | Replaced by direct `exec.Command` via `--no-docker` flag on the worker                                                              |

---

## Required Code Changes

### 1. `worker.go` — honour `GCS_EMULATOR_ENDPOINT` when creating the storage client

In `startWorker`, check the env var before calling `storage.NewClient`:

```go
var gcsOpts []option.ClientOption
if endpoint := os.Getenv("GCS_EMULATOR_ENDPOINT"); endpoint != "" {
    gcsOpts = append(gcsOpts,
        option.WithEndpoint(endpoint),
        option.WithoutAuthentication(),
    )
}
gcsClient, err := storage.NewClient(ctx, gcsOpts...)
```

`option` comes from `google.golang.org/api/option`, which is already a direct dependency.
No new flag or module needed.

### 2. `worker.go` — `--no-docker` execution mode

Add a direct-exec implementation of `ExecuteDockerCommand`:

```go
func executeCommandDirect(ctx context.Context, _ string, command []string,
    workDir string, _ []string, tel *TaskEventLog) error {
    cmd := exec.CommandContext(ctx, command[0], command[1:]...)
    cmd.Dir = workDir
    // pipe stdout+stderr to tel exactly like executeDockerCommand does
    ...
}
```

Add `noDocker bool` field to `workerState` (set from the `--no-docker` CLI flag).
`workerState.mainLoop` passes `executeCommandDirect` instead of `executeDockerCommand`
when the flag is set.

### 3. `cli_main.go` — `--no-docker` flag on `worker` command

```go
cli.BoolFlag{Name: "no-docker", Usage: "run task commands directly without Docker (ignores image name)"},
```

Pass through to `startWorker` → stored on `workerState` → used in `mainLoop`.

### 4. `monitor/emulator/server.go` — context-cancellable start (optional, for future tests)

The current `Run()` blocks on `SIGINT`/`SIGTERM`. Add a companion usable from tests:

```go
func StartServer(ctx context.Context, addr string, queueTime time.Duration, noDocker bool) error {
    s := newServer(queueTime, noDocker)
    mux := http.NewServeMux()
    s.registerRoutes(mux)
    srv := &http.Server{Addr: addr, Handler: mux}
    go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }()
    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        return err
    }
    return nil
}
```

Existing `Run()` is unchanged. Low priority for this plan — batch API emulator is not
used by the two functional tests.

---

## New Package: `functest/`

Create `v100/functest/` (separate package so it can use `_test` build constraints and
import the emulator binaries without polluting the main module).

### `helpers.go` — infrastructure helpers

```go
// freePort returns a random available TCP port.
func freePort() int

// startFirestoreEmulator starts the Firestore emulator subprocess,
// sets FIRESTORE_EMULATOR_HOST, polls until ready, and registers cleanup.
func startFirestoreEmulator(t *testing.T)

// startPubSubEmulator starts the PubSub emulator subprocess,
// sets PUBSUB_EMULATOR_HOST, polls until ready, and registers cleanup.
func startPubSubEmulator(t *testing.T)

// startGCSEmulator starts fake-gcs-server,
// sets GCS_EMULATOR_ENDPOINT, polls until ready, and registers cleanup.
// fake-gcs-server must be on PATH (install: go install github.com/fsouza/fake-gcs-server@latest).
func startGCSEmulator(t *testing.T)

// startWorker starts a full worker (via v100.startWorker + mainLoop) in a goroutine.
// The worker inherits the test's env vars for emulator routing.
// noDocker=true uses executeCommandDirect.
func startWorker(t *testing.T, ctx context.Context, project, db, workpoolID string, noDocker bool)

// submitJob writes a workpool, job, and tasks to Firestore and publishes job_created.
// Returns the generated jobID.
func submitJob(t *testing.T, ctx context.Context,
    fsClient *firestore.Client, psClient *pubsub.Client,
    workpoolID string, commands [][]string, dockerImage string,
    gcsRoot string) string  // gcsRoot is the GCS bucket/prefix for result_path and log_path

// waitForAllTasksTerminal polls Firestore until all tasks for jobID are terminal
// (success/error/failed/killed), or the timeout expires.
// Returns a map of taskID → final status.
func waitForAllTasksTerminal(t *testing.T, ctx context.Context,
    fsClient *firestore.Client, jobID string, timeout time.Duration) map[string]string

// waitForTasksInStates polls until all tasks for jobID are in one of the given states.
func waitForTasksInStates(t *testing.T, ctx context.Context,
    fsClient *firestore.Client, jobID string, states []string, timeout time.Duration)

// killJob calls the kill logic directly (same as the CLI kill command).
func killJob(t *testing.T, ctx context.Context,
    fsClient *firestore.Client, psClient *pubsub.Client, jobID string)
```

**Emulator startup helper pattern** (Firestore, PubSub — identical structure):

1. Pick a free port via `freePort()`
2. `exec.Command("gcloud", "beta", "emulators", "firestore", "start", "--host-port=localhost:PORT")`
3. `t.Setenv("FIRESTORE_EMULATOR_HOST", "localhost:PORT")`
4. Poll `localhost:PORT` with `net.Dial("tcp", ...)` every 100 ms until connected (30 s max)
5. `t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })`

**GCS emulator startup**:

1. Pick a free port
2. `exec.Command("fake-gcs-server", "-scheme", "http", "-port", PORT, "-backend", "memory")`
3. `t.Setenv("GCS_EMULATOR_ENDPOINT", "http://localhost:PORT/storage/v1/")`
4. Poll the emulator's health endpoint until ready
5. `t.Cleanup(...)`

If `gcloud` or `fake-gcs-server` is not found on `PATH`, the helper calls `t.Skip(...)`.

### `functest_test.go` — the two tests

**Test 1: Submit and complete**

```go
func TestSubmitAndComplete(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()

    startFirestoreEmulator(t)
    startPubSubEmulator(t)
    startGCSEmulator(t)

    const project, db = "test-project", "sparkles"
    workpoolID := "pool-" + uuid.New().String()

    startWorker(t, ctx, project, db, workpoolID, true /*noDocker*/)
    startWorker(t, ctx, project, db, workpoolID, true)

    fsClient := newFirestoreClient(t, ctx, project, db)
    psClient := newPubSubClient(t, ctx, project)

    jobID := submitJob(t, ctx, fsClient, psClient, workpoolID,
        [][]string{{"sh", "-c", "echo hello"}, {"sh", "-c", "echo world"}},
        "ignored-image",
        "gs://test-bucket/results/"+uuid.New().String())

    statuses := waitForAllTasksTerminal(t, ctx, fsClient, jobID, 90*time.Second)

    for taskID, status := range statuses {
        if status != v100.StatusSuccess {
            t.Errorf("task %s: want success, got %s", taskID, status)
        }
    }
}
```

**Test 2: Kill job**

```go
func TestKillJob(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()

    startFirestoreEmulator(t)
    startPubSubEmulator(t)
    startGCSEmulator(t)

    const project, db = "test-project", "sparkles"
    workpoolID := "pool-" + uuid.New().String()

    startWorker(t, ctx, project, db, workpoolID, true)
    startWorker(t, ctx, project, db, workpoolID, true)

    fsClient := newFirestoreClient(t, ctx, project, db)
    psClient := newPubSubClient(t, ctx, project)

    jobID := submitJob(t, ctx, fsClient, psClient, workpoolID,
        [][]string{{"sleep", "100000"}, {"sleep", "100000"}},
        "ignored-image",
        "gs://test-bucket/results/"+uuid.New().String())

    waitForTasksInStates(t, ctx, fsClient, jobID,
        []string{v100.StatusClaimed, v100.StatusRunning}, 30*time.Second)

    killJob(t, ctx, fsClient, psClient, jobID)

    statuses := waitForAllTasksTerminal(t, ctx, fsClient, jobID, 30*time.Second)

    for taskID, status := range statuses {
        if status != v100.StatusKilled {
            t.Errorf("task %s: want killed, got %s", taskID, status)
        }
    }
}
```

---

## Implementation Order

1. **`GCS_EMULATOR_ENDPOINT` support in `worker.go`** — one-line env-var check, no new flags
2. **`--no-docker` flag + `executeCommandDirect` in `worker.go` / `cli_main.go`**
3. **`functest/helpers.go`** — emulator launchers, `startWorker`, `submitJob`, polling helpers
4. **`functest/functest_test.go`** — the two tests
5. **`emulator.StartServer`** — optional, useful for future tests involving the batch API

---

## Notes

- The PubSub emulator requires the `sparkles-worker-in` and `sparkles-events` topics to exist
  before workers subscribe. `submitJob` (or a separate `ensureTopics` helper) should create
  them idempotently via the PubSub admin API before writing to Firestore.
- Workers started in goroutines share the test's `ctx`; cancelling it tears them down cleanly.
- `gcloud` and `fake-gcs-server` must be on `PATH`. Helpers call `t.Skip` if not found.
- Run with: `go test ./functest/ -timeout 5m -v`
- fake-gcs-server install: `go install github.com/fsouza/fake-gcs-server@latest`
