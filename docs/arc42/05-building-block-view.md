# 5. Building Block View

## 5.1 Level 1: Top-level modules

```
v100/
├── cmd/sparkles/main.go     entrypoint: assembles v100.NewApp() + dev.Command()
├── cli_main.go              top-level CLI: worker | submit | kill | monitor
├── worker.go                worker process: claim/stage/run/report loop
├── task_queue.go            Firestore Job/Task/WorkPool structs + FirestoreTaskQueue
├── task_event_log.go        per-task log/metric buffering & streaming
├── task_metrics.go          resource usage sampling types (ResourceUsageEvent, VolumeUsage)
├── resource_usage.go        cgroup + `docker inspect` post-task resource accounting
├── resources.go             Resources: named float64 capacity map
├── transfer_client.go       GCSTransferClient: GCS upload/download
├── events.go                EventRecord + EventPublisher (sparkles-events topic + Events collection)
├── submit_cmd.go            `sparkles submit`
├── kill.go                  `sparkles kill`
├── scheduler/               generic leading-edge-throttle + trailing-coalescing poll scheduler
├── monitor/                 control-plane / autoscaler / watchdog (see 5.2)
├── dev/                     operational tooling + dashboard-backend (see 5.3)
└── functest/                end-to-end tests against emulators (no monitor, no Docker, no Batch API)
```

## 5.2 Level 2: `monitor/`

The autoscaling/watchdog half of the control plane, run by `sparkles serve`
(or `sparkles dev monitor` on its own). Owns everything under "autoscaling"
and "cluster health."

| File                                            | Responsibility                                                                                                                                                                         |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `interfaces.go`                                 | All monitor domain types (`WorkPool`, `Worker`, `Task`, `BatchAPIRequest`, ...) and store/client interfaces used for dependency inversion and test fakes.                              |
| `monitor.go`                                    | `Monitor` struct, `RunMonitorLoop`, shared state-transition helpers (`saveState`, `recordIncident`, `markBatchFailed`/`Terminated`/`Done`, `checkHaltThreshold`).                      |
| `provision.go`                                  | Provisioning poll: compares pending-task demand to active workers per workpool, creates `BatchAPIRequest`s.                                                                            |
| `batch_startup_monitor.go`                      | Owns `pending` batches; promotes to `started` on first worker registration, fails on GCP-reported failure/stuck-in-queue.                                                              |
| `cluster_reconciler.go`                         | Owns `started` batches; cross-references live GCP VMs vs. Firestore to detect over-provisioning, partial startup failure, and zombie VMs; terminates individual VMs or aborts a batch. |
| `requeue_orphaned.go`                           | Resets tasks of workers with an expired heartbeat back to `pending`; marks the worker `zombie`.                                                                                        |
| `workpool_summary_poll.go`                      | Recomputes `WorkPoolSummary` (`idle`/`ok`/`unhealthy`/`halted`) from current Firestore state.                                                                                          |
| `job_summary_poll.go`                           | Recomputes `JobSummary` from task-state counts.                                                                                                                                        |
| `expiry_cleaner.go`                             | Deletes documents past their `expiry` TTL, every 30 minutes.                                                                                                                           |
| `adapters.go`                                   | Firestore-backed implementations of all store interfaces.                                                                                                                              |
| `batch_api.go`                                  | `GCPBatchAPIClient`: wraps GCP Batch, Compute Engine, and Cloud Logging APIs.                                                                                                          |
| `pubsub_receiver.go`                            | Pub/Sub adapters for `batch-api-notifications` and `job_created`.                                                                                                                      |
| `remote_batch_client.go` / `emulator/server.go` | HTTP client/server pair standing in for the real GCP Batch API in dev/test.                                                                                                            |
| `*_test.go`, `testfakes_test.go`                | Unit tests and in-memory fakes for every interface.                                                                                                                                    |
| `functest/`                                     | A second, nested functional-test package.                                                                                                                                              |

## 5.3 Level 2: `dev/`

`sparkles dev ...` subcommands — operational and test tooling, plus the
dashboard-backend REST API. Kept as a separate package from `v100` and
`monitor` specifically to avoid an import cycle (`dev` imports both
`v100` and `monitor`; neither of those may import `dev`).

| File                      | Responsibility                                                                                                                                      |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `commands.go`             | CLI wiring for all `dev` subcommands.                                                                                                               |
| `dashboard_backend.go`    | REST API server implementing `openapi.yaml` — job submission, workpool/worker/batch/task/event queries, log streaming, Pub/Sub subscription helper. |
| `addworker.go`            | `dev add-worker`: manually create a GCP Batch job + `BatchAPIRequest`, bypassing normal provisioning.                                               |
| `api_key.go`              | `dev add-api-key` + bearer-token auth middleware for the dashboard-backend.                                                                         |
| `batchapi.go`             | Launches the Batch API emulator.                                                                                                                    |
| `clean_expired.go`        | `dev clean-expired`: manual/cron TTL sweep.                                                                                                         |
| `create_topics.go`        | `dev create-topics`: idempotently creates required Pub/Sub topics/subscriptions.                                                                    |
| `dumpdb.go`               | `dev dumpdb`: debugging dump of tasks/workpools.                                                                                                    |
| `export.go`               | `dev export`: dump any Firestore collection as JSON.                                                                                                |
| `set_config.go`           | `dev set-config`: load `SparklesConfig` into Firestore.                                                                                             |
| `simulate.go`             | `dev simulate`: synthetic load generator against the monitor without real GCP Batch/Compute.                                                        |
| `test_profile_command.go` | `dev test-profile-command`: run a Docker command with the worker's metric sampler attached, for debugging.                                          |
| `workpool_spec.go`        | `WorkpoolSpec` JSON struct + content-hash-based ID resolution.                                                                                      |

## 5.4 Level 3: Key types (selected)

- **`task_queue.go`**: `WorkPool`, `Job`, `Task`, `FirestoreTaskQueue`
  (`ClaimTask`, job/task CRUD).
- **`worker.go`**: worker main loop, `Worker` Firestore doc fields
  (`WorkerID, WorkpoolID, BatchID, InstanceName, Status, Expiry, HeartbeatExpiry`).
- **`events.go`**: `EventRecord`, `EventPublisher`.
- **`resources.go`**: `Resources` (`map[string]float64`, e.g.
  `slots=4, mem=32`) — capacity advertised by workers, requirements
  declared by jobs/tasks.

See [Data Model](12-glossary.md) and `datamodel.md` for full field-level
detail of every Firestore collection.
