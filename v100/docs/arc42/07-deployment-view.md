# 7. Deployment View

## 7.1 Build & artifact

There is **no Dockerfile anywhere in the repository** and no container
image is built for worker or monitor. Instead:

- `build-linux-amd64.sh` cross-compiles a single static binary
  (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/sparkles`, version
  stamped via `-ldflags`) and uploads it directly to a GCS path, e.g.
  `gs://sparkles-test-0625/bin/sparkles-linux-amd64-dev`.

## 7.2 Worker VMs

- Provisioned on demand by the GCP Batch API using a stock VM image (no
  custom image).
- Each Batch job's task group runs two runnables per VM:
  1. a throwaway `cloudsdktool/cloud-sdk:slim` container that
     `gcloud storage cp`s the `sparkles` binary from
     `SparklesWorkerGCSPath` onto the VM's `RootDir`;
  2. a script runnable that `chmod +x`s it and execs
     `sparkles worker ...` **directly on the VM host** (not inside a
     container).
- The `sparkles worker` process then shells out to the host's
  `/usr/bin/docker` to run the actual user task in a container — so the
  end-user's task _does_ run containerized, even though the orchestrating
  process does not.
- Configured per workpool: machine type, region/zones, service account,
  resource capacity, max worker count, preemptible-attempt budget, VM
  shutdown grace period, linger time (`WorkPool` fields, see
  `datamodel.md`).

## 7.3 Control-plane processes

- **Monitor** (`sparkles monitor`) and **dashboard-backend**
  (`sparkles dev dashboard-backend`) are meant to run as long-lived
  processes. Nothing in `v100/` prescribes a specific hosting platform —
  a small always-on VM or any container platform works.
- `start.sh` shows the local-dev deployment pattern: uses `mprocs` to run
  `sparkles monitor`, `sparkles dev dashboard-backend`, and the separate
  `dashboard` frontend (a Node/npm project outside `v100/`) concurrently
  against sample config/workpool/job JSON files.

## 7.4 GCP resources used

| Service                 | Used for                                                                            |
| ----------------------- | ----------------------------------------------------------------------------------- |
| Cloud Firestore         | System of record (all collections)                                                  |
| Cloud Pub/Sub           | `sparkles-events`, `sparkles-worker-in-<worker_id>`, `batch-api-notifications`      |
| Cloud Storage           | Task file transfer; worker binary hosting                                           |
| GCP Batch API           | VM job creation/monitoring for worker pools                                         |
| Compute Engine API      | Listing/terminating individual VMs during anomaly handling                          |
| Cloud Logging           | Batch job logs (`LogsPolicy: CLOUD_LOGGING`)                                        |
| IAM Credentials API     | Short-lived tokens for the dashboard-backend's Pub/Sub "subscriber" service account |
| Compute metadata server | Worker reads its own instance name at startup (skippable via `--no-gcp`)            |

## 7.5 Local development / test deployment

- Firestore, Pub/Sub, and GCS emulators (`gcloud beta emulators ...`,
  `fake-gcs-server`) stand in for real GCP in functional tests
  (`v100/functest/`, `v100/monitor/functest/`).
- `monitor/emulator/server.go` (launched via `sparkles dev batchapi-emulator`) stands in for the real GCP Batch API, paired with
  `monitor/remote_batch_client.go` as its client.
- `sparkles dev simulate` drives the monitor with a synthetic load
  generator against real Firestore/Pub-Sub but no real GCP Batch/Compute,
  for load-testing autoscaling logic cheaply.
