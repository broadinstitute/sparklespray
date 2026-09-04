# 7. Deployment View

## 7.1 Build & artifact

There is **no Dockerfile anywhere in the repository** and no container
image is built for worker or monitor. Instead:

- `v100/build.sh [version]` cross-compiles a single static binary
  (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/sparkles`) to
  `v100/bin/sparkles-linux-amd64-<version>`, version-stamped via `-ldflags`.
  `version` defaults to `git describe --tags --always --dirty` if not given.
  The same binary is used both to bootstrap worker VMs and to run the
  control plane (§7.4) — see §7.2 for what else this script does.
- `v100/upload-worker-binary.sh [version] [gcs-path]` runs `build.sh` and
  then `gcloud storage cp`s the result to the fixed GCS path worker VMs
  bootstrap from (`sparklesWorkerGCSPath` in `SparklesConfig`/workpool
  specs — see §7.3), e.g. `gs://sparkles-test-0625/bin/sparkles-linux-amd64-dev`.
  That path is a stable "latest" location the upload overwrites, not
  versioned per artifact — VMs always pull whatever was most recently
  uploaded there.

## 7.2 Dashboard UI embedding

The dashboard frontend (`dashboard/`, a separate React/Vite/npm project) is
built and embedded directly into the `sparkles` binary, so the whole control
plane — REST API and UI — is one process on one port; there is no separate
frontend deployment in production. Worker VMs never serve HTTP and thus never
use the embedded UI, but building it into every artifact (rather than
maintaining a second, UI-less binary) keeps there being exactly one build
script and one artifact to reason about — the frontend build is cheap enough
that a worker VM downloading the extra bytes at boot costs nothing material.

- `v100/build.sh` runs `npm ci && npm run build` in `dashboard/`, copies the
  resulting `dashboard/dist/*` into `v100/dev/webui/dist/`, then
  cross-compiles `./cmd/sparkles`. It skips the frontend build (an mtime
  check against a `.frontend-build-marker` breadcrumb, not a content hash)
  when nothing under `dashboard/` looks newer than the last build — `--force`
  overrides this. `v100/dev/webui/webui.go` `//go:embed all:dist`s the
  `dist/` directory into the binary; a tracked placeholder `index.html` keeps
  `go build ./...` compiling on a fresh checkout with no Node/npm step
  (`go:embed` requires at least one matched file), at the cost of a plain
  `go build` (skipping `build.sh`) serving that placeholder instead of the
  real UI.
- At runtime, `dashboard_backend.go`'s `http.ServeMux` registers
  `webui.Handler()` on `"/"` (serving embedded files, with an SPA fallback to
  `index.html` for react-router client-side routes) and an explicit
  `"/api/"` 404 handler, so unmatched API paths never fall through to the UI
  fallback. `apiKeyAuthMiddleware` only gates paths under `/api/`, so the UI
  loads with no auth and the app itself prompts for/stores an API key in the
  browser.
- See [v100/docs/deploying-dashboard-backend.md](../deploying-dashboard-backend.md)
  for the full remote-install walkthrough (prerequisites, building, copying
  the binary, a systemd unit example).

## 7.3 Worker VMs

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

## 7.4 Control-plane processes

- **`sparkles serve`** is the deployment entry point: one long-lived process
  running both the monitor's polls and the dashboard-backend's HTTP server
  (API + embedded UI) against shared Firestore/Pub-Sub clients. It shuts both
  halves down on `SIGINT`/`SIGTERM`, draining in-flight requests first, and
  exits if either half fails fatally rather than running half-alive — so a
  service manager's restart policy is the recovery mechanism. Nothing in
  `v100/` prescribes a hosting platform; a small always-on VM or any
  container platform works.
- The halves can still be run separately — **`sparkles dev monitor`** and
  **`sparkles dev dashboard-backend`** — which is useful when restarting or
  attaching to just one of them. They're under `dev` because running them
  apart is a development convenience, not the deployment shape.
- `start.sh` shows the local-dev pattern: `mprocs` running `sparkles serve`
  alongside a separate `npm run dev` process for the `dashboard` frontend
  (Vite dev server, proxying `/api` to the backend's port), against sample
  config/workpool/job JSON files. The separate frontend process is
  deliberately _not_ the embedded-UI production path from §7.2 — it gives
  hot-reload on frontend edits, which the production binary can't (its UI is
  baked in at `build.sh` time).

## 7.5 GCP resources used

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

## 7.6 Local development / test deployment

- Firestore, Pub/Sub, and GCS emulators (`gcloud beta emulators ...`,
  `fake-gcs-server`) stand in for real GCP in functional tests
  (`v100/functest/`, `v100/monitor/functest/`).
- `monitor/emulator/server.go` (launched via `sparkles dev batchapi-emulator`) stands in for the real GCP Batch API, paired with
  `monitor/remote_batch_client.go` as its client.
- `sparkles dev simulate` drives the monitor with a synthetic load
  generator against real Firestore/Pub-Sub but no real GCP Batch/Compute,
  for load-testing autoscaling logic cheaply.
