# 2. Constraints

## 2.1 Technical Constraints

- **Language/runtime**: Go (single static binary, `CGO_ENABLED=0`), built via
  `build-linux-amd64.sh`. Chosen so worker VMs need no language runtime
  installed.
- **CLI framework**: [`github.com/urfave/cli`](https://github.com/urfave/cli)
  v1 — _not_ Cobra. (`cli_main.go`, `dev/commands.go`)
- **GCP-only**: the system is built directly against GCP APIs (Batch,
  Compute Engine, Firestore, Pub/Sub, Cloud Storage, Cloud Logging, IAM
  Credentials) — there is no cloud-abstraction layer. Portability to
  another cloud is not a design goal.
- **System of record**: Cloud Firestore. All durable state (jobs, tasks,
  workers, workpools, summaries, events) lives there; there is no separate
  SQL database.
- **Task execution**: user task commands run inside Docker containers on
  the worker VM (`/usr/bin/docker`, invoked by the `sparkles worker`
  process, which itself runs directly on the VM host, not containerized).
  A `--no-docker` direct-exec mode exists solely for local dev/tests.
- **No custom VM images / no Dockerfiles**: worker VMs use a stock GCP
  Batch-provided image; the `sparkles` binary is fetched from GCS at
  startup rather than baked into an image.
- **Module path**: `github.com/broadinstitute/sparklespray/v100` (`go.mod`).

## 2.2 Organizational Constraints

- Developed and operated within CDS for internal ad-hoc batch
  workloads.
- Coexists in the same repository as the legacy Python `sparkles`
  implementation and the separate Node/npm `dashboard` frontend. An earlier,
  now-deleted top-level `dashboard-backend/` (also Go, but built on Cloud
  Datastore and a GCE-"cluster"-based data model) was superseded by
  `v100/dev/dashboard_backend.go`'s workpool/batch/worker Firestore model and
  removed from the repo; the `dashboard/` frontend was migrated in-place to
  match. `v100/dev/dashboard_backend.go` now also serves the built `dashboard/`
  frontend directly (see [Deployment View](07-deployment-view.md)).
- The `monitor` package must not import the top-level `v100` package (and
  vice versa where `v100/dev` needs `monitor`) to avoid an import cycle —
  this shapes where types are duplicated (see
  [Architectural Decisions](09-architectural-decisions.md)).

## 2.3 Conventions

- Firestore documents distinguish **`status`** fields (raw, single-writer,
  on primary documents: `Tasks`, `Workers`, `BatchAPIRequests`) from
  **`state`** fields (derived/aggregated, on rollup/summary/history
  documents) — enforced by naming convention, documented in `datamodel.md`.
- Every collection has exactly one writer; other processes influence it
  only by publishing an `Events` document, never by writing it directly.
