# 4. Solution Strategy

- **One binary, three roles.** `worker`, `monitor`, and `dev dashboard-backend`
  are all subcommands of the same static Go binary. This collapses the
  Python implementation's separate install/venv story into "download one
  file from GCS, `chmod +x`, run" for VM bootstrap.

- **Firestore as the single source of truth, with a strict `status` vs.
  `state` split.** Primary entities (`Tasks`, `Workers`, `BatchAPIRequests`)
  carry a `status` written by exactly one owning process. Derived rollups
  (`JobSummary`, `WorkPoolSummary`) carry a `state` recomputed periodically
  by a dedicated poller from the primary entities — never written directly
  by anyone else. Cross-collection influence happens only via the
  append-only `Events` collection, never via direct writes into another
  collection's document. This trades a small amount of staleness (poll
  interval) for freedom from write races and an unambiguous "who changed
  this and when."

- **Workers claim work directly from Firestore, bypassing the monitor.**
  Task claiming is a Firestore-transaction race (fetch up to 100 pending
  candidates for a job, shuffle, transactionally flip one to `claimed`),
  not a request to a central scheduler. The monitor is therefore not a
  single point of failure or throughput bottleneck for task execution —
  confirmed by the functional-test design, which runs full
  submit→execute→complete and submit→kill scenarios with no monitor
  process at all.

- **Decompose the monitor into small, independently-scheduled pollers**
  sharing one debounced scheduler (`scheduler/`), each reacting to
  Pub/Sub notifications with a timer fallback: provisioning, batch startup
  monitoring, cluster reconciliation (anomaly detection), orphaned-task
  requeueing, workpool/job summary computation, and expiry cleanup. Each
  owns exactly one concern and one Firestore collection's `state`/rollup
  field, per the single-writer discipline above.

- **Prefer preemptible VMs, with a hard fallback budget.** The provisioning
  poll requests preemptible VMs up to `MaxPreemptibleWorkerAttempts` per
  workpool, then falls back to on-demand, splitting a single demand delta
  across two `BatchAPIRequest`s when the budget boundary falls mid-request.

- **Halt systematically-failing workpools.** A synchronous check
  (`checkHaltThreshold`) runs whenever a batch is marked fully failed; if
  the last `MaxConsecutiveFailedBatches` recorded outcomes (drawn from the
  `Events` log, so it also sees synchronous `CreateJob` failures that never
  produced a `BatchAPIRequest` document) are all failures, the workpool
  flips to a sticky `halted` state that blocks further provisioning until a
  new job submission resets it.

- **Build every GCP integration behind a narrow interface**
  (`monitor/interfaces.go`: `BatchAPIClient`, `WorkPoolStore`,
  `BatchRequestStore`, `WorkerStore`, `TaskStore`, `PubSubReceiver`,
  `EventStore`, `Clock`), with in-memory fakes for unit tests and local
  emulators (Firestore/Pub-Sub/GCS emulators, a hand-built Batch API
  emulator) for functional tests — so the whole system is testable without
  any real GCP credentials or spend.
