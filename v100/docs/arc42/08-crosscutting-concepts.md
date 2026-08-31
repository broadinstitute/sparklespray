# 8. Cross-cutting Concepts

## 8.1 Data model

Full field-level reference: `v100/datamodel.md` (kept up to date with the
implementation) and `v100/cluster-health.md` (state machines, with
file:line references into `monitor/*.go`).

Firestore collections and their single owner/writer:

| Collection                       | Doc ID              | Owner (writer)                                               | Kind              |
| -------------------------------- | ------------------- | ------------------------------------------------------------ | ----------------- |
| `Jobs`                           | `job_id`            | dashboard-backend (submit)                                   | immutable         |
| `Tasks`                          | `task_id`           | dashboard-backend (create) / worker (status) / kill (killed) | primary, `status` |
| `WorkPools`                      | `workpool_id`       | dashboard-backend (spec, immutable) / monitor (`State`)      | mixed             |
| `Workers`                        | `worker_id` (UUID)  | worker (create/heartbeat) / monitor (`zombie`)               | primary, `status` |
| `BatchAPIRequests`               | internal `batch_id` | monitor                                                      | primary, `status` |
| `WorkPoolSummary` / `...History` | `workpool_id`       | `workpool_summary_poll.go` only                              | derived, `state`  |
| `JobSummary` / `...History`      | `job_id`            | `job_summary_poll.go` only                                   | derived, `state`  |
| `Events`                         | `event_id`          | `EventPublisher` (mirrors `sparkles-events`)                 | append-only       |
| `TaskLog`                        | —                   | worker (opt-in streaming)                                    | append-only       |
| `APIKeys`                        | —                   | `dev add-api-key`                                            | auth              |
| `SparklesConfig`                 | `default`           | `dev set-config`                                             | config            |

Relationships: `Jobs` 1—N `Tasks` (`job_id`); `Jobs`/`Tasks` N—1
`WorkPools` (`workpool_id`); `WorkPools` 1—N `BatchAPIRequests`
(`workpool_id`); `BatchAPIRequests` 1—N `Workers` (`batch_id`); `Workers`
1—N `Tasks` (`owning_worker_id`, only while claimed).

## 8.2 `status` vs. `state` naming convention

Documented explicitly in `datamodel.md`. `status` = raw field on a primary
document, owned by one writer, reflects that document's own lifecycle
(e.g. `Task.status: pending|claimed|running|writing|success|error|failed|killed`).
`state` = derived/aggregated field on a summary/history document,
recomputed periodically from primary documents by a dedicated poller
(e.g. `WorkPoolSummary.state: idle|ok|unhealthy|halted`). This is enforced
by convention/naming, not by the type system.

## 8.3 Single-writer-per-collection & events-as-influence

No process writes into a collection it doesn't own. A process that needs
to influence another collection's derived state publishes a document to
the append-only `Events` collection (and the `sparkles-events` Pub/Sub
topic) instead — e.g. job submission doesn't flip `WorkPoolSummary.state`
directly; it publishes `job_created`, and the next
`workpool_summary_poll` run reads that history to decide `idle → ok`.
This avoids write races and keeps "who last changed this and why" legible
from one place per collection.

## 8.4 Task status lifecycle

`pending → claimed → running → writing → {success | error | failed | killed}`.
The three-way terminal split distinguishes: `success` (exit 0), `error`
(user command failed, non-zero exit — a program bug), and `failed`
(infrastructure failure — staging/upload error, worker crash mid-task,
resource mismatch) from `killed` (explicit user action).

## 8.5 Scheduling / debouncing

`scheduler/` implements a generic "leading-edge throttle with trailing
coalescing" poll scheduler shared by every monitor poller: a
notification (e.g. from Pub/Sub) triggers an immediate run if the poller
is idle; further notifications during a run are coalesced into a single
follow-up run; a timer fallback (default 5 min) guarantees progress even
with no notifications; a minimum interval (default 5s) prevents
thrashing. This lets each poller react promptly to real events without
either polling tightly or missing bursts.

## 8.6 Resource accounting

`resources.go`'s `Resources` type is a `map[string]float64` (e.g.
`slots=4, mem=32`). Workers advertise total capacity via `--resources`;
jobs/tasks declare requirements; a worker pre-fails all of a job's
pending tasks if the job's requirements exceed the worker's total
capacity, rather than claiming and failing them one at a time.

## 8.7 Observability

Every lifecycle transition (worker start/stop, task state change, job
created/terminated, workpool state change, batch failed/succeeded,
workpool incident) is published to `sparkles-events` and mirrored into
the append-only `Events` Firestore collection by `EventPublisher`
(`events.go`). This is the sole audit trail used both by
`checkHaltThreshold` (§6.4) and by the dashboard for history views.
Per-task logs and periodic resource-usage samples are optionally streamed
live to `TaskLog` when a `stream_task_updates` control message is sent to
a worker.

## 8.8 Authentication

The dashboard-backend uses a bearer-token scheme: API keys are generated
via `dev add-api-key` and stored in the `APIKeys` Firestore collection;
`sparkles submit` requires `SPARKLES_API_KEY` in the environment.
A separate, short-lived-token flow (via IAM Credentials API, minted for a
"subscriber" service account configured in `SparklesConfig`) supports a
browser-facing Pub/Sub subscription endpoint for the dashboard frontend.

## 8.9 Testability

Every GCP-facing dependency in `monitor/` sits behind a narrow interface
(`Clock`, `BatchAPIClient`, `WorkPoolStore`, `BatchRequestStore`,
`WorkerStore`, `TaskStore`, `PubSubReceiver`, `EventStore`), each with an
in-memory fake (`monitor/testfakes_test.go`) for unit tests, and real
local emulators (Firestore/Pub-Sub/GCS, plus a hand-rolled Batch API
emulator) for functional tests. No unit or functional test requires real
GCP credentials or incurs GCP spend.
