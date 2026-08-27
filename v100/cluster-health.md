# Cluster health: workpool states and transitions

This document describes how the monitor (`v100/monitor/*.go`) currently
tracks and reports the health of a workpool's cluster of worker VMs. It
reflects the implementation as it exists today, not an aspirational design.

## The states involved

Several independent state machines are layered together. The one people
usually mean by "cluster health" is `WorkPoolStatus`, but it's derived from —
and interacts with — the others.

| State machine    | Values                                                                                             | Owner                                                    |
| ---------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `WorkPoolStatus` | `idle`, `ok`, `unhealthy`, `halted`                                                                | The monitor; stored on `WorkPoolState`/`WorkPoolSummary` |
| `BatchStatus`    | `pending`, `started`, `failed`, `completed`, `deleted`                                             | The monitor; stored on `BatchAPIRequest`                 |
| Worker status    | `started`, `stopped`                                                                               | The worker process itself, via heartbeats                |
| Task status      | `pending`, `claimed`, `running`, `writing`, plus terminal (`success`, `error`, `failed`, `killed`) | Workers, as tasks execute                                |

`WorkPoolStatus` is the one surfaced most prominently (dashboard, `sparkles dev submit`'s polling loop) and the one this document focuses on.

## The pollers that drive all of this

The monitor runs several independent, debounced pollers
(`RunMonitorLoop`, `v100/monitor/monitor.go:127`), each on its own
notification-or-timer schedule (2s minimum, 30s fallback, except where
noted):

| Poller                        | File                                     | Scope                                                       | Wakes on                                                     |
| ----------------------------- | ---------------------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| Tier 1: task recovery         | `monitor.go` (`runRequeueOrphanedTasks`) | all workpools                                               | fixed timer only                                             |
| Tier 2: cluster reconciler    | `cluster_reconciler.go`                  | batches in `pending`/`started`                              | PubSub notification for a started batch; timer fallback      |
| Tier 3: batch startup monitor | `batch_startup_monitor.go`               | batches in `pending`                                        | PubSub notification for a pending batch; timer fallback      |
| Provisioning poll             | `provision.go`                           | workpools not `idle`/`halted`                               | `job_created`/`workpool_state_change` events; timer fallback |
| Job summary poll              | (job summary machinery)                  | non-terminal jobs                                           | `job_created`/`task_state_update` events; timer fallback     |
| WorkPool summary poll         | `workpool_summary_poll.go`               | non-idle workpools, or idle ones with a fresh `job_created` | batch/job events; timer fallback                             |
| Expiry cleaner                | `monitor.go`                             | all expired documents                                       | fixed 30-minute timer                                        |

Tiers 2 and 3 do **not** filter by `WorkPoolStatus` — they keep reconciling
batches (terminating zombie VMs, detecting GCP-reported failures, etc.) for
a workpool regardless of whether it's `ok`, `unhealthy`, or `halted`, as long
as it still has batches in `pending`/`started`. Only the **provisioning
poll** checks `WorkPoolStatus`, and only to decide whether to submit _new_
batches (`provision.go:37`, `:58`).

## `WorkPoolStatus` states

- **`idle`** — no pending tasks, no non-terminal workers. The default/rest
  state.
- **`ok`** — actively running work, no known problems.
- **`unhealthy`** — actively running work, but the watchdog has recorded at
  least one incident (a batch or worker anomaly) recently. This is a soft,
  self-healing warning state, not a hard stop — see below.
- **`halted`** — the monitor has stopped submitting new batches for this
  workpool. This is the one hard-stop state.

### `idle` ↔ `ok`

Driven entirely by the WorkPool summary poll (`workpool_summary_poll.go:60`),
based on counting non-terminal workers (status ≠ `stopped`) and non-terminal
tasks (status not in `success`/`error`/`failed`/`killed`):

- `nonTerminal == 0` → state becomes `idle` (from any state — see the
  `halted`/`unhealthy` interaction note below).
- `nonTerminal > 0` and current state is `idle` → state becomes `ok`.
- A `job_created` event on an `idle` workpool forces a summary recompute
  even though the workpool is otherwise excluded from the "active" poll set,
  so a freshly-submitted job promptly flips `idle` → `ok`
  (`workpool_summary_poll.go:41`).

### `ok`/`idle` → `unhealthy`

`recordIncident` (`monitor.go:343`) is called from tier 2
(`cluster_reconciler.go`) and tier 3 (`batch_startup_monitor.go`) whenever
they detect an anomaly:

- A batch fails outright (see `markBatchFailed` below).
- A subset of VMs fail to register a worker within the grace period, while
  others in the same batch succeed (`cluster_reconciler.go`, "Some workers
  registered; surgically terminate the VMs that never did").
- A zombie worker (heartbeat expired, VM still running) is terminated, below
  the abort threshold (`cluster_reconciler.go`, per-zombie loop).

`recordIncident` is now purely a log-to-Events operation — it publishes a
`workpool_incident` event carrying a human-readable message and does **not**
touch `WorkPoolState` at all. The `→ unhealthy` transition itself is decided
by the WorkPool summary poll instead, alongside the `idle ↔ ok` logic above
(see the next section) — deliberately centralizing all of the "what's our
current banner state" logic in one poller rather than having tier 2/3 mutate
state directly at incident time.

This is safe to defer to the next poll tick because `unhealthy` doesn't gate
any control-flow decision (unlike `halted` — see `provision.go:37`, which
only skips `idle`/`halted` workpools). The summary poll is woken by the same
batch notifications that cause incidents in the first place, so in practice
the lag is negligible.

### `idle` ↔ `ok` ↔ `unhealthy`: all one decision, in one poller

The WorkPool summary poll (`updateWorkPoolSummary`,
`workpool_summary_poll.go:61`) computes the banner state from three pieces of
data gathered fresh on every poll — it does not carry any of this as
persisted mutable state on `WorkPoolState` (which only ever holds the bare
`State` enum):

1. `nonTerminal` — count of non-`stopped` workers plus non-terminal tasks.
2. The current `State`, to detect (and preserve) `halted`.
3. `incidentCount` — the number of `workpool_incident` events for this
   workpool in the last hour (`defaultHaltCheckWindow`), fetched via
   `EventStore.ListRecentWorkpoolIncidents`.

The decision, in priority order:

```go
switch {
case nonTerminal == 0:
    newState = idle       // regardless of prior state, including halted
case oldState == halted:
    newState = halted     // sticky; only nonTerminal==0 clears it
case incidentCount > 0:
    newState = unhealthy
default:
    newState = ok
}
```

Because `incidentCount` comes from the same windowed query used for the
`WorkPoolSummary.state_message`/`last_incident_at`/`incident_count` fields
shown on the dashboard, the banner state and those detail fields can never
disagree: a workpool showing `unhealthy` always has a matching recent
incident to point to, and it reverts to `ok` exactly when the last incident
ages out of the one-hour window — not on a one-shot, potentially-stale
timer. `saveState`'s `workpool_state_change` event message is passed
explicitly at the point of transition (only non-empty when transitioning to
`unhealthy` or `halted`), since `WorkPoolState` has no `StateMessage` field
to read it from.

See `datamodel.md`'s `Events` section for the `workpool_incident` event
schema. Halting is **not** itself published as a `workpool_incident` — only
via `workpool_state_change` — so it doesn't count toward the incident tally
above; that event type is reserved for the anomalies that lead up to a halt.

### `unhealthy` → `halted`: the halt threshold

Unlike the `→ unhealthy` transition, `→ halted` stays synchronous —
deliberately not moved into the periodic summary poll, since `halted` gates
provisioning and delaying it would let more broken batches through before
provisioning actually stops. `checkHaltThreshold` (`monitor.go:376`) is
called at the end of every full
batch failure (via `markBatchFailed`, see below) and via `submitBatch` — it
inspects the most recent batch **outcomes** (not raw `BatchAPIRequest`
records — see "Batch outcome events" below) for the workpool within the last
hour (`defaultHaltCheckWindow`). If the most recent
`WorkPool.MaxConsecutiveFailedBatches` (default 2) outcomes in that window
are _all_ failures, the workpool transitions to `halted`, with a message
like "Last 2 batches all failed within the last hour — possible
configuration problem" passed to `saveState` (and hence into the
`workpool_state_change` event) — not persisted on `WorkPoolState` itself.

### `halted` is a one-way, sticky terminal state

Nothing in the current implementation ever transitions a workpool back out
of `halted` automatically:

- The WorkPool summary poll's state-decision switch explicitly preserves
  `halted` over any `incidentCount > 0` result (see above) — `recordIncident`
  itself no longer touches `WorkPoolState`, so it can't overwrite `halted`
  either way.
- The WorkPool summary poll explicitly leaves `halted` alone even once
  `nonTerminal` drops to... actually it _does_ clear to `idle` once
  `nonTerminal == 0` (that branch isn't conditioned on the old state) — so a
  halted workpool that fully drains its tasks/workers will show as `idle`.
  But as long as it still has non-terminal work, it stays `halted` forever.
- The provisioning poll refuses to submit any new batches for a `halted`
  workpool, indefinitely (`provision.go:58`).

In practice, getting out of `halted` today means either manual Firestore
intervention or submitting a corrected workpool config under a new
workpool ID (workpool IDs are content-hashed from the spec by default, so a
genuinely fixed config naturally gets a fresh ID and a fresh `WorkPoolState`
— see `resolveWorkpoolID` in `v100/dev/submit.go`).

## `BatchStatus` lifecycle

A `BatchAPIRequest` document is created in `submitBatch` (`provision.go:120`)
after a successful call to the GCP Batch API's `CreateJob`, starting in
`pending`. From there:

- **Tier 3** owns `pending` batches. It promotes a batch to `started` the
  first time a worker registers (`RegisteredWorkerCount >= 1`), or to
  `failed` if it fails/completes with zero workers registered, or if it
  never leaves the queue within `max_time_in_queue`.
- **Tier 2** owns `started` batches. It promotes to `completed` on GCP-reported
  success, or to `failed` on GCP-reported failure, over-provisioning, no
  worker registering within the startup grace period, or too many zombie
  workers.
- Either tier can mark a batch `deleted` if the GCP Batch API reports 404 for
  its job (manually deleted or expired).

Whenever a batch is marked `failed` in full (as opposed to a partial,
batch-continues incident — see above), the code goes through the
`markBatchFailed` helper (`monitor.go:355`), which: sets
`Status = failed`/`Unhealthy = true` on the batch, saves it, calls
`recordIncident` (logs to Events only) and `PublishBatchFailed`, and calls
`checkHaltThreshold`.

## Batch outcome events (`batch_failed` / `batch_succeeded`)

`checkHaltThreshold` reads from the `Events` Firestore collection rather
than from `BatchAPIRequest` documents directly. This matters for one
specific case: if the GCP Batch API's `CreateJob` call itself fails
synchronously (e.g. an invalid job name), `submitBatch` never creates a
`BatchAPIRequest` at all — there's no batch to mark failed. That failure is
still published as a `batch_failed` event directly (with the API's error as
the reason), so it's counted toward the halt threshold even though no batch
document ever existed for it. `batch_succeeded` is published once per batch,
the first time it's confirmed to have a registered worker (the same
`pending → started` transition tier 3 uses).

See `datamodel.md`'s `Events` section and `batch-success-failure-events.md`
for the full design rationale.

## Known asymmetries (as of this writing)

Two places don't fully reflect the batch-outcome-events change above yet:

1. **`submitBatch`'s synchronous `CreateJob` failure doesn't call
   `recordIncident`** the way `markBatchFailed` does for every other failure
   path — it only publishes the `batch_failed` event (for `checkHaltThreshold`),
   not a `workpool_incident` event (for the summary poll's `incidentCount`
   check). So a workpool can accumulate `MaxConsecutiveFailedBatches - 1`
   silent `CreateJob` failures with `WorkPoolStatus` still showing `ok` the
   whole time, then jump straight to `halted` on the next one, with no
   intermediate `unhealthy` warning.
2. **`WorkPoolSummary.UnhealthyBatchCount`/`BatchAPIRequestCounts`** (surfaced
   via the dashboard) are computed purely from `BatchAPIRequest` documents
   (`workpool_summary_poll.go:70-92`). Since a `CreateJob` failure never
   produces one, these dashboard-facing counts don't reflect it at all, even
   though it's now visible in the `Events` log and does count toward halting.
