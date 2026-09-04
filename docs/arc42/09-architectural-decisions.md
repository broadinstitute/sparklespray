# 9. Architectural Decisions

Recorded as short ADR-style entries. Where a decision is only visible via
a source comment or design doc rather than a formal ADR file, that's
noted.

## AD-1: Single static Go binary; control plane is a single process

**Decision**: the worker and the control plane are subcommands of one binary,
replacing the Python implementation's installable package + virtualenv setup.
`sparkles serve` runs the monitor and the dashboard-backend together in one
process against shared Firestore/Pub-Sub clients; each can still be run alone
under `sparkles dev` for debugging.
**Consequence**: worker VM bootstrap is "download one file from GCS,
`chmod +x`, run" — no runtime install step on the VM at all — and a
control-plane host supervises one unit instead of two that must be started,
restarted, and upgraded together.
**Tradeoff**: the two halves share a fate. A fatal error in either exits the
process, so recovery depends on the service manager's restart policy rather
than one half surviving the other. That's deliberate: a monitor-less backend
accepts jobs nothing will ever provision for, which is worse than being down.

## AD-2: Workers claim tasks directly from Firestore, not via the monitor

**Decision**: task claiming is a client-side Firestore-transaction race
(fetch up to 100 pending candidates, shuffle, transactionally claim one),
not a request routed through a central scheduler process.
**Consequence**: the monitor is optional for pure task execution — proven
by the functional tests, which run full submit→execute and submit→kill
scenarios with no monitor process running at all. Avoids a
single-point-of-failure/bottleneck for claim throughput.
**Tradeoff**: "shuffle 100 and race" is somewhat wasteful under heavy
contention with many workers hitting the same job.

## AD-3: `status` vs. `state` naming convention + single-writer-per-collection

**Decision**: raw per-document fields are named `status`, derived/rollup
fields are named `state`; every collection has exactly one writer, and
cross-collection influence happens only via the append-only `Events`
collection (`datamodel.md`).
**Consequence**: unambiguous ownership makes it possible to reason about
any given Firestore write in isolation; avoids write races on rollups.

## AD-4: Batch outcomes recorded as events, not only as `BatchAPIRequest.status`

**Decision**: `checkHaltThreshold` queries the `Events` collection for
`batch_failed`/`batch_succeeded` outcomes rather than scanning
`BatchAPIRequest` documents directly.
**Rationale**: a synchronous `CreateJob` failure never produces a
`BatchAPIRequest` document at all — querying `BatchAPIRequests` would
silently miss that failure class for halt-threshold purposes. This was an
explicit bug fix, documented in `datamodel.md` (lines ~313-317).
**Known gap**: the dashboard-facing `WorkPoolSummary` batch counts are
still computed purely from `BatchAPIRequest` docs, so they _do_ miss that
same failure class (see [Risks](11-risks-technical-debt.md)).

## AD-5: Asymmetric timing for `halted` vs. `idle`/`ok`/`unhealthy`

**Decision**: the transition into `halted` is decided synchronously, the
instant a batch is marked fully failed. The `idle`/`ok`/`unhealthy`
"health banner" is decided only by the periodic `workpool_summary_poll`.
**Rationale**: halting gates future provisioning and must not lag by a
poll interval; the cosmetic banner can tolerate that staleness
(`cluster-health.md`).

## AD-6: Duplicate `WorkPool`/`EmptyVolume` types in `v100` and `monitor`

**Decision**: rather than share one type across packages, `v100` and
`monitor` each define their own equivalent `WorkPool`/`EmptyVolume`
structs, kept aligned by convention (matching field names/Firestore tags).
**Rationale**: `monitor` cannot import `v100` without an import cycle
(`v100/dev` imports `monitor`; `v100`'s own `monitor` CLI subcommand would
need to import `monitor` too), and introducing a third shared-types
package was judged not worth it at this scale (`new-command-plan.md`).
**Tradeoff**: the two type definitions can drift; there is no compiler
check that they stay aligned.

## AD-7: Preemptible-first provisioning with a hard fallback budget

**Decision**: the provisioning poll requests preemptible VMs up to
`MaxPreemptibleWorkerAttempts` per workpool, then falls back to
on-demand VMs, splitting a single demand delta across two
`BatchAPIRequest`s when the attempt budget boundary falls mid-request
(`autoscaler.md`).
**Rationale**: balances cost savings (preemptible is cheaper) against a
guarantee of forward progress (on-demand VMs aren't subject to
preemption).

## AD-8: Every GCP dependency behind a narrow interface, with fakes/emulators

**Decision**: `monitor/interfaces.go` defines small interfaces for every
GCP touchpoint; unit tests use in-memory fakes, functional tests use
local emulators (Firestore/Pub-Sub/GCS emulators, a hand-built Batch API
emulator).
**Rationale**: fast, deterministic, offline tests; explicit fake-clock
control over time-dependent logic (heartbeat expiry, halt thresholds,
scheduler debouncing) rather than real sleeps.

## AD-9: `urfave/cli` v1, not Cobra

**Decision**: the CLI framework is `github.com/urfave/cli` v1
(`cli_main.go`, `dev/commands.go`).
**Note**: no rationale is recorded in the docs; called out here mainly to
correct an assumption made when researching this document — file names
like `submit_cmd.go` might suggest Cobra convention, but the actual
implementation is urfave/cli.
