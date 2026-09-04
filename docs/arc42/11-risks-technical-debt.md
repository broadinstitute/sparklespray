# 11. Risks and Technical Debt

These are self-documented gaps (from `cluster-health.md`'s "Known
asymmetries" / "Future work" sections and code-level observations), not
speculative — treat them as a starting backlog, and re-verify against
current code before acting on any of them.

## 11.1 Known gaps

- **Synchronous `CreateJob` failures don't call `recordIncident`.**
  `submitBatch`'s synchronous failure path is invisible to the
  `unhealthy` health banner even though it _does_ count toward `halted`
  (via the `Events`-based check in `checkHaltThreshold`, AD-4). A user
  could see a workpool jump straight from `ok` to `halted` with no
  preceding `unhealthy` warning.
- **`WorkPoolSummary` batch counts miss synchronous `CreateJob` failures.**
  Computed purely from `BatchAPIRequest` documents, which — per AD-4 —
  are never created for that failure class. Dashboard-visible batch
  counts can therefore undercount failures relative to what actually
  drove a workpool to `halted`.
- **VM reconciliation is inert: all three anomaly checks never fire.**
  `reconcileVMs` filters running VMs by the GCE label
  `sparkles-worker-batch` (`monitor/cluster_reconciler.go`), but the only
  label ever set on a worker VM is `sparkles-workpool`
  (`labelWorkpool`/`gcpJobLabels` in `monitor/batch_api.go`). So
  `ListRunningVMs` always returns an empty map, and over-provisioning
  detection, per-VM startup-failure termination, and zombie-VM
  termination all silently no-op. Task recovery is unaffected
  (`requeue_orphaned.go` makes no GCP calls), but zombie VMs are never
  actually shut down, so a preempted-then-hung VM keeps costing money.
  Compounding it, `Worker.instance_name` is stored as the compound path
  `project/<p>/zone/<z>/instance/<i>` (`worker.go`) while the VM map is
  keyed by bare instance name, so those lookups can't match either.
  **Fix both together:** correcting the label alone would make every
  registered VM look unregistered and start terminating healthy workers
  once past the startup grace period.

## 11.2 Structural risks

- **Duplicate `WorkPool`/`EmptyVolume` types (AD-6)** in `v100` and
  `monitor` packages, kept aligned only by convention (matching field
  names/Firestore tags), with no compiler-enforced consistency. A future
  field addition/rename in one without the other would silently
  desynchronize Firestore reads/writes between the two packages.
- **Design docs vs. implementation drift.** An earlier planning doc
  (`autoscale-imp.md`) referred to a `v100/autoscaler/` package that was
  renamed to `v100/monitor/` (`484ff16 Rename autoscaler → monitor`) and
  was deleted outright once superseded, rather than kept around to mislead
  readers about file locations. Treat `docs/design/cluster-health.md` and
  `docs/design/datamodel.md` as the implementation-accurate docs; watch for
  the same kind of drift in the remaining design docs as `v100/` evolves.
- **"Shuffle up to 100 and race" task claiming (AD-2)** may become a
  contention/throughput bottleneck as worker-count-per-job scales up;
  no load-test data is cited in the design docs establishing where this
  breaks down.

## 11.3 Process/coverage risks

- The 9 scenarios in `autoscaler_testing.md` (S1–S9) are a target test
  matrix; the actual `monitor/*_test.go` coverage should be checked
  against this list rather than assumed complete — the design doc itself
  notes some scenarios are "documented, not necessarily all yet
  implemented as literal test functions."
- No Dockerfile / container image exists for `monitor` or
  `dashboard-backend`; whoever operates these processes today does so
  via an undocumented (outside `v100/`) deployment mechanism. This
  architecture doc cannot describe production deployment topology because
  nothing in the repo specifies it — worth confirming with whoever
  currently runs it operationally.

## 11.4 Documentation debt

This arc42 documentation set was generated from static analysis of code
and in-repo design docs on 2026-08-31 at commit `6fd277d`. It has not been
validated against a running system or against the people who operate it.
Treat section 7 (Deployment View) especially as inferred/best-effort
rather than confirmed operational fact.
