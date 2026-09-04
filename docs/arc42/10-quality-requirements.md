# 10. Quality Requirements

## 10.1 Quality Tree (summary)

- **Resilience**
  - Recover automatically from VM preemption without treating it as an
    incident.
  - Detect and recover from zombie/hung workers (heartbeat expiry →
    requeue task, mark worker zombie).
  - Detect and recover from partial/failed VM startup and
    over-provisioning (cluster reconciler anomalies 1–3).
  - Stop provisioning for a workpool that fails systematically
    (`halted`), rather than retrying indefinitely and accumulating spend.
- **Testability**
  - No unit or functional test requires live GCP credentials or incurs
    GCP spend.
  - Time-dependent behavior (heartbeats, halt thresholds, debouncing) is
    driven by a fake `Clock`, not real sleeps, so tests are fast and
    deterministic.
- **Cost efficiency**
  - Preemptible VMs preferred up to a configurable per-workpool attempt
    budget before falling back to on-demand.
  - Idle workpools (`nonTerminal == 0`) provision zero workers.
- **Observability**
  - Every state transition of every entity is recorded as an `Events`
    document / `sparkles-events` message, independent of the entity's own
    current-state fields — history is never solely inferable from current
    state.
- **Operational simplicity**
  - Worker VM bootstrap requires no runtime installation beyond
    downloading and executing a single static binary.

## 10.2 Concrete scenarios (from `autoscaler_testing.md`)

| #   | Scenario                      | Expected behavior                                                                                                                 |
| --- | ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| S1  | Happy path                    | Job submitted → workers provisioned → tasks complete → workpool returns to `idle`.                                                |
| S2  | Preemption recovery           | Worker VM preempted mid-task → task requeued → replacement worker provisioned → task completes.                                   |
| S3  | Configuration error           | Workpool consistently fails to start VMs (e.g. bad image) → workpool transitions to `halted` after `MaxConsecutiveFailedBatches`. |
| S4  | Fast-failing batch            | Batch fails quickly and repeatedly → counted toward halt threshold without long timeouts.                                         |
| S5  | Zombie surgical-then-abort    | A few zombie VMs are surgically terminated; exceeding `MaxZombiesBeforeAbort` aborts the whole batch.                             |
| S6  | Stuck in queue                | A batch stuck pending in GCP Batch's queue is detected and failed rather than waited on forever.                                  |
| S7  | Notification burst coalescing | A burst of Pub/Sub notifications results in one coalesced poll run, not one per notification.                                     |
| S8  | Preemptible-budget exhaustion | After exhausting `MaxPreemptibleWorkerAttempts`, provisioning falls back to on-demand VMs.                                        |
| S9  | Halted-workpool reset         | A new job submission on a `halted` workpool resets it to `ok`, allowing provisioning to resume.                                   |

These are documented as the target test matrix; see `monitor/*_test.go`
for the actual current implementation coverage.
