# Plan: batch success/failure events for workpool halting

## Problem

`checkHaltThreshold` (v100/monitor/monitor.go) decides whether to halt a
workpool by listing recent `BatchAPIRequest` documents and checking if the
last N are all `BatchStatusFailed`. This has a gap: when `CreateJob` itself
fails synchronously in `submitBatch` (v100/monitor/provision.go) — e.g.
because the workpool ID produced an invalid GCP job name — no
`BatchAPIRequest` document is ever written. The failure is only logged
(`provision.go:48-50`) and retried every poll cycle, forever, with nothing
persisted and no halt ever triggered.

We considered synthesizing a fake `BatchAPIRequest` for this case, but that
requires fabricating an ID for a batch that never actually existed, which is
surprising. Instead: record batch outcomes (success/failure) as first-class
events in the existing `Events` log, and have `checkHaltThreshold` read from
there instead of from `BatchAPIRequest`.

## Design

### 1. Two new event types: `batch_failed` / `batch_succeeded`

One event per **batch attempt outcome**, not per worker — a healthy batch
that registers 5 workers must count as a single success, not five, or the
success/failure ratio gets skewed.

- `batch_failed` — published at every point a batch (or a `CreateJob` call
  that never became a batch) is judged to have failed.
- `batch_succeeded` — published once, the first time a batch is confirmed to
  have at least one worker registered.

No new `EventRecord` fields are strictly required: `WorkpoolID` (already a
field) identifies which workpool the outcome belongs to, and the existing
`StateMessage` field (already used by `workpool_state_change`) can carry the
human-readable reason (e.g. Google's reported failure reason) for
`batch_failed`.

### 2. Centralize the "mark batch failed" call sites

Today there are 7 separate call sites that set `batch.Status = BatchStatusFailed`, `batch.Unhealthy = true`, call `recordIncident`, save the
batch, save workpool state, and call `checkHaltThreshold`:

- `batch_startup_monitor.go:64-74` (failed before any workers started)
- `batch_startup_monitor.go:77-87` (completed with no workers registered)
- `batch_startup_monitor.go:95-108` (never left the queue in time)
- `cluster_reconciler.go:76-93` (Batch API reported failure)
- `cluster_reconciler.go:123-140` (over-provisioning anomaly)
- `cluster_reconciler.go:145-163` (no worker registered within grace period)
- `cluster_reconciler.go:207-223` (too many zombie workers)

Introduce one helper in `monitor.go`:

```go
// markBatchFailed records batch as failed, raises an incident on ws.State,
// saves both, publishes a batch_failed event, and checks the halt threshold.
func (a *Monitor) markBatchFailed(ctx context.Context, ws *WorkPoolWithState, batch *BatchAPIRequest, reason string, now time.Time) error {
	batch.Status = BatchStatusFailed
	batch.Unhealthy = true
	recordIncident(ws.State, reason, now)
	if err := a.batches.Save(ctx, batch); err != nil {
		return fmt.Errorf("save batch: %w", err)
	}
	if err := a.saveState(ctx, ws.State); err != nil {
		return fmt.Errorf("save pool: %w", err)
	}
	if a.batchOutcomes != nil {
		if err := a.batchOutcomes.PublishBatchFailed(ctx, ws.Pool.WorkpoolID, reason); err != nil {
			log.Printf("markBatchFailed: publish batch_failed for workpool %s: %v", ws.Pool.WorkpoolID, err)
		}
	}
	return a.checkHaltThreshold(ctx, ws.Pool, ws.State)
}
```

Replace all 7 call sites with `return a.markBatchFailed(ctx, ws, batch, "<reason>", now)`.
This is a pure refactor for those sites (behavior preserved) plus the new event publish.

### 3. `submitBatch`'s synchronous `CreateJob` failure

In `provision.go`, when `a.batchAPI.CreateJob(...)` returns an error, there is
no `BatchAPIRequest` to mark failed. Publish the event directly:

```go
jobID, err := a.batchAPI.CreateJob(ctx, &WorkerJobSpec{ ... })
if err != nil {
	if a.batchOutcomes != nil {
		if pubErr := a.batchOutcomes.PublishBatchFailed(ctx, pool.WorkpoolID, err.Error()); pubErr != nil {
			log.Printf("submitBatch: publish batch_failed for workpool %s: %v", pool.WorkpoolID, pubErr)
		}
	}
	return fmt.Errorf("create GCP batch job: %w", err)
}
```

This closes the original gap: a bad job name (or any other synchronous
`CreateJob` rejection) is now visible in the Events log and counted toward
the halt threshold, instead of silently retrying forever.

### 4. The one "success" signal

`batch_startup_monitor.go:90-93`:

```go
if batch.RegisteredWorkerCount >= 1 {
	batch.Status = BatchStatusStarted
	return a.batches.Save(ctx, batch)
}
```

This only runs for batches still in `BatchStatusPending` (tier 3 only
processes pending batches, `batch_startup_monitor.go:20`), so it fires
exactly once per batch — the first poll where a worker has registered. This
is the natural, single point to publish `batch_succeeded`:

```go
if batch.RegisteredWorkerCount >= 1 {
	batch.Status = BatchStatusStarted
	if err := a.batches.Save(ctx, batch); err != nil {
		return err
	}
	if a.batchOutcomes != nil {
		if err := a.batchOutcomes.PublishBatchSucceeded(ctx, ws.Pool.WorkpoolID); err != nil {
			log.Printf("checkBatchStartup: publish batch_succeeded for workpool %s: %v", ws.Pool.WorkpoolID, err)
		}
	}
	return nil
}
```

### 5. Publisher plumbing (follows the existing `EventPublisher` pattern)

- `v100/events.go`: add `BatchOutcomeEvent` types and
  `PublishBatchFailed(ctx, workpoolID, reason string) error` /
  `PublishBatchSucceeded(ctx, workpoolID string) error` methods on
  `EventPublisher`, following `PublishWorkpoolStateChange`'s shape (writes an
  `EventRecord` with `Type: "batch_failed"`/`"batch_succeeded"`, `WorkpoolID`,
  and — for the failed case — `StateMessage: reason`).
- `v100/monitor/interfaces.go`: add a narrow interface

  ```go
  // BatchOutcomePublisher publishes one event per batch attempt outcome
  // (success or failure), used to feed checkHaltThreshold's event-log query.
  type BatchOutcomePublisher interface {
    PublishBatchFailed(ctx context.Context, workpoolID, reason string) error
    PublishBatchSucceeded(ctx context.Context, workpoolID string) error
  }
  ```

- `v100/monitor/monitor.go`: add `batchOutcomes BatchOutcomePublisher` field
  and `SetBatchOutcomePublisher(p BatchOutcomePublisher)` setter, matching
  `SetWorkpoolStatePublisher`/`SetJobTerminatedPublisher`.
- `v100/cli_main.go` (`runMonitor`): wire `m.SetBatchOutcomePublisher(ep)`
  alongside the existing `SetJobTerminatedPublisher`/`SetWorkpoolStatePublisher`
  calls, since `EventPublisher` (`ep`) implements all of them.

### 6. Reading it back: `EventStore` and `checkHaltThreshold`

Add one narrow query method to `EventStore` (`v100/monitor/interfaces.go`),
matching the existing single-purpose style of `ListJobCreatedSince`:

```go
// BatchOutcome is one batch_failed/batch_succeeded event.
type BatchOutcome struct {
	Failed    bool // true for batch_failed, false for batch_succeeded
	Timestamp time.Time
}

type EventStore interface {
	ListJobCreatedSince(ctx context.Context, since time.Time) ([]JobCreatedRecord, error)

	// ListRecentBatchOutcomes returns batch_failed/batch_succeeded events for
	// workpoolID with timestamp > since, ordered most-recent-first.
	ListRecentBatchOutcomes(ctx context.Context, workpoolID string, since time.Time) ([]BatchOutcome, error)
}
```

Firestore implementation (`v100/monitor/adapters.go`, alongside
`FirestoreEventStore.ListJobCreatedSince`):

```go
func (s *FirestoreEventStore) ListRecentBatchOutcomes(ctx context.Context, workpoolID string, since time.Time) ([]BatchOutcome, error) {
	q := s.fs.Collection(eventsCollection).
		Where("workpool_id", "==", workpoolID).
		Where("type", "in", []string{"batch_failed", "batch_succeeded"}).
		Where("timestamp", ">", since).
		OrderBy("timestamp", firestore.Desc)
	// ... iterate, map Type=="batch_failed" -> Failed: true
}
```

This requires a composite Firestore index on `(workpool_id ==, type in, timestamp >, order by timestamp)` — add it wherever the project's other
composite indexes are declared/deployed.

Rewrite `checkHaltThreshold`:

```go
// defaultHaltCheckWindow bounds how far back checkHaltThreshold looks in the
// Events log. Kept short (not a rolling "all time" window) both to keep the
// Firestore query cheap and because the failure mode we care about most is
// "things never start" — a burst of failures right now — not a slow drift
// over hours.
const defaultHaltCheckWindow = 1 * time.Hour

func (a *Monitor) checkHaltThreshold(ctx context.Context, pool *WorkPool, state *WorkPoolState) error {
	n := pool.MaxConsecutiveFailedBatches
	if n <= 0 {
		n = defaultMaxConsecutiveFailedBatches
	}

	since := a.clock.Now().Add(-defaultHaltCheckWindow)
	recent, err := a.events.ListRecentBatchOutcomes(ctx, pool.WorkpoolID, since)
	if err != nil {
		return fmt.Errorf("list recent batch outcomes for workpool %s: %w", pool.WorkpoolID, err)
	}

	// ListRecentBatchOutcomes returns DESC by timestamp; take only the most recent n.
	if len(recent) > n {
		recent = recent[:n]
	}

	if len(recent) == n {
		allFailed := true
		for _, o := range recent {
			if !o.Failed {
				allFailed = false
				break
			}
		}
		if allFailed {
			state.State = WorkPoolStatusHalted
			state.StateMessage = fmt.Sprintf(
				"Last %d batches all failed within the last hour — possible configuration problem", n)
			state.LastIncidentAt = a.clock.Now()
			if err := a.saveState(ctx, state); err != nil {
				return fmt.Errorf("save workpool %s: %w", pool.WorkpoolID, err)
			}
		}
	}
	return nil
}
```

Same decision semantics as today (count-based "last N outcomes", not a
wall-clock ratio) — just backed by the Events log instead of
`BatchAPIRequest`, and now covering the synchronous `CreateJob` failure case.
If fewer than N outcomes happened in the last hour, we don't halt (not
enough data), exactly like the current `len(recent) == n` guard.

Note the semantic change from today: because the query is time-bounded to
the last hour, a workpool with N-1 failures in the last hour and no other
activity will _not_ halt, even if those were literally the workpool's only N-1
attempts ever. This is an intentional trade-off for a cheap, boundable query
per the "things failing to start" priority above — an all-time equivalent
would require either an unbounded `ListByWorkpool`-style query (what we're
moving away from) or maintaining a separate rolling counter.

### 7. Test fakes

Add `ListRecentBatchOutcomes` to `FakeEventStore` (`monitor/testfakes_test.go`),
matching the existing `AddEvent`/`ListJobCreatedSince` shape, e.g. a second
slice `BatchOutcomes []BatchOutcome` plus an `AddBatchOutcome(workpoolID string, failed bool, ts time.Time)`
helper, filtered by `since` the same way `ListJobCreatedSince` is.

### 8. Out of scope / follow-ups

- `GET /api/v1/events` (`dashboard_backend.go`)'s `eventResponse` doesn't
  currently surface `StateMessage` at all (an existing gap — even
  `workpool_state_change` events lose their message today). Surfacing
  `batch_failed`'s reason in the dashboard UI needs that field added too;
  tracked separately, not required for the halt-threshold logic itself.
- `openapi.yaml` doesn't currently document any of the dashboard-backend GET
  endpoints (only `POST /api/v1/job`), so no changes needed there for this
  work.
- `BatchAPIRequest`/`WorkPoolSummary.UnhealthyBatchCount` and friends stay as
  they are — this plan only changes what `checkHaltThreshold` reads from, not
  the existing batch/workpool summary bookkeeping.
