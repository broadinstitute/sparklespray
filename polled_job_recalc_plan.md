# Plan: Polled JobSummary Recomputation

## Problem

`SparklesV6JobSummary` is currently kept up to date by `startSummaryUpdater` in
`dashboard-backend/main.go`, which subscribes to the `sparkles-v6-events` Pub/Sub
topic and calls `recomputeJobSummary` for every job ID that appears in a message.
This can fall out of sync if the dashboard restarts, if Pub/Sub messages are lost,
or if the subscription falls behind.

## Proposed Solution

Replace the Pub/Sub-driven updater with a polling loop backed by a new Firestore
collection, `SparklesV6JobSummaryStale`. The CLI and worker write a lightweight
upsert to this collection whenever they update a task. The dashboard polls the
entire collection every 30 seconds, recomputes the summary for each job present,
and then deletes the record — but only after a successful recompute and only via
a transaction that verifies the record has not been updated since it was read
(checked by `event_uuid`). This means no last-poll timestamp needs to be
persisted across restarts: the collection itself is the queue.

The `SparklesV6JobSummaryStale` write is fire-and-forget and requires no transaction.
Multiple workers clobbering each other on the same document is explicitly fine —
the dashboard only needs to detect that _something_ changed, not _what_ or in
what order.

---

## New Firestore Collection: `SparklesV6JobSummaryStale`

**Key:** `NameKey("SparklesV6JobSummaryStale", job_id)`

| Field                        | Type   | Indexed | Description                  |
| ---------------------------- | ------ | ------- | ---------------------------- |
| of the write (informational) |
| `event_uuid`                 | string | no      | UUID of the triggering event |

The dashboard queries the entire collection
(no filter), and `event_uuid` is used only for the post-recompute transactional
delete guard — not for querying.

---

## Change 1: Worker — `simulator/main.go`

The `writeEvent` function is called for every lifecycle event. It already extracts
the `type` and publishes to Pub/Sub. Extend it to also upsert
`SparklesV6JobSummaryStale` whenever the event carries a `job_id`.

**In `writeEvent` (after the existing `dsClient.Put` for the event):**

```go
const LastJobUpdateCollection = "SparklesV6JobSummaryStale"

// extract job_id and event_id from the property list
var jobID, eventID string
for _, p := range props {
    switch p.Name {
    case "job_id":
        jobID, _ = p.Value.(string)
    case "event_id":
        eventID, _ = p.Value.(string)
    }
}

if jobID != "" {
    lju := datastore.PropertyList{
        {Name: "timestamp", Value: time.Now().UTC(), NoIndex: false},
        {Name: "event_uuid", Value: eventID, NoIndex: true},
    }
    ljuKey := datastore.NameKey(LastJobUpdateCollection, jobID, nil)
    if _, err := dsClient.Put(ctx, ljuKey, &lju); err != nil {
        log.Printf("WARNING: failed to write LastJobUpdate for job %q: %v", jobID, err)
        // non-fatal: the polling loop will catch up on the next interval
    }
}
```

This covers all worker-originated task events (`task_claimed`, `task_exec_started`,
`task_exec_complete`, `task_complete`, `task_failed`, `task_orphaned`) because they
all go through `writeEvent` with a `job_id` property.

The real worker (`src/sparklesworker/`) has an equivalent `writeEvent` function and
needs the same change applied there.

---

## Change 2: CLI — `job_queue.py`

The CLI writes tasks in two situations that need a `SparklesV6JobSummaryStale` upsert:

### 2a. Job submission (`submit_job`)

After `batch.flush()` completes (which writes all tasks and the job document),
add a single upsert for the new job. Use the same `event_id` written to the
`job_started` event (available from `_write_job_started_event` — thread the UUID
back to the caller, or generate it before the call and pass it in).

```python
LAST_JOB_UPDATE_COLLECTION = "SparklesV6JobSummaryStale"

def _write_last_job_update(self, job_id: str, event_uuid: str):
    key = self.client.key(LAST_JOB_UPDATE_COLLECTION, job_id)
    entity = datastore.Entity(key=key, exclude_from_indexes=("event_uuid",))
    entity["event_uuid"] = event_uuid
    self.client.put(entity)
```

Call `_write_last_job_update(job_id, event_id)` at the end of `submit_job`, after
`_write_job_started_event`.

### 2b. Any other CLI path that updates task status directly

Check `task_store.py` and `cluster_service.py` for direct calls to `TaskStore.insert`
or `client.put` on a task entity outside of job submission. If any exist, add a
corresponding `_write_last_job_update` call after each one, extracting the
`job_id` from the task being updated.

---

## Change 3: Dashboard — `dashboard-backend/main.go`

### 3a. Add the struct

```go
const JobSummaryStaleCollection = "SparklesV6JobSummaryStale"

type JobSummaryStale struct {
    JobID string
    EventUUID string
}
```

### 3b. Replace `startSummaryUpdater` with `startSummaryPoller`

Remove `startSummaryUpdater` and its Pub/Sub subscription entirely. Add:

TODO: Change below to sleep 30s between polls instead of using ticker. We don't care about the poll happening every 30s, we just don't want the polling to happen too fast.

```go
const summaryPollInterval = 30 * time.Second

func startSummaryPoller(ctx context.Context) {
    go func() {
        ticker := time.NewTicker(summaryPollInterval)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                // No timestamp filter — the collection is the queue.
                q := datastore.NewQuery(LastJobUpdateCollection)
                var updates []LastJobUpdate
                keys, err := dsClient.GetAll(ctx, q, &updates)
                if err != nil {
                    log.Printf("Summary poller: query error: %v", err)
                    continue
                }

                for i, k := range keys {
                    jobID := k.Name
                    if err := recomputeJobSummary(ctx, jobID); err != nil {
                        log.Printf("Summary poller: recompute failed for %q: %v", jobID, err)
                        continue // leave the record; retry next poll
                    }
                    // Delete only if the record has not been updated since we read it.
                    // If a new event arrived while we were recomputing, leave the record
                    // so the next poll picks it up.
                    eventUUID := updates[i].EventUUID
                    _, txErr := dsClient.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
                        var current StaleJobSummary
                        if err := tx.Get(k, &current); err != nil {
                            return err
                        }
                        if current.EventUUID != eventUUID {
                            return nil // changed; leave it for the next poll
                        }
                        return tx.Delete(k)
                    })
                    if txErr != nil {
                        log.Printf("Summary poller: delete failed for %q: %v", jobID, txErr)
                    }
                }
                log.Printf("Summary poller: processed %d job update(s)", len(keys))
            }
        }
    }()
}
```

Replace the `startSummaryUpdater(ctx)` call in `main` with `startSummaryPoller(ctx)`.

---

## Tuning

| Parameter             | Recommended default | Notes                                  |
| --------------------- | ------------------- | -------------------------------------- |
| `summaryPollInterval` | 30 s                | Trades recency for Datastore read cost |

No clock-skew padding is needed because records are deleted only after a
successful recompute; no timestamp watermark is maintained.

---

## Indexing

`SparklesV6JobSummaryStale` requires no custom indexes. The dashboard queries the
entire collection with no filter, and both fields are unindexed. No `index.yaml`
entry is needed for this collection.
