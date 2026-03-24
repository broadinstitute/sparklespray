# Monitor Tool — Design Document

## Overview

`autoscaler` is a polling service that manages a fleet of GCP VM clusters. A single monitor process manages **multiple `Cluster` instances**, polling each on a fixed interval (e.g. every 30 seconds). Its responsibilities are:

1. **Scaling** — resize each cluster to match current queue depth
2. **Orphan detection** — identify tasks whose owning node has disappeared
3. **Failure detection** — identify when nodes are failing to start, which likely indicates a configuration problem

This document covers the design decisions for all three purposes.

---

## Data Types

### Cluster

```go
type Cluster interface {
    // Immutable identity fields
    uuID               string
    machineType        string
    workerDockerImage  string
    workerCommandArgs  []string
    pubSubInTopic      string
    pubSubOutTopic     string

    // Mutable fields managed by the monitor
    maxPreemptableAttempts  int
    maxInstanceCount        int
    usedPreemptableAttempts int

    // Catch-all JSON blob for any state the monitor needs between polls
    monitorState string
}
```

**Immutable fields** describe the cluster's configuration and never change after creation.

**Mutable fields** are managed by the monitor or updated manually by the user via UI (e.g. `maxInstanceCount`). `usedPreemptableAttempts` tracks how many preemptable nodes have been requested in the current job run (see Preemptable Budget below).

**`monitorState`** is a JSON string used as an escape hatch for any additional per-cluster state the monitor needs to persist between polling calls (e.g. failure counts for purpose 3). Its schema is defined by the monitor implementation and should be versioned if it grows in complexity.

---

## Purpose 1: Scaling

### Goal

On each poll, determine how many new nodes to request so that the number of running nodes tracks the number of incomplete tasks, up to `maxInstanceCount`.

### Preemptable Budget

Each cluster has a budget of preemptable node-attempts per job run (`maxPreemptableAttempts`). Preemptable VMs are cheaper but can be reclaimed by GCP at any time. The budget limits how many times the cluster will attempt to use preemptable nodes before falling back to non-preemptable ones.

**Budget reset:** `usedPreemptableAttempts` resets to zero whenever `nonCompleteTaskCount` drops to zero — i.e., at the end of each job run. This means each new batch of work starts with a fresh preemptable budget.

### Lingering Node

When a cluster is cold (no nodes currently running), the first node created is designated as a **lingering node**. The lingering node has a longer idle timeout than regular worker nodes; it stays alive after the queue drains in case new tasks arrive shortly afterward, avoiding a full cold-start cycle for small follow-on work.

There is exactly **one lingering node per cold start**. If the queue drains while nodes are already running, no new lingering node is created.

The lingering node **uses the preemptable budget if available**, to save cost. If no preemptable budget remains, it is created as a non-preemptable node (since it needs to survive long enough to be useful).

### Request Type

```go
type Request struct {
    nodeCount      int
    isPreemptable  bool
    shouldLinger   bool
}
```

Requests are batched by type. A single `Request` with `nodeCount > 1` means "create this many nodes with these properties."

### Algorithm: `determineNodesToCreate`

```go
func determineNodesToCreate(
    nonCompleteTaskCount          int,
    maxInstanceCount              int,
    maxPreemptableAttempts        int,
    usedPreemptableAttempts       int,
    currentRequestedInstanceCount int,
) []*Request {

    targetCount := min(maxInstanceCount, nonCompleteTaskCount)
    nodesToRequest := targetCount - currentRequestedInstanceCount
    if nodesToRequest <= 0 {
        return nil
    }

    remainingPreemptableAttempts := maxPreemptableAttempts - usedPreemptableAttempts
    var requests []*Request

    // Cold start: create one lingering node first
    if currentRequestedInstanceCount == 0 {
        isPreemptable := remainingPreemptableAttempts > 0
        if isPreemptable {
            remainingPreemptableAttempts--
        }
        requests = append(requests, &Request{
            nodeCount:     1,
            isPreemptable: isPreemptable,
            shouldLinger:  true,
        })
        nodesToRequest--
    }

    // Fill remaining slots with preemptable nodes while budget allows
    if remainingPreemptableAttempts > 0 && nodesToRequest > 0 {
        nodeCount := min(remainingPreemptableAttempts, nodesToRequest)
        requests = append(requests, &Request{
            nodeCount:     nodeCount,
            isPreemptable: true,
            shouldLinger:  false,
        })
        nodesToRequest -= nodeCount
    }

    // Any remaining slots are non-preemptable
    if nodesToRequest > 0 {
        requests = append(requests, &Request{
            nodeCount:     nodesToRequest,
            isPreemptable: false,
            shouldLinger:  false,
        })
    }

    return requests
}
```

**Key decisions:**

- `targetCount` is capped at `maxInstanceCount`, not just `nonCompleteTaskCount`, to prevent runaway scaling.
- The function only ever _adds_ nodes. Scale-down is intentionally out of scope — the only supported way to remove all nodes is a full cluster termination, which is performed manually and is not the monitor's responsibility.
- Requests are returned as a slice so callers can batch-create by type, which maps naturally to GCP Batch API job submissions.

---

## Purpose 2: Orphan Detection

### Goal

Identify tasks that are claimed (have an `ownedBy` field set) but whose owning node is no longer in the set of running instances. These tasks need to be re-queued.

### Types

```go
type Task struct {
    id      string
    ownedBy string  // always non-empty for claimed tasks
}
```

### Algorithm: `findOrphanedTasks`

```go
func findOrphanedTasks(claimedTasks []*Task, runningInstances []string) []string {
    running := make(map[string]struct{}, len(runningInstances))
    for _, id := range runningInstances {
        running[id] = struct{}{}
    }

    var orphaned []string
    for _, task := range claimedTasks {
        if _, ok := running[task.ownedBy]; !ok {
            orphaned = append(orphaned, task.id)
        }
    }
    return orphaned
}
```

**Key decisions:**

- The input `claimedTasks` contains only tasks with an owner — unowned (queued) tasks are filtered upstream before this function is called and are never considered orphaned.
- The function returns task IDs, leaving re-queue mechanics to the caller.
- Uses a hash set for O(n) lookup rather than O(n²) nested iteration.

---

## Purpose 3: Node Failure Detection

### Goal

Detect when nodes are repeatedly failing to start, which likely indicates a misconfiguration (bad Docker image, invalid startup args, etc.) rather than transient GCP issues. When this condition is detected, stop creating new nodes and alert.

### Mechanism

The monitor queries the GCP Batch API to count how many batch jobs for a given cluster have failed. A heuristic determines whether the failure rate warrants halting new node creation.

### Heuristic (TBD)

The specific heuristic is not yet defined. Candidates include:

- **Absolute threshold:** halt if more than N jobs have failed in the current run.
- **Rate threshold:** halt if more than X% of recently submitted jobs have failed.
- **Consecutive failures:** halt if the last N jobs all failed (no successes in between).

The chosen heuristic and its parameters should be stored in `monitorState` or in the `Cluster` config once settled.

### Action on Detection

When the heuristic fires, the monitor:

1. **Stops creating new nodes** for that cluster — `determineNodesToCreate` is not called (or its result is suppressed).
2. **Emits an alert** (mechanism TBD — could be a log event, a Pub/Sub message to `pubSubOutTopic`, or an external alerting system).
3. Does **not** drain or shut down the cluster — any currently running nodes continue until they finish or are manually stopped.

The halted state should be recorded in `monitorState` so it persists across polls and the monitor doesn't repeatedly re-trigger alerts.

---

## Open Questions

| #   | Question                                                                                                 | Status |
| --- | -------------------------------------------------------------------------------------------------------- | ------ |
| 1   | What is the exact node failure heuristic and its threshold values?                                       | Open   |
| 2   | How is the alert emitted when failures are detected?                                                     | Open   |
| 3   | What is the polling interval, and is it configurable per cluster?                                        | Open   |
| 4   | How is `monitorState` persisted — is `Cluster` stored in a database, and is the monitor the sole writer? | Open   |
| 5   | Should the lingering node timeout be configurable per cluster, or fixed?                                 | Open   |
