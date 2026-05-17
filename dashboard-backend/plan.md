# Cluster Health Monitor — Implementation Plan

## Overview

Add a background goroutine (`startClusterHealthMonitor`) to `main.go` that loops continuously, sleeping 60 seconds between passes. Each pass queries all clusters from Datastore, skips any with no activity in the last 24 hours, then calls `pollCluster` for each active one. `pollCluster` fetches live state from the GCP Compute and Cloud Batch APIs, reconciles against Datastore task records, and writes a `ClusterStatus` entity back to Datastore. A new HTTP endpoint exposes the latest status for the UI.

---

---

## New Dependencies

Two new Go client libraries need to be added via `go get`:

| Library                             | Purpose                                             |
| ----------------------------------- | --------------------------------------------------- |
| `cloud.google.com/go/compute/apiv1` | List GCE instances per zone; list zones in a region |
| `cloud.google.com/go/batch/apiv1`   | List and inspect Cloud Batch jobs by cluster label  |

All monitor state lives in a single struct. No new globals are added.

```go
type ClusterMonitor struct {
    projectID       string
    dsClient        *datastore.Client
    instancesClient *compute.InstancesClient
    zonesClient     *compute.ZonesClient
    batchClient     *batch.JobsClient
    zonesCache      map[string][]string // region → zone names, populated on first poll
}
```

No mutex on `zonesCache` — polling is sequential so there is no concurrent access.

Required IAM roles on the dashboard service account (confirmed to be available):

- `roles/compute.viewer`
- `roles/batch.viewer`

---

## Data Model

### New struct: `ClusterStatus`

```go
const ClusterStatusCollection = "SparklesV6ClusterStatus"

type ClusterStatus struct {
    ClusterID                   string    `datastore:"cluster_id"                     json:"clusterId"`
    LastUpdate                  time.Time `datastore:"last_update"                    json:"lastUpdate"`
    SubmittedWorkerRequests     int       `datastore:"submitted_worker_requests"      json:"submittedWorkerRequests"`
    ShortFailedWorkerRequests   int       `datastore:"short_failed_worker_requests"   json:"shortFailedWorkerRequests"`
    OtherFailedWorkerRequests   int       `datastore:"other_failed_worker_requests"   json:"otherFailedWorkerRequests"`
    CompletedWorkerRequests     int       `datastore:"completed_worker_requests"      json:"completedWorkerRequests"`
    SeenCompletions             []string  `datastore:"seen_completions,noindex"       json:"seenCompletions"`
    InstanceInUseCount          int       `datastore:"instance_in_use_count"          json:"instanceInUseCount"`
    OrphanedTaskCount           int       `datastore:"orphaned_task_count"            json:"orphanedTaskCount"`
    IdleInstanceCount           int       `datastore:"idle_instance_count"            json:"idleInstanceCount"`
    RunningTaskCount            int       `datastore:"running_task_count"             json:"runningTaskCount"`
    PreemptableInstanceCount    int       `datastore:"preemptable_instance_count"     json:"preemptableInstanceCount"`
    NonPreemptableInstanceCount int       `datastore:"non_preemptable_instance_count" json:"nonPreemptableInstanceCount"`
}
```

**Datastore key:** `datastore.NameKey(ClusterStatusCollection, clusterID, nil)` — one entity per cluster, upserted on every poll.

`SeenCompletions` does not need a size cap; the number of jobs per cluster is small.

### Updated `Cluster` struct

Three fields need to be added to the existing `Cluster` struct in both the dashboard backend (`main.go`) and the worker (`events.go`):

```go
type Cluster struct {
    ClusterID   string    `datastore:"cluster_id"   json:"cluster_id"`
    MachineType string    `datastore:"machine_type" json:"machine_type"`
    CreatedAt   time.Time `datastore:"created_at"   json:"created_at"`
    Region      string    `datastore:"region"       json:"region"`       // new
    LastUpdated time.Time `datastore:"last_updated" json:"last_updated"` // new
    Expiry      time.Time `datastore:"expiry"       json:"-"`             // new
}
```

**Worker change (`events.go`, `WriteCluster`):** the signature gains a `zone string` parameter (already available in `main.go` at the `WriteCluster` call site, line 339, since `zone` is fetched on line 285). Region is derived by dropping the trailing zone letter from the zone name (`us-central1-a` → `us-central1`).

```go
func (ew *EventWriter) WriteCluster(ctx context.Context, machineType, zone string) error {
    now := time.Now().UTC()
    parts := strings.Split(zone, "-")
    region := strings.Join(parts[:len(parts)-1], "-")
    c := &Cluster{
        ClusterID:   ew.clusterID,
        MachineType: machineType,
        Region:      region,
        CreatedAt:   now,
        LastUpdated: now,
        Expiry:      now.Add(7 * 24 * time.Hour),
    }
    key := datastore.NameKey(ClusterCollection, ew.clusterID, nil)
    _, err := ew.dsClient.Put(ctx, key, c)
    return err
}
```

The call site in `worker/main.go` changes from `eventWriter.WriteCluster(ctx, machineType)` to `eventWriter.WriteCluster(ctx, machineType, zone)`.

> **Note on `CreatedAt`:** `WriteCluster` currently overwrites the whole entity on every call, so `CreatedAt` is reset each time a new worker starts. This was pre-existing behaviour. If preserving the original creation time matters, a read-before-write would be needed. No change proposed here unless requested.

---

## Core Algorithm: `(m *ClusterMonitor) poll(ctx, cluster)`

`poll` is a method on `ClusterMonitor`. It receives the full `Cluster` entity so it can read `cluster.Region` directly without any additional lookup.

### Step 0 — Read existing ClusterStatus from Datastore

```go
var status ClusterStatus
key := datastore.NameKey(ClusterStatusCollection, cluster.ClusterID, nil)
err := m.dsClient.Get(ctx, key, &status)
if err == datastore.ErrNoSuchEntity {
    status = ClusterStatus{ClusterID: cluster.ClusterID}
} else if err != nil {
    return fmt.Errorf("get ClusterStatus for %q: %w", cluster.ClusterID, err)
}
```

### Step 1 — Fetch live state

Three data sources fetched at the start of each poll.

**a) Tasks from Datastore**

```go
var tasks []Task
dq := datastore.NewQuery(TaskCollection).FilterField("cluster_id", "=", cluster.ClusterID)
if _, err := m.dsClient.GetAll(ctx, dq, &tasks); err != nil {
    return fmt.Errorf("query tasks for %q: %w", cluster.ClusterID, err)
}
```

**b) Running GCE instances from Compute API**

Instances are labelled `sparkles-cluster=<clusterID>` by the CLI at creation time. The zone list for `cluster.Region` is fetched once and cached; then `ListInstances` is called once per zone with the label filter.

```go
// zonesForRegion returns the zone names for a region, fetching and caching on first call.
func (m *ClusterMonitor) zonesForRegion(ctx context.Context, region string) ([]string, error) {
    if zones, ok := m.zonesCache[region]; ok {
        return zones, nil
    }
    it := m.zonesClient.List(ctx, &computepb.ListZonesRequest{
        Project: m.projectID,
        Filter:  fmt.Sprintf(`name:"%s-*"`, region),
    })
    var zones []string
    for {
        z, err := it.Next()
        if err == iterator.Done { break }
        if err != nil { return nil, fmt.Errorf("list zones for region %q: %w", region, err) }
        zones = append(zones, z.GetName())
    }
    m.zonesCache[region] = zones
    return zones, nil
}
```

Usage in `poll`:

```go
zones, err := m.zonesForRegion(ctx, cluster.Region)
if err != nil {
    return err
}

// vmNames maps "zone/instanceName" → isPreemptable
vmNames := make(map[string]bool)
filter := fmt.Sprintf(`labels.sparkles-cluster="%s"`, cluster.ClusterID)

for _, zone := range zones {
    it := m.instancesClient.List(ctx, &computepb.ListInstancesRequest{
        Project: m.projectID,
        Zone:    zone,
        Filter:  filter,
    })
    for {
        inst, err := it.Next()
        if err == iterator.Done { break }
        if err != nil { return fmt.Errorf("list instances zone %q: %w", zone, err) }
        isSpot := inst.GetScheduling().GetProvisioningModel() == "SPOT"
        vmNames[zone+"/"+inst.GetName()] = isSpot
    }
}
```

The key format `zone/instanceName` matches how the worker stores `Task.Owner` (set in `worker/main.go` as `zone + "/" + instanceName`). In practice a region has 2–4 zones, so this is 2–4 targeted list calls rather than a project-wide scan.

**c) Cloud Batch jobs from Batch API**

Batch jobs are labelled `sparkles-cluster=<clusterID>` (confirmed in `batch_api.py` line 181). The Batch API is scoped to `cluster.Region`.

```go
var batchJobs []*batchpb.Job
parent := fmt.Sprintf("projects/%s/locations/%s", m.projectID, cluster.Region)
filter := fmt.Sprintf(`labels.sparkles-cluster = "%s"`, cluster.ClusterID)
it := m.batchClient.ListJobs(ctx, &batchpb.ListJobsRequest{
    Parent: parent,
    Filter: filter,
})
for {
    job, err := it.Next()
    if err == iterator.Done { break }
    if err != nil { return fmt.Errorf("list batch jobs: %w", err) }
    batchJobs = append(batchJobs, job)
}
```

The filter syntax matches the existing Go code: `labels.sparkles-cluster = "%s"`.

`SubmittedWorkerRequests` is a snapshot of the current Batch job count:

```go
status.SubmittedWorkerRequests = len(batchJobs)
```

### Step 2 — Identify new Batch job completions

Terminal Batch job states are `SUCCEEDED`, `FAILED`, and `CANCELLED`. For each terminal job, check whether its name is already in `SeenCompletions`. New completions are those not yet seen.

```go
allBatchJobNames := make(map[string]struct{}, len(batchJobs))
for _, j := range batchJobs {
    allBatchJobNames[j.Name] = struct{}{}
}

seenSet := make(map[string]struct{}, len(status.SeenCompletions))
for _, id := range status.SeenCompletions {
    seenSet[id] = struct{}{}
}

var newCompletions []*batchpb.Job
for _, j := range batchJobs {
    if !isBatchJobTerminal(j) { continue }
    if _, seen := seenSet[j.Name]; seen { continue }
    newCompletions = append(newCompletions, j)
    seenSet[j.Name] = struct{}{}
}

// Prune IDs no longer present in the live query, then append new ones
kept := status.SeenCompletions[:0]
for _, id := range status.SeenCompletions {
    if _, exists := allBatchJobNames[id]; exists {
        kept = append(kept, id)
    }
}
for _, j := range newCompletions {
    kept = append(kept, j.Name)
}
status.SeenCompletions = kept
```

```go
func isBatchJobTerminal(j *batchpb.Job) bool {
    s := j.Status.State
    return s == batchpb.JobStatus_SUCCEEDED ||
        s == batchpb.JobStatus_FAILED ||
        s == batchpb.JobStatus_CANCELLED
}
```

### Step 3 — Classify new completions

For each new terminal Batch job, compute its runtime and categorise it.

Runtime is obtained via `job.GetStatus().GetRunDuration().AsDuration()` (confirmed from existing Go code in the codebase).

```go
for _, j := range newCompletions {
    runtime := j.GetStatus().GetRunDuration().AsDuration()
    isShort := runtime < 10*time.Second
    isSuccess := j.GetStatus().GetState() == batchpb.JobStatus_SUCCEEDED

    if isShort {
        status.ShortFailedWorkerRequests++
    } else if isSuccess {
        status.CompletedWorkerRequests++
    } else {
        status.OtherFailedWorkerRequests++
    }
}
```

A job that `SUCCEEDED` but ran for < 10 seconds is still counted as `ShortFailedWorkerRequests` — short duration implies a startup/config failure regardless of reported exit state. This matches the spec's intent.

### Step 4 — (Handled inline in Step 3)

### Step 5 — Match VMs with tasks by owner

```go
// claimedByOwner: "zone/instanceName" → Task
claimedByOwner := make(map[string]*Task)
for i := range tasks {
    if tasks[i].Status == "claimed" {
        claimedByOwner[tasks[i].Owner] = &tasks[i]
    }
}
```

`Task.Owner` is stored as `"zone/instanceName"` (set in the worker as `zone + "/" + instanceName`), which matches the keys in `vmNames` built in Step 1b.

### Step 6-9 — Compute counts

```go
status.InstanceInUseCount = 0
status.IdleInstanceCount = 0
status.PreemptableInstanceCount = 0
status.NonPreemptableInstanceCount = 0

for vmKey, isSpot := range vmNames {
    if isSpot {
        status.PreemptableInstanceCount++
    } else {
        status.NonPreemptableInstanceCount++
    }
    if _, hasClaimed := claimedByOwner[vmKey]; hasClaimed {
        status.InstanceInUseCount++
    } else {
        status.IdleInstanceCount++
    }
}

// All tasks with status "claimed", whether or not their VM is still alive
status.RunningTaskCount = len(claimedByOwner)

// Claimed tasks whose owner VM is not in the live VM set
status.OrphanedTaskCount = 0
for owner := range claimedByOwner {
    if _, alive := vmNames[owner]; !alive {
        status.OrphanedTaskCount++
    }
}
```

Preemptability is read from the GCE instance's `scheduling.provisioningModel` field (SPOT vs STANDARD/PREEMPTIBLE), which Batch sets when it provisions the VM. This avoids a cross-reference join between Compute instances and Batch jobs.

### Step 10 — Write back to Datastore

```go
status.LastUpdate = time.Now()
if _, err := m.dsClient.Put(ctx, key, &status); err != nil {
    return fmt.Errorf("put ClusterStatus for %q: %w", cluster.ClusterID, err)
}
```

---

## Background Goroutine: `(m *ClusterMonitor) start(ctx)`

Uses `time.Sleep` rather than a `time.Ticker` — the goal is to throttle frequency, not hit exact intervals. Clusters are polled sequentially. Any cluster with no activity in the last 24 hours is skipped to avoid unnecessary API calls.

Activity is determined by querying `Cluster` entities directly for `last_updated > cutoff`. This is efficient (one Datastore query, no task scan) because the worker bumps `Cluster.LastUpdated` every time a new VM starts.

```go
func (m *ClusterMonitor) start(ctx context.Context) {
    go func() {
        for {
            select {
            case <-ctx.Done():
                return
            default:
            }

            cutoff := time.Now().Add(-24 * time.Hour)
            dq := datastore.NewQuery(ClusterCollection).FilterField("last_updated", ">", cutoff)
            var clusters []Cluster
            if _, err := m.dsClient.GetAll(ctx, dq, &clusters); err != nil {
                log.Printf("ClusterHealthMonitor: failed to list active clusters: %v", err)
            } else {
                for _, c := range clusters {
                    if err := m.poll(ctx, c); err != nil {
                        log.Printf("ClusterHealthMonitor: poll(%q) error: %v", c.ClusterID, err)
                    }
                }
            }

            select {
            case <-ctx.Done():
                return
            case <-time.After(60 * time.Second):
            }
        }
    }()
}
```

---

## Call Site in `main()`

No new flag needed. Region comes from the `Cluster` entity. Construct the monitor, then call `start`:

```go
monitor := &ClusterMonitor{
    projectID:       *projectID,
    dsClient:        dsClient,
    instancesClient: instancesClient,
    zonesClient:     zonesClient,
    batchClient:     batchClient,
    zonesCache:      make(map[string][]string),
}

startSummaryUpdater(ctx)
monitor.start(ctx)
```

The three new clients (`instancesClient`, `zonesClient`, `batchClient`) are created and defer-closed in `main()` before this block, following the same pattern as `dsClient` and `psClient`.

---

## New HTTP Endpoint

```
GET /api/v1/cluster/{cluster_id}/status
```

```go
func handleClusterStatus(w http.ResponseWriter, r *http.Request) {
    clusterID := r.PathValue("cluster_id")
    var status ClusterStatus
    key := datastore.NameKey(ClusterStatusCollection, clusterID, nil)
    if err := dsClient.Get(r.Context(), key, &status); err == datastore.ErrNoSuchEntity {
        writeError(w, http.StatusNotFound, "NOT_FOUND", "no status for cluster")
        return
    } else if err != nil {
        writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore get failed")
        return
    }
    writeJSON(w, http.StatusOK, status)
}
```

Registered in `main()`:

```go
mux.HandleFunc("GET /api/v1/cluster/{cluster_id}/status", handleClusterStatus)
```

---

## Implementation Checklist

**Worker (`cli/sparklespray/worker/src/github.com/broadinstitute/sparklesworker/events.go`)**

1. Add `Region`, `LastUpdated`, and `Expiry` fields to `Cluster` struct.
2. Update `WriteCluster` signature to accept `zone string`; derive `Region` from it; populate all three new fields.
3. Update `WriteCluster` call site in `worker/main.go` to pass `zone`.

**Dashboard backend (`dashboard-backend/main.go`)** 4. Add `cloud.google.com/go/compute/apiv1` and `cloud.google.com/go/batch/apiv1` via `go get`. 5. Create `instancesClient`, `zonesClient`, and `batchClient` locals in `main()`; defer-close them; pass into `ClusterMonitor`. 6. Add `Region`, `LastUpdated`, and `Expiry` fields to the dashboard's `Cluster` struct. 7. Add `ClusterStatusCollection` constant and `ClusterStatus` struct. 8. Define `ClusterMonitor` struct. 9. Implement methods: `(m *ClusterMonitor) zonesForRegion`, `(m *ClusterMonitor) poll`, `(m *ClusterMonitor) start`. 10. Implement helper: `isBatchJobTerminal`. 11. Construct `ClusterMonitor` in `main()` and call `monitor.start(ctx)`. 12. Implement and register `handleClusterStatus`.

---

All questions resolved. No open items.
