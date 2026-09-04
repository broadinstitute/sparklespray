# Plan: `dev add-worker <workpool_spec.json> --project <project>`

## Answers incorporated

- Command will write a `BatchAPIRequest` to Firestore (same as `submitBatch`).
- `--vm-count` defaults to 1.
- `--preemptible` is an optional flag.
- Consolidate to a single `EmptyVolume` definition — use `v100.EmptyVolume` as the canonical shape.

---

## Remaining issue: import cycle prevents literal type sharing

`monitor` cannot import `v100` (comment in `interfaces.go` confirms this to avoid an
import cycle). So there will still be two Go types named `EmptyVolume`. The fix is to
**align their field names and Firestore tags exactly** so conversion is trivial and they
are effectively the same shape:

| Field      | `v100.EmptyVolume` (canonical)                                        | `monitor.EmptyVolume` (must match)              |
| ---------- | --------------------------------------------------------------------- | ----------------------------------------------- |
| Mount path | `MountPoint string` / `firestore:"mount_point"` / `json:"mountPoint"` | currently `MountPath` — rename to `MountPoint`  |
| Type       | `Type string` / `firestore:"type"` / `json:"type"`                    | same ✓                                          |
| Size       | `SizeInGB int` / `firestore:"size_in_gb"` / `json:"sizeInGB"`         | currently `SizeGB int64` — rename + change type |

After this change, field-by-field conversion between the two is a mechanical one-liner.

---

## Files and changes

### 1. `v100/task_queue.go` — add `SparklesWorkerGCSPath` to `v100.WorkPool`

```go
type WorkPool struct {
    ...
    SparklesWorkerGCSPath string `firestore:"sparkles_worker_gcs_path"`
}
```

### 2. `v100/dev/submit.go` — add `SparklesWorkerGCSPath` to `WorkpoolSpec`; copy it in `devSubmit`

```go
type WorkpoolSpec struct {
    ...
    SparklesWorkerGCSPath string `json:"sparklesWorkerGCSPath"`
}
```

`devSubmit` already copies `RootDir` and `EmptyVolumes` to `v100.WorkPool`. Add:

```go
workpool := v100.WorkPool{
    ...
    SparklesWorkerGCSPath: workpoolSpec.SparklesWorkerGCSPath,
}
```

### 3. `v100/monitor/interfaces.go` — three changes

**a) Align `monitor.EmptyVolume` field names to `v100.EmptyVolume`:**

```go
type EmptyVolume struct {
    MountPoint string // was MountPath
    Type       string
    SizeInGB   int    // was SizeGB int64
}
```

**b) Add `RootDir`, `EmptyVolumes`, `SparklesWorkerGCSPath` to `monitor.WorkPool`:**

```go
type WorkPool struct {
    ...
    RootDir               string
    SparklesWorkerGCSPath string
    EmptyVolumes          []EmptyVolume
}
```

**c) Update `WorkerJobSpec` field access** — no signature change needed; callers
that set `EmptyVolumes` will now use the renamed fields.

### 4. `v100/monitor/adapters.go` — add fields to `firestoreWorkPool` and `toWorkPool`

Add to `firestoreWorkPool`:

```go
type firestoreWorkPool struct {
    ...
    RootDir               string        `firestore:"root_dir"`
    SparklesWorkerGCSPath string        `firestore:"sparkles_worker_gcs_path"`
    EmptyVolumes          []EmptyVolume `firestore:"empty_volumes"`
}
```

Note: `monitor.EmptyVolume` must carry the same firestore tags as `v100.EmptyVolume`
(`mount_point`, `type`, `size_in_gb`) so Firestore deserialises documents written by
either path correctly. Add the tags in step 3a.

Update `toWorkPool` to copy the new fields:

```go
return &WorkPool{
    ...
    RootDir:               f.RootDir,
    SparklesWorkerGCSPath: f.SparklesWorkerGCSPath,
    EmptyVolumes:          f.EmptyVolumes,
}
```

### 5. `v100/monitor/batch_api.go` — update field names after rename

`ev.MountPath` → `ev.MountPoint`, `ev.SizeGB` → `ev.SizeInGB`.

### 6. `v100/monitor/remote_batch_client.go` and `emulator/server.go`

Update any struct literals or field accesses that used `MountPath` / `SizeGB`.

### 7. `v100/monitor/provision.go` — fix `submitBatch` (pre-existing bug)

`submitBatch` currently leaves `RootDir`, `SparklesWorkerGCSPath`, and `EmptyVolumes`
empty. Fix:

```go
jobID, err := a.batchAPI.CreateJob(ctx, &WorkerJobSpec{
    ...
    RootDir:               pool.RootDir,
    SparklesWorkerGCSPath: pool.SparklesWorkerGCSPath,
    EmptyVolumes:          pool.EmptyVolumes,
})
```

### 8. `v100/dev/addworker.go` — new file

```go
func runDevAddWorker(c *cli.Context) error {
    specFile  := c.Args().Get(0)   // required
    project   := c.String("project")   // required
    db        := c.String("db")
    vmCount   := c.Int("vm-count")     // default 1
    preemptible := c.Bool("preemptible")

    workpoolSpec, err := readJSON[WorkpoolSpec](specFile)
    workpoolID, err   := resolveWorkpoolID(workpoolSpec)

    batchClient, err := monitor.NewGCPBatchAPIClient(ctx, project)

    batchID := uuid.New().String()
    jobID, err := batchClient.CreateJob(ctx, &monitor.WorkerJobSpec{
        WorkpoolID:            workpoolID,
        BatchID:               batchID,
        Region:                workpoolSpec.Region,
        MachineType:           workpoolSpec.MachineType,
        VMCount:               vmCount,
        Preemptible:           preemptible,
        RootDir:               workpoolSpec.RootDir,
        SparklesWorkerGCSPath: workpoolSpec.SparklesWorkerGCSPath,
        EmptyVolumes:          toMonitorEmptyVolumes(workpoolSpec.EmptyVolumes),
    })

    // write BatchAPIRequest to Firestore
    fsClient, _ := firestore.NewClientWithDatabase(ctx, project, db)
    batchStore := monitor.NewFirestoreBatchRequestStore(fsClient)
    batchStore.Create(ctx, &monitor.BatchAPIRequest{
        BatchID:         batchID,
        JobID:           jobID,
        WorkpoolID:      workpoolID,
        ExpectedVMCount: vmCount,
        Preemptible:     preemptible,
        SubmittedAt:     time.Now(),
        Status:          monitor.BatchStatusPending,
    })

    fmt.Printf("created batch job: %s (batchID: %s)\n", jobID, batchID)
}

// toMonitorEmptyVolumes converts v100.EmptyVolume → monitor.EmptyVolume.
// After field-name alignment (step 3a) this is a trivial copy.
func toMonitorEmptyVolumes(vs []v100.EmptyVolume) []monitor.EmptyVolume {
    out := make([]monitor.EmptyVolume, len(vs))
    for i, v := range vs {
        out[i] = monitor.EmptyVolume{MountPoint: v.MountPoint, Type: v.Type, SizeInGB: v.SizeInGB}
    }
    return out
}
```

### 9. `v100/dev/commands.go` — register `add-worker`

```go
{
    Name:      "add-worker",
    ArgsUsage: "<workpool-spec-json>",
    Flags: []cli.Flag{
        cli.StringFlag{Name: "project"},
        cli.StringFlag{Name: "db", Value: defaultDB},
        cli.IntFlag{Name: "vm-count", Value: 1},
        cli.BoolFlag{Name: "preemptible"},
    },
    Action: runDevAddWorker,
},
```

---

## Execution order

1. Align `monitor.EmptyVolume` fields (step 3a) + add firestore tags — compile-fix
   `batch_api.go`, `remote_batch_client.go`, `emulator/server.go` (step 5, 6).
2. Add `SparklesWorkerGCSPath` to `v100.WorkPool` and `WorkpoolSpec` (steps 1–2).
3. Add fields to `monitor.WorkPool`, `firestoreWorkPool`, `toWorkPool` (steps 3b, 4).
4. Fix `provision.go` (step 7).
5. Implement `add-worker` command (steps 8–9).
