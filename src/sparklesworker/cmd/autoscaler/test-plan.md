# Test Plan: Autoscaler

## Context

A large amount of new code has been added across the autoscaler package:

- `RedisMethodsForPoll` and `RedisSparklesMethodsForPoll` (new Redis shims for local testing)
- `StartMockBatchAPI` (subprocess-based mock for the Batch API)
- Updated `poll.go` integrating the new shims

Current tests (`poll_test.go`) focus on the deterministic logic in `poll.go` using in-memory mocks. The new Redis implementations and mock batch API have **zero test coverage**. Several pure helper functions are also untested.

---

## Files to Create/Modify

- `cmd/autoscaler/helpers_test.go` — new: unit tests for pure helper functions
- `cmd/autoscaler/redis_test.go` — new: integration tests for Redis shims using miniredis
- `cmd/autoscaler/poll_test.go` — add: two new edge-case Poll() tests

---

## Step 1 — Add `miniredis` dependency

`github.com/alicebob/miniredis/v2` provides an in-memory Redis server for Go tests — no external daemon needed. Add it to `go.mod`:

```bash
go get github.com/alicebob/miniredis/v2
```

---

## Step 2 — `helpers_test.go`: pure function unit tests

These functions have no external dependencies and are directly testable.

### `normalizeLabel()` (batch_api.go:50)

| Input           | Expected Output | Why                               |
| --------------- | --------------- | --------------------------------- |
| `"my-cluster"`  | `"my-cluster"`  | already valid                     |
| `"My Cluster!"` | `"my-cluster-"` | uppercase + special chars         |
| `"123abc"`      | `"x-123abc"`    | starts with digit -> prepend "x-" |
| `""`            | `""`            | empty string                      |

### `makeUniqueLabel()` (batch_api.go:61)

- Output length <= 63 chars
- Output contains the truncated input prefix
- Long input (>57 chars) is truncated correctly

### `getMonitorState()` (types.go:88)

- Empty `MonitorState` string -> returns empty `MonitorState{}` with no error
- Valid JSON -> round-trips all three fields correctly
- Invalid JSON -> returns non-nil error

---

## Step 3 — `redis_test.go`: Redis shim integration tests

Use `miniredis.RunT(t)` to create an in-process Redis per test.

### `RedisMethodsForPoll` (redis_methods.go)

**TestRedisMethodsForPoll_SubmitAndList**

- Submit 2 jobs with `submitBatchJobs`, different instance counts
- Call `listBatchJobs` -> verify both appear with `State=Pending` and correct `RequestedInstances`

**TestRedisMethodsForPoll_RegionFilter**

- Submit one job in region A, one in region B
- `listBatchJobs` with region A -> only returns region A job

**TestRedisMethodsForPoll_DeleteAll**

- Submit 2 jobs, call `deleteAllBatchJobs`, then `listBatchJobs` -> returns empty

### `RedisSparklesMethodsForPoll` (redis_sparkles_methods.go)

**TestRedisSparkles_ClusterRoundTrip**

- Store a cluster JSON directly in Redis (`SET cluster:testcluster ...`)
- Call `getClusterConfig("testcluster")` -> verify all fields match

**TestRedisSparkles_MonitorStateRoundTrip**

- Pre-seed a cluster, call `updateClusterMonitorState` with a `MonitorState`, then re-read
  with `getClusterConfig` + `getMonitorState()` -> verify all fields persist

**TestRedisSparkles_TaskFiltering**

- Seed 3 tasks: one pending, one claimed, one complete
  (in both `task:{id}` keys and `tasks:{clusterID}` SET)
- `getClaimedTasks` -> 1 task
- `getPendingTaskCount` -> 1
- `getNonCompleteTaskCount` -> 2 (pending + claimed)

**TestRedisSparkles_MarkTasksPending**

- Seed a claimed task, call `markTasksPending`, re-read -> `status=pending`, `OwnedByWorkerID=""`

**TestRedisSparkles_GetTasksCompletedBy**

- Seed 2 tasks owned by jobA (1 complete, 1 pending), 1 task owned by jobB (complete)
- `getTasksCompletedBy("jobA")` -> 1; `getTasksCompletedBy("jobB")` -> 1; `"jobC"` -> 0

### `MockBatchAPI` (mock_batchapi.go)

Test `mockBatchAPIPoll` directly (not `StartMockBatchAPI`) to avoid timing races.

**TestMockBatchAPI_CompleteJob**

- Seed a `redisBatchJob` with `State=Pending`, `WorkerCommandArgs=["true"]`, `InstanceCount=2`
- Call `mockBatchAPIPoll` -> job becomes `Running`
- Poll Redis until job reaches final state (with timeout) -> expect `Complete`

**TestMockBatchAPI_FailedJob**

- Same but with `WorkerCommandArgs=["false"]`
- Expect final state `Failed`

---

## Step 4 — Additional `poll_test.go` edge cases

**TestPoll_CorruptedMonitorState**

- Set `clusterConfig.MonitorState = "not-valid-json"` in mockSparkles
- `Poll()` should return a non-nil error

**TestPoll_SubmitFailure**

- `submitBatchJobsFn` returns an error
- `Poll()` returns an error and `updateClusterMonitorState` is NOT called with an incremented `batchJobRequests`

---

## Verification

```bash
cd go/src/sparklesworker
go test ./cmd/autoscaler/... -v -run TestRedis
go test ./cmd/autoscaler/... -v -run TestMock
go test ./cmd/autoscaler/... -v -run TestHelpers
go test ./cmd/autoscaler/...   # all pass
```
