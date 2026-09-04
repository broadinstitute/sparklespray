# Autoscaler Testing Strategy

The autoscaler is complex enough — multiple pollers, time-dependent logic, two external services, a
PubSub throttle, and a state machine — that correctness cannot be verified by inspection alone. This
document describes the interfaces to mock, the unit tests to write per component, and the end-to-end
scenarios that verify the system as a whole.

---

## Guiding principles

- **No real GCP calls in tests.** Every external service is hidden behind an interface with an
  in-memory fake. Tests run offline, fast, and deterministically.
- **Control time explicitly.** All time-dependent logic (grace periods, fallback timers, heartbeat
  expiry) uses an injected clock. Tests advance time manually rather than sleeping.
- **Test state transitions, not implementation.** Assert what changed in Firestore (WorkPool status,
  BatchAPIRequest status, Task status) rather than which internal methods were called.
- **One scenario per test.** Each test sets up the minimal state needed to exercise one behavior and
  asserts the minimal outcome. Avoid multi-step mega-tests.

---

## Interfaces

These are the seams to mock. Each interface has exactly one real implementation (talking to GCP /
Firestore) and one fake implementation used in tests.

### `Clock`

All `now()` calls in the autoscaler go through this interface. The fake starts at an arbitrary fixed
time and advances only when the test calls `advance(duration)`.

```
interface Clock:
    now() → timestamp
```

### `BatchAPIClient`

Wraps the GCP Batch API. The fake maintains an in-memory dict of jobs keyed by `job_id`.

```
interface BatchAPIClient:
    create_job(workpool_id, batch_id, vm_count, preemptible) → job_id
    get_job_status(job_id) → BatchJobStatus
        # BatchJobStatus: QUEUED | SCHEDULED | RUNNING | SUCCEEDED | FAILED
    list_running_vms(label_filter) → dict[instance_name → VMInfo]
        # label_filter is either batch_id or workpool_id
    terminate_vm(instance_name) → void
    terminate_job(job_id) → void
```

The fake for `list_running_vms` derives its answer from the fake job state: a job in state RUNNING
has all its VMs present; SUCCEEDED/FAILED has none. Individual VMs can be removed by calling
`terminate_vm` on the fake.

The fake also supports test helpers:

```
fake.set_job_status(job_id, status)   # transition a job to a given status
fake.fail_vm(instance_name)           # remove a specific VM without terminating the job
```

### `FirestoreClient`

Rather than mocking Firestore's SDK directly, expose a typed interface per collection. Tests use
an in-memory dict-backed implementation.

```
interface WorkPoolStore:
    get(workpool_id) → WorkPool
    update(workpool_id, fields) → void

interface BatchRequestStore:
    create(batch) → void
    get(batch_id) → BatchAPIRequest
    update(batch_id, fields) → void
    list_by_workpool(workpool_id, status_filter) → [BatchAPIRequest]

interface WorkerStore:
    get(worker_id) → Worker
    list_by_workpool(workpool_id) → [Worker]
    list_by_batch(batch_id) → [Worker]
    update(worker_id, fields) → void

interface TaskStore:
    list_by_worker(worker_id, status_filter) → [Task]
    list_by_workpool(workpool_id, status_filter) → [Task]
    update(task_id, fields) → void
```

The in-memory fake should support a `snapshot()` / `restore()` helper so tests can cheaply clone
state before a step and compare what changed after.

### `PubSubReceiver`

The fake exposes a `deliver(notification)` method that tests call to simulate an arriving message.
The autoscaler registers a callback; the fake invokes it synchronously when `deliver` is called.

```
interface PubSubReceiver:
    on_message(callback: fn(notification) → void) → void
```

### `scheduler.Scheduler`

The leading-edge throttle with trailing coalescing is implemented in the `scheduler` package
(`v100/scheduler/`). Each poller registers with `scheduler.Add(minDelay, maxDelay, callback)` and
receives a `notify` func to call on incoming PubSub notifications.

In tests, construct the scheduler with `scheduler.New(fakeClock)` where `fakeClock` is a
`*scheduler.FakeClock`. Advance time with `fakeClock.Advance(d)` and drive the main loop with
`stepLoop` (see `scheduler_test.go`) rather than a real goroutine:

```go
timerCh, runDue := sched.GetNextCallback()
select {
case cb := <-sched.NotifyChannel():
    cb()
case <-timerCh:
    runDue()
}
```

The `scheduler` package itself is already tested in `scheduler/scheduler_test.go`. Autoscaler tests
only need to verify that the correct `notify` func is called when a PubSub notification arrives for
a given batch, and that the right poller callback fires as a result.

---

## Unit tests

### `scheduler.Scheduler`

These tests live in `scheduler/scheduler_test.go` and are already implemented. They cover the full
throttle/coalescing contract so autoscaler-level tests don't need to re-verify it:

| Test                                                    | What it covers                                                     |
| ------------------------------------------------------- | ------------------------------------------------------------------ |
| `TestLeadingEdgeFirstCallImmediate`                     | First `notify()` fires callback immediately                        |
| `TestSecondNotifyWithinCooldownSchedulesTrailing`       | Second `notify()` within `minDelay` schedules one trailing call    |
| `TestAdditionalNotifiesWithinCooldownAreCoalesced`      | Further `notify()` calls while trailing is pending are dropped     |
| `TestMaxDelayFiresWithoutNotification`                  | Callback runs after `maxDelay` even with no `notify()`             |
| `TestMaxDelayResetsAfterEachRun`                        | `maxDelay` resets from `lastRan`, not from registration time       |
| `TestNotifyAfterCooldownIsLeadingEdgeAgain`             | `notify()` after cooldown is a new leading edge, not a trailing    |
| `TestMultipleEntriesSoonestDueFirst`                    | Multiple registered entries: soonest due fires first               |
| `TestInFlightLeadingEdgeSchedulesTrailingForLateNotify` | `notify()` while in-flight queues a trailing call                  |
| `TestStepLoopIntegration`                               | `NotifyChannel` and `GetNextCallback` interact correctly in a loop |

### Autoscaler Poll

| Test                                                | Setup                                                                           | Assert                                                     |
| --------------------------------------------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| No pending tasks                                    | 0 pending tasks, 0 workers                                                      | no BatchAPIRequest created                                 |
| Demand met by active workers                        | 5 pending tasks, 5 active workers                                               | no BatchAPIRequest created                                 |
| Creates batch to meet demand                        | 10 pending tasks, 0 workers, max_worker_count=100                               | BatchAPIRequest created with expected_vm_count=10          |
| Caps at max_worker_count                            | 200 pending tasks, 0 workers, max_worker_count=50                               | BatchAPIRequest vm_count=50                                |
| Caps at max_workers_per_request                     | 200 pending tasks, 0 workers, max_worker_count=200, max_workers_per_request=100 | BatchAPIRequest vm_count=100 (not 200)                     |
| Full preemptible budget → preemptible only          | 10 tasks, preemptible_attempted=0, budget=20                                    | one preemptible BatchAPIRequest, vm_count=10               |
| Exhausted preemptible budget → non-preemptible only | 10 tasks, preemptible_attempted=20, budget=20                                   | one non-preemptible BatchAPIRequest                        |
| Partial preemptible budget → split request          | 10 tasks, preemptible_attempted=17, budget=20                                   | one preemptible vm_count=3, one non-preemptible vm_count=7 |
| Halted workpool → no provisioning                   | status=halted                                                                   | no BatchAPIRequest created                                 |
| Workpool transitions idle→ok on first batch         | status=idle, 5 pending tasks                                                    | workpool.status==ok after poll                             |

### Task Recovery

| Test                             | Setup                                                                      | Assert                                                 |
| -------------------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------ |
| Expired heartbeat, claimed tasks | worker heartbeat_expiry < now, 3 tasks owned by worker with status=claimed | all 3 tasks reset to pending, owning_worker_id cleared |
| Expired heartbeat, no tasks      | worker heartbeat_expiry < now, no tasks                                    | no tasks changed                                       |
| Unexpired heartbeat              | worker heartbeat_expiry > now                                              | no tasks changed                                       |
| Mixed workers                    | 1 expired worker with tasks, 1 live worker with tasks                      | only expired worker's tasks reset                      |
| Task in writing state            | worker expired, task.status=writing                                        | task reset to pending                                  |
| Task in pending state            | worker expired, task.status=pending                                        | task unchanged (already pending)                       |

### Cluster Reconciler

**Batch API lifecycle:**

| Test                       | Setup                                                  | Assert                                                               |
| -------------------------- | ------------------------------------------------------ | -------------------------------------------------------------------- |
| FAILED batch               | batch.status=started, Batch API returns FAILED         | batch.status=failed, batch.unhealthy=true, workpool.status=unhealthy |
| SUCCEEDED batch            | batch.status=started, Batch API returns SUCCEEDED      | batch.status=completed, no VM checks, workpool unchanged             |
| FAILED triggers halt check | batch.status=started, FAILED, N-1 prior failed batches | workpool.status=halted                                               |

**Anomaly 1 — over-provisioning:**

| Test                   | Setup                                   | Assert                                                             |
| ---------------------- | --------------------------------------- | ------------------------------------------------------------------ |
| More VMs than expected | gcp_vms count > batch.expected_vm_count | all VMs terminated, batch.status=failed, workpool.status=unhealthy |
| Exact count            | gcp_vms count == expected_vm_count      | no terminations                                                    |

**Anomaly 2 — startup failure:**

| Test                                       | Setup                                                 | Assert                                                                                  |
| ------------------------------------------ | ----------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Before grace period                        | running_since=1min ago, max_time_to_start_worker=5min | no action                                                                               |
| No workers registered after grace          | running_since=6min ago, registered_worker_count=0     | whole batch terminated, batch.status=failed                                             |
| Some workers registered, some VMs orphaned | running_since=6min ago, 3 of 5 VMs have workers       | 2 orphaned VMs terminated, batch.unhealthy=true, batch.status unchanged (still started) |
| Batch API SUCCEEDED during Anomaly 2 check | running_since=6min ago, batch_api_status=SUCCEEDED    | batch.status=completed, no VM terminations                                              |
| running_since null (still queued)          | running_since=null                                    | Anomaly 2 skipped entirely                                                              |

**Anomaly 3 — zombies:**

| Test                            | Setup                                                     | Assert                                           |
| ------------------------------- | --------------------------------------------------------- | ------------------------------------------------ |
| Zombie below threshold          | 1 zombie, max_zombies_before_abort=3                      | 1 VM terminated surgically, batch.unhealthy=true |
| Zombie above threshold          | 4 zombies, max_zombies_before_abort=3                     | all VMs in batch terminated, batch.status=failed |
| Heartbeat expired but VM gone   | worker heartbeat expired, VM not in gcp_vms               | no termination (VM already gone)                 |
| Within shutdown grace period    | heartbeat_expiry = 30s ago, vm_shutdown_grace_period=1min | no action yet                                    |
| Just past shutdown grace period | heartbeat_expiry = 90s ago, vm_shutdown_grace_period=1min | VM terminated                                    |

**Idle transition:**

| Test                            | Setup                           | Assert                    |
| ------------------------------- | ------------------------------- | ------------------------- |
| No VMs remain, status=ok        | all batches completed, no VMs   | workpool.status=idle      |
| No VMs remain, status=unhealthy | all batches done after incident | workpool.status=idle      |
| VMs still present               | one batch still running         | workpool.status unchanged |

### Batch Startup Monitor

| Test                                      | Setup                                                                | Assert                                         |
| ----------------------------------------- | -------------------------------------------------------------------- | ---------------------------------------------- |
| Job reaches RUNNING — stamp running_since | batch.status=pending, API returns RUNNING, running_since=null        | running_since set to now                       |
| running_since not overwritten             | batch.status=pending, running_since already set, API returns RUNNING | running_since unchanged                        |
| FAILED job                                | API returns FAILED                                                   | batch.status=failed, workpool.status=unhealthy |
| SUCCEEDED with no workers                 | API returns SUCCEEDED, registered_worker_count=0                     | batch.status=failed, workpool.status=unhealthy |
| Worker registered → promote to started    | registered_worker_count=1                                            | batch.status=started                           |
| Stuck in queue past max_time_in_queue     | running_since=null, submitted_at > max_time_in_queue ago             | batch.status=failed                            |
| Not yet past max_time_in_queue            | running_since=null, submitted_at 5min ago, max_time_in_queue=15min   | no action                                      |

### Halt threshold check

| Test                            | Setup                                                               | Assert                         |
| ------------------------------- | ------------------------------------------------------------------- | ------------------------------ |
| N consecutive failed batches    | last N classified batches all have status=failed                    | workpool.status=halted         |
| N-1 consecutive failures        | last N-1 classified batches failed, one prior is started            | not halted                     |
| Pattern broken by started batch | failed, started, failed (N=2) — most recent 2 are [failed, started] | not halted                     |
| Pending batches excluded        | 2 failed batches + 1 pending; N=2                                   | halted (pending doesn't count) |
| Halted workpool stays halted    | already halted, another batch fails                                 | status remains halted          |

### Job submission

| Test                | Setup            | Assert                                              |
| ------------------- | ---------------- | --------------------------------------------------- |
| idle → ok           | status=idle      | status=ok, status_message cleared, incident_count=0 |
| halted → ok         | status=halted    | status=ok, status_message cleared, incident_count=0 |
| ok unchanged        | status=ok        | status=ok, no change                                |
| unhealthy unchanged | status=unhealthy | status=unhealthy, no change                         |

---

## Scenario tests

Scenario tests wire all components together (with fakes for external services) and run through a
realistic sequence of events, asserting final state. They're slower than unit tests but verify
that the components interact correctly.

### S1: Happy path — job completes cleanly

1. Submit a job: 20 pending tasks, workpool `idle`
2. Autoscaler poll: expect 1 preemptible BatchAPIRequest (vm_count=20), workpool→`ok`
3. Fake Batch API: transition job to RUNNING
4. Batch startup monitor poll: expect `running_since` stamped, batch stays `pending`
5. Simulate 20 workers registering (increment `registered_worker_count`); batch startup monitor poll: expect batch→`started`
6. Workers complete tasks; all heartbeats expire cleanly (heartbeat_expiry set to now on shutdown)
7. Fake Batch API: transition job to SUCCEEDED
8. Cluster reconciler poll: expect batch→`completed`, workpool→`idle`

### S2: Preemption recovery

1. 10 tasks pending; autoscaler creates a 10-VM preemptible batch; 10 workers register
2. Workers each claim a task
3. Advance time: 3 workers have heartbeat_expiry in the past (preempted); 7 are live
4. Task recovery runs: expect 3 tasks reset to `pending`, owning_worker_id cleared
5. Autoscaler poll: 3 pending tasks, 7 live workers → creates a new batch for 3 VMs
6. New workers register, tasks complete

### S3: Configuration error — workers never start, workpool halted

1. 5 tasks pending; autoscaler creates a 5-VM preemptible batch (Batch 1)
2. Fake Batch API: Batch 1 transitions to RUNNING; `running_since` stamped
3. Advance time past `max_time_to_start_worker`; `registered_worker_count` stays 0
4. Cluster reconciler Anomaly 2: whole batch terminated, Batch 1 → `failed`, workpool → `unhealthy`
5. Halt check: only 1 failed batch; `max_consecutive_failed_batches=2` → not halted
6. Autoscaler poll: 5 tasks still pending; creates Batch 2
7. Same sequence: Batch 2 → `failed`
8. Halt check: 2 consecutive failures → workpool → `halted`
9. Autoscaler poll: `status=halted` → no new batch created
10. Assert tasks remain pending and no further VMs are requested

### S4: Fast-failing batch — caught by the batch startup monitor

1. 5 tasks pending; autoscaler creates Batch 1; job reaches RUNNING, `running_since` stamped
2. VMs fail immediately and disappear; Batch API transitions to FAILED
3. PubSub notification arrives; batch startup monitor runs immediately (leading edge)
4. Batch startup monitor: batch_api_status=FAILED → Batch 1 marked `failed`, workpool → `unhealthy`
5. Autoscaler poll: creates Batch 2; same failure occurs → workpool → `halted`

### S5: Zombie detection — surgical then abort

1. 5-VM batch; all workers register; advance time so 2 workers have heartbeat expired + VMs still running
2. Cluster reconciler Anomaly 3: 2 zombies below `max_zombies_before_abort=3` → 2 VMs terminated surgically
3. Batch stays `started` (not failed); workpool → `unhealthy`; `incident_count=2`
4. Advance time; 3 more workers become zombies
5. Cluster reconciler: 3 zombies > threshold → entire batch terminated, batch → `failed`, halt check runs

### S6: Stuck-in-queue batch

1. Autoscaler creates a batch; Batch API stays in `QUEUED` (no capacity)
2. Batch startup monitor polls every 30s; `running_since` never set
3. Advance time past `max_time_in_queue`
4. Batch startup monitor: batch → `failed`, workpool → `unhealthy`, halt check runs

### S7: Notification burst — coalescing

Uses `scheduler.New(fakeClock)` so time is fully controlled.

1. Register cluster reconciler callback with `sched.Add(min_time_between_polls, max_time_between_polls, clusterReconciler)`; capture `notify`
2. Deliver 10 PubSub notifications in a loop, calling `notify()` each time (t=0s..4s via `fakeClock.Advance`)
3. Step the loop once (no clock advance): assert notify channel fires, cluster reconciler poll runs — poll count = 1
4. `fakeClock.Advance(min_time_between_polls)`: step the loop — scheduled timer fires trailing call, poll count = 2
5. No more items in notify channel
6. `fakeClock.Advance(max_time_between_polls)`: step the loop — fallback timer fires, poll count = 3

### S8: Preemptible budget exhaustion — non-preemptible fallback

1. `max_preemptible_worker_attempts=10`; submit a job with 20 tasks
2. Autoscaler poll: expect one preemptible batch (vm_count=10) + one non-preemptible batch (vm_count=10)
3. All preemptible VMs are preempted; 10 tasks still pending
4. Autoscaler poll: `preemptible_attempted=10`, budget exhausted → non-preemptible batch for 10

### S9: New job submission resets halted workpool

1. Workpool reaches `halted` via repeated failures; `incident_count=4`
2. User submits a new job
3. Job submission: workpool → `ok`, `status_message` cleared, `incident_count=0`
4. Autoscaler poll: provisioning resumes (status is no longer `halted`)

---

## Test infrastructure notes

- **Firestore emulator**: for integration tests that want real Firestore semantics (query ordering,
  atomic increments), the [Firestore emulator](https://firebase.google.com/docs/emulator-suite) can
  replace the in-memory fake. Recommended for the `check_halt_threshold` query (ordering by
  `submitted_at DESC`) which is easy to get wrong.
- **Clock injection**: every component should accept a `scheduler.Clock` at construction time.
  `scheduler.RealClock` is used in production; `scheduler.NewFakeClock(epoch)` is used in tests.
  Avoid any direct calls to the system clock; they make tests non-deterministic and impossible to
  fast-forward.
- **Fake atomics**: `registered_worker_count` is incremented atomically in production (Firestore
  transactions). The in-memory fake should enforce this by serializing all updates; concurrent
  increment races must not be possible in tests.
- **Scenario test helpers**: a `World` builder that sets up WorkPool, BatchAPIRequests, Workers, and
  Tasks in a consistent state in one call reduces boilerplate and keeps scenario tests readable.
