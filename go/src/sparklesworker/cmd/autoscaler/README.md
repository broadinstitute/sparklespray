# autoscaler

The autoscaler monitors a Sparklespray cluster and manages the lifecycle of GCP Batch worker nodes. It runs as a polling loop, adjusting the number of running workers to match the current task queue depth.

## How it works

Each poll cycle runs three phases:

### Phase 0 — Cluster health check

Before scaling, the autoscaler checks whether any batch jobs have completed without doing any useful work (i.e. the job started, ran, and stopped without completing a single Sparkles task). This is a signal that the worker configuration is broken — for example, a bad Docker image or misconfigured command.

If the count of such "suspicious failures" exceeds `MaxSuspiciousFailures` (a field on the cluster config), the autoscaler deletes all running batch jobs and halts further scaling to avoid repeatedly launching broken workers.

### Phase 1 — Orphan detection

The autoscaler queries Firestore for tasks with `status=claimed` whose `owned_by_worker_id` does not correspond to any currently running GCE instance. These are "orphaned" tasks — their worker has disappeared (preempted, crashed, etc.) without completing the task. Orphaned tasks are reset to `status=pending` so they can be picked up again.

### Phase 2 — Scaling

The autoscaler counts non-complete tasks and compares that to the number of workers already requested (active Batch jobs). If more workers are needed, it submits new GCP Batch jobs up to `MaxInstanceCount`.

**Preemptable node strategy:**

The cluster config includes a `MaxPreemptableAttempts` budget. On a cold start (no running workers), the autoscaler:

1. Creates one **lingering** non-preemptable node first. This node uses a longer idle timeout so it stays alive briefly after the queue drains, avoiding a full cold-start penalty if new tasks arrive shortly after.
2. Fills the remaining slots with **preemptable (Spot) VMs** until the preemptable budget is exhausted.
3. Any remaining slots beyond the budget use non-preemptable VMs.

The preemptable budget resets each time the cluster drains to zero workers.

### State persistence

The autoscaler persists a small `MonitorState` blob in the cluster's Firestore document between polls. This tracks:

- `batchJobRequests` — cumulative count of Batch job submissions, used to detect GCP API propagation delays (if the number of visible Batch jobs doesn't match the expected count, the autoscaler skips that poll).
- `completedJobIds` — previously seen completed job IDs, so each job is only evaluated for suspicious failures once.
- `suspiciouslyFailedToRun` — running total of suspicious failures.

## Configuration

Cluster configuration is stored in the Firestore `Cluster` collection. Key fields:

| Field                      | Description                                              |
| -------------------------- | -------------------------------------------------------- |
| `machine_type`             | GCE machine type for worker nodes                        |
| `worker_docker_image`      | Container image run on each worker                       |
| `worker_command_args`      | Command and arguments passed to the container            |
| `region`                   | GCP region for Batch job submission (e.g. `us-central1`) |
| `zones`                    | List of GCE zones to query for running instances         |
| `max_instance_count`       | Hard cap on the number of concurrent workers             |
| `max_preemptable_attempts` | Total preemptable node budget per run                    |
| `max_suspicious_failures`  | Threshold before the autoscaler halts the cluster        |

## Running

**Against GCP:**

```
sparklesworker autoscaler \
  --project my-gcp-project \
  --cluster my-cluster-id \
  --poll-interval 10s
```

**Local testing with Redis (see below):**

```
sparklesworker autoscaler \
  --cluster my-cluster-id \
  --redis localhost:6379 \
  --poll-interval 2s
```

## Local testing mode (`--redis`)

Passing `--redis <addr>` switches the autoscaler to a fully local mode that requires no GCP credentials. Both backends — the Sparkles task queue and the GCP Batch API — are replaced with Redis-backed fakes.

### Redis-backed Sparkles methods

`RedisSparklesMethodsForPoll` replaces Firestore with Redis for all task queue and cluster config operations. The key layout is:

| Key                   | Value                                          |
| --------------------- | ---------------------------------------------- |
| `cluster:<clusterID>` | JSON-encoded `Cluster` config                  |
| `task:<taskID>`       | JSON-encoded `task_queue.Task`                 |
| `tasks:<clusterID>`   | Redis SET of task IDs belonging to the cluster |

### Mock Batch API

`StartMockBatchAPI` runs a background goroutine that simulates GCP Batch. It polls Redis for `batch_job:*` keys with `state=Pending`, then:

1. Marks the job `Running`.
2. Launches `InstanceCount` copies of `WorkerCommandArgs` as local subprocesses concurrently.
3. Waits for all subprocesses to finish, then marks the job `Complete` (all succeeded) or `Failed` (any failed).

Batch jobs are stored in Redis under `batch_job:<jobID>` and contain the same fields the GCP path uses (`state`, `cluster_id`, `region`, `instance_count`, `worker_command_args`, `worker_docker_image`).

This means you can test the full autoscaler loop — including orphan recovery, health checks, and scaling logic — by pointing a local Redis instance at the autoscaler and seeding cluster config and tasks directly into Redis.
