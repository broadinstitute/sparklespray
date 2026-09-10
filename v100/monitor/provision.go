package monitor

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"time"
)

const batchIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// CreateBatchID generates a random batch ID for a new GCP Batch job submission.
func CreateBatchID() string {
	const idLen = 30
	b := make([]byte, idLen)
	for i := range b {
		b[i] = batchIDChars[rand.IntN(len(batchIDChars))]
	}
	return "sparkles-" + string(b)
}

// runProvisioningPoll is the provisioning loop. Runs every 1 minute.
// For each workpool it compares pending task demand against active worker supply
// and submits BatchAPIRequests to close the gap, preferring preemptible VMs up to the
// workpool's budget before falling back to non-preemptible.
func (a *Monitor) runProvisioningPoll(ctx context.Context) error {

	all, err := a.pools.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list workpools: %w", err)
	}
	a.vlogf("list work pools returned %d", len(all))
	var pools []*WorkPoolWithState
	for _, ws := range all {
		a.vlogf("runProvisioningPoll pool %s status = %s", ws.Pool.WorkpoolID, string(ws.State.State))
		if ws.State.State != WorkPoolStatusIdle && ws.State.State != WorkPoolStatusHalted {
			pools = append(pools, ws)
		}
	}
	a.vlogf("After filtering by not idle and not halted: %d", len(pools))

	if len(pools) > 0 {
		a.lastActivity = time.Now()
		a.vlogf("provisioning poll: Found %d active workpools", len(pools))

		for _, ws := range pools {
			if err := a.runProvisioningPollForWorkpool(ctx, ws); err != nil {
				log.Printf("provisioning poll: workpool %s: %v", ws.Pool.WorkpoolID, err)
			}
		}
	}
	return nil
}

func (a *Monitor) runProvisioningPollForWorkpool(ctx context.Context, ws *WorkPoolWithState) error {
	// workpool.status is the provisioning guard: halted means stop.
	if ws.State.State == WorkPoolStatusHalted {
		return nil
	}

	pool := ws.Pool
	now := a.clock.Now()

	pendingCount, err := a.tasks.CountPending(ctx, pool.WorkpoolID)
	if err != nil {
		return fmt.Errorf("count pending tasks: %w", err)
	}

	// Count VMs already requested (pending or started) rather than live workers.
	// Workers take time to start, so counting heartbeats would trigger double-provisioning
	// while the first batch is still coming up.
	activeBatches, err := a.batches.ListByWorkpool(ctx, pool.WorkpoolID, []BatchStatus{
		BatchStatusPending, BatchStatusStarted,
	})
	if err != nil {
		return fmt.Errorf("list active batches: %w", err)
	}
	requestedCount := 0
	for _, b := range activeBatches {
		requestedCount += b.ExpectedVMCount
	}

	target := min(pool.MaxWorkerCount, pendingCount)
	needed := max(0, target-requestedCount)
	if needed == 0 {
		a.vlogf("provisioning poll: %d pending tasks in pool %s, but %d already requested, so nothing more needed", (pendingCount), pool.WorkpoolID, requestedCount)
		return nil
	}

	maxPerRequest := param(pool.MaxWorkersPerRequest, defaultMaxWorkersPerRequest)
	toRequest := min(needed, maxPerRequest)

	preemptibleAttempted, err := a.batches.SumPreemptibleVMCount(ctx, pool.WorkpoolID)
	if err != nil {
		return fmt.Errorf("sum preemptible VM count: %w", err)
	}

	remainingPreemptible := max(0, pool.MaxPreemptibleWorkerAttempts-preemptibleAttempted)
	preemptibleCount := min(toRequest, remainingPreemptible)
	nonPreemptibleCount := toRequest - preemptibleCount

	a.vlogf("monitor: submitting a batch request for %d preemptible VMs and %d nonpreemptible VMs (already requested %d)", preemptibleCount, nonPreemptibleCount, requestedCount)
	if preemptibleCount > 0 {
		if err := a.submitBatch(ctx, pool, preemptibleCount, true, now); err != nil {
			return fmt.Errorf("submit preemptible batch: %w", err)
		}
	}

	if nonPreemptibleCount > 0 {
		if err := a.submitBatch(ctx, pool, nonPreemptibleCount, false, now); err != nil {
			return fmt.Errorf("submit non-preemptible batch: %w", err)
		}
	}

	return nil
}

// submitBatch creates a BatchAPIRequest in Firestore and the corresponding GCP Batch API job.
func (a *Monitor) submitBatch(ctx context.Context, pool *WorkPool, vmCount int, preemptible bool, now time.Time) error {
	batchID := CreateBatchID()

	jobID, err := a.batchAPI.CreateJob(ctx, &WorkerJobSpec{
		WorkpoolID:            pool.WorkpoolID,
		BatchID:               batchID,
		ProjectID:             pool.ProjectID,
		Region:                pool.Region,
		MachineType:           pool.MachineType,
		VMCount:               vmCount,
		Preemptible:           preemptible,
		RootDir:               pool.RootDir,
		SparklesWorkerGCSPath: pool.SparklesWorkerGCSPath,
		EmptyVolumes:          pool.EmptyVolumes,
		Resources:             pool.Resources,
		ServiceAccount:        pool.ServiceAccount,
		Labels:                pool.Labels,
		DBName:                a.dbName,
		LingerTime:            pool.LingerTime,
		BootDiskSizeGb:        pool.BootDiskSizeGb,
		BootDiskType:          pool.BootDiskType})
	if err != nil {
		if a.batchOutcomes != nil {
			if pubErr := a.batchOutcomes.PublishBatchFailed(ctx, pool.WorkpoolID, err.Error()); pubErr != nil {
				log.Printf("submitBatch: publish batch_failed for workpool %s: %v", pool.WorkpoolID, pubErr)
			}
		}
		return fmt.Errorf("create GCP batch job: %w", err)
	}

	batch := &BatchAPIRequest{
		BatchID:         batchID,
		JobID:           jobID,
		ProjectID:       pool.ProjectID,
		WorkpoolID:      pool.WorkpoolID,
		ExpectedVMCount: vmCount,
		Preemptible:     preemptible,
		SubmittedAt:     now,
		Expiry:          now.Add(7 * 24 * time.Hour),
		Status:          BatchStatusPending,
		Labels:          pool.Labels,
	}
	if err := a.batches.Create(ctx, batch); err != nil {
		return fmt.Errorf("save batch record: %w", err)
	}
	return nil
}
