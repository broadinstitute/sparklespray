package monitor

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Runs on PubSub notifications for pending batches
// or after max_time_between_polls. Only processes pending batches; once promoted to started,
// failed, or completed, Tier 2 owns the batch.
func (a *Monitor) runBatchStartupMonitor(ctx context.Context) error {
	pools, err := a.pools.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list workpools: %w", err)
	}

	for _, pool := range pools {
		pendingBatches, err := a.batches.ListByWorkpool(ctx, pool.WorkpoolID, []BatchStatus{BatchStatusPending})
		if err != nil {
			log.Printf("tier3: list pending batches for workpool %s: %v", pool.WorkpoolID, err)
			continue
		}

		now := a.clock.Now()
		for _, batch := range pendingBatches {
			if err := a.checkBatchStartup(ctx, pool, batch, now); err != nil {
				log.Printf("tier3: batch %s: %v", batch.BatchID, err)
			}
		}
	}
	return nil
}

func (a *Monitor) checkBatchStartup(ctx context.Context, pool *WorkPool, batch *BatchAPIRequest, now time.Time) error {
	apiStatus, err := a.batchAPI.GetJobStatus(ctx, batch.JobID)
	if err != nil {
		return fmt.Errorf("get job status: %w", err)
	}

	if apiStatus == BatchJobStatusDeleted {
		log.Printf("tier3: batch %s: GCP job %s no longer exists (404); marking batch as deleted", batch.BatchID, batch.JobID)
		batch.Status = BatchStatusDeleted
		return a.batches.Save(ctx, batch)
	}

	if apiStatus == BatchJobStatusRunning && batch.RunningSince == nil {
		t := now
		batch.RunningSince = &t
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch (running_since): %w", err)
		}
	}

	if apiStatus == BatchJobStatusFailed {
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(pool, fmt.Sprintf("Batch job %s failed before any workers started", batch.JobID), now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
		if err := a.pools.Save(ctx, pool); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
		return a.checkHaltThreshold(ctx, pool)
	}

	if apiStatus == BatchJobStatusSucceeded {
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(pool, fmt.Sprintf("Batch job %s completed with no workers registered", batch.JobID), now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
		if err := a.pools.Save(ctx, pool); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
		return a.checkHaltThreshold(ctx, pool)
	}

	if batch.RegisteredWorkerCount >= 1 {
		batch.Status = BatchStatusStarted
		return a.batches.Save(ctx, batch)
	}

	maxTimeInQueue := param(pool.MaxTimeInQueue, defaultMaxTimeInQueue)
	if batch.RunningSince == nil && now.Sub(batch.SubmittedAt) > maxTimeInQueue {
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(pool,
			fmt.Sprintf("Batch job %s never left the queue within max_time_in_queue", batch.JobID),
			now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
		if err := a.pools.Save(ctx, pool); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
		return a.checkHaltThreshold(ctx, pool)
	}

	return nil
}
