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

	for _, ws := range pools {
		pendingBatches, err := a.batches.ListByWorkpool(ctx, ws.Pool.WorkpoolID, []BatchStatus{BatchStatusPending})
		if err != nil {
			log.Printf("tier3: list pending batches for workpool %s: %v", ws.Pool.WorkpoolID, err)
			continue
		}

		now := a.clock.Now()
		for _, batch := range pendingBatches {
			if err := a.checkBatchStartup(ctx, ws, batch, now); err != nil {
				log.Printf("tier3: batch %s: %v", batch.BatchID, err)
			}
		}
	}
	return nil
}

func (a *Monitor) checkBatchStartup(ctx context.Context, ws *WorkPoolWithState, batch *BatchAPIRequest, now time.Time) error {
	apiStatus, err := a.batchAPI.GetJobStatus(ctx, batch.JobID)
	if err != nil {
		return fmt.Errorf("get job status: %w", err)
	}

	// RegisteredWorkerCount is derived from actual Worker records — it's the
	// monitor's job to compute and persist it, not the worker's.
	workers, err := a.workers.ListByBatch(ctx, batch.BatchID)
	if err != nil {
		return fmt.Errorf("list workers for batch: %w", err)
	}
	batch.RegisteredWorkerCount = len(workers)

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

	if apiStatus == BatchJobStatusFailed && batch.RegisteredWorkerCount == 0 {
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(ws.State, fmt.Sprintf("Batch job %s failed before any workers started", batch.JobID), now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
		if err := a.saveState(ctx, ws.State); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
		return a.checkHaltThreshold(ctx, ws.Pool, ws.State)
	}

	if apiStatus == BatchJobStatusSucceeded && batch.RegisteredWorkerCount == 0 {
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(ws.State, fmt.Sprintf("Batch job %s completed with no workers registered", batch.JobID), now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
		if err := a.saveState(ctx, ws.State); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
		return a.checkHaltThreshold(ctx, ws.Pool, ws.State)
	}

	if batch.RegisteredWorkerCount >= 1 {
		batch.Status = BatchStatusStarted
		return a.batches.Save(ctx, batch)
	}

	if batch.RunningSince == nil && now.Sub(batch.SubmittedAt) > defaultMaxTimeInQueue {
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(ws.State,
			fmt.Sprintf("Batch job %s never left the queue within max_time_in_queue", batch.JobID),
			now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
		if err := a.saveState(ctx, ws.State); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
		return a.checkHaltThreshold(ctx, ws.Pool, ws.State)
	}

	return nil
}
