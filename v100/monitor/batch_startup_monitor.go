package monitor

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Runs on PubSub notifications for pending batches
// or after max_time_between_polls. Only processes pending batches; once promoted to started,
// failed, or completed, the cluster reconciler owns the batch.
func (a *Monitor) runBatchStartupMonitor(ctx context.Context) error {
	pools, err := a.pools.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list workpools: %w", err)
	}

	for _, ws := range pools {
		pendingBatches, err := a.batches.ListByWorkpool(ctx, ws.Pool.WorkpoolID, []BatchStatus{BatchStatusPending})
		if err != nil {
			log.Printf("batch startup monitor: list pending batches for workpool %s: %v", ws.Pool.WorkpoolID, err)
			continue
		}

		now := a.clock.Now()
		for _, batch := range pendingBatches {
			if err := a.checkBatchStartup(ctx, ws, batch, now); err != nil {
				log.Printf("batch startup monitor: batch %s: %v", batch.BatchID, err)
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
		log.Printf("batch startup monitor: batch %s: GCP job %s no longer exists (404); marking batch as deleted", batch.BatchID, batch.JobID)
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
		return a.markBatchFailed(ctx, ws, batch, fmt.Sprintf("Batch job %s failed before any workers started", batch.JobID), now)
	}

	if apiStatus == BatchJobStatusSucceeded && batch.RegisteredWorkerCount == 0 {
		return a.markBatchFailed(ctx, ws, batch, fmt.Sprintf("Batch job %s completed with no workers registered", batch.JobID), now)
	}

	if batch.RegisteredWorkerCount >= 1 {
		batch.Status = BatchStatusStarted
		if err := a.batches.Save(ctx, batch); err != nil {
			return err
		}
		// This branch only runs for batches still in BatchStatusPending (the
		// batch startup monitor only processes pending batches), so it fires
		// exactly once per batch — the first poll where a worker has registered.
		if a.batchOutcomes != nil {
			if err := a.batchOutcomes.PublishBatchSucceeded(ctx, ws.Pool.WorkpoolID); err != nil {
				log.Printf("checkBatchStartup: publish batch_succeeded for workpool %s: %v", ws.Pool.WorkpoolID, err)
			}
		}
		return nil
	}

	if batch.RunningSince == nil && now.Sub(batch.SubmittedAt) > defaultMaxTimeInQueue {
		return a.markBatchFailed(ctx, ws, batch, fmt.Sprintf("Batch job %s never left the queue within max_time_in_queue", batch.JobID), now)
	}

	return nil
}
