package monitor

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Runs on PubSub notifications for started batches
// or after max_time_between_polls. Makes GCP API calls for both VM list and Batch API status.
func (a *Monitor) runClusterReconciler(ctx context.Context) error {
	pools, err := a.pools.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list workpools: %w", err)
	}

	for _, ws := range pools {
		if err := a.reconcileWorkpool(ctx, ws); err != nil {
			log.Printf("cluster reconciler: workpool %s: %v", ws.Pool.WorkpoolID, err)
		}
	}
	return nil
}

func (a *Monitor) reconcileWorkpool(ctx context.Context, ws *WorkPoolWithState) error {
	now := a.clock.Now()

	activeBatches, err := a.batches.ListByWorkpool(ctx, ws.Pool.WorkpoolID, []BatchStatus{
		BatchStatusPending, BatchStatusStarted,
	})
	if err != nil {
		return fmt.Errorf("list active batches: %w", err)
	}

	for _, batch := range activeBatches {
		done, err := a.reconcileBatch(ctx, ws, batch, now)
		if err != nil {
			log.Printf("cluster reconciler: batch %s: %v", batch.BatchID, err)
		}
		if done {
			// batch was failed/terminated/completed; reload pool state so we don't overwrite a halted status.
			ws, err = a.pools.Get(ctx, ws.Pool.WorkpoolID)
			if err != nil {
				return fmt.Errorf("reload workpool after batch %s: %w", batch.BatchID, err)
			}
		}
	}

	return nil
}

// reconcileBatch processes one batch. Returns (done=true) if the batch was terminated
// and the caller should reload the workpool before continuing.
func (a *Monitor) reconcileBatch(ctx context.Context, ws *WorkPoolWithState, batch *BatchAPIRequest, now time.Time) (done bool, err error) {
	apiStatus, err := a.batchAPI.GetJobStatus(ctx, batch.JobID)
	if err != nil {
		return false, fmt.Errorf("get job status: %w", err)
	}

	if apiStatus == BatchJobStatusDeleted {
		log.Printf("cluster reconciler: batch %s: GCP job %s no longer exists (404); marking batch as deleted", batch.BatchID, batch.JobID)
		batch.Status = BatchStatusDeleted
		return true, a.batches.Save(ctx, batch)
	}

	// Stamp running_since the first time we observe RUNNING.
	if apiStatus == BatchJobStatusRunning && batch.RunningSince == nil {
		t := now
		batch.RunningSince = &t
		if err := a.batches.Save(ctx, batch); err != nil {
			return false, fmt.Errorf("save batch (running_since): %w", err)
		}
	}

	if apiStatus == BatchJobStatusFailed {
		if err := a.batchAPI.PrintBatchDebuggingInfo(ctx, batch.JobID); err != nil {
			log.Printf("cluster reconciler: print batch debugging info for %s: %v", batch.JobID, err)
		}
		if err := a.batchAPI.TerminateJob(ctx, batch.JobID); err != nil {
			log.Printf("cluster reconciler: terminate job %s: %v", batch.JobID, err)
		}
		return true, a.markBatchFailed(ctx, ws, batch, fmt.Sprintf("Batch job %s reported failure by Batch API", batch.JobID), now)
	}

	if apiStatus == BatchJobStatusSucceeded {
		batch.Status = BatchStatusCompleted
		return true, a.batches.Save(ctx, batch)
	}

	// Job is QUEUED/SCHEDULED/RUNNING — reconcile VMs against Firestore.
	return false, a.reconcileVMs(ctx, ws, batch, apiStatus, now)
}

func (a *Monitor) reconcileVMs(ctx context.Context, ws *WorkPoolWithState, batch *BatchAPIRequest, apiStatus BatchJobStatus, now time.Time) error {
	gcpVMs, err := a.batchAPI.ListRunningVMs(ctx, "sparkles-worker-batch", batch.BatchID, ws.Pool.Zones)
	if err != nil {
		return fmt.Errorf("list running VMs: %w", err)
	}

	workers, err := a.workers.ListByBatch(ctx, batch.BatchID)
	if err != nil {
		return fmt.Errorf("list workers for batch: %w", err)
	}

	registeredInstances := make(map[string]bool, len(workers))
	for _, w := range workers {
		registeredInstances[w.InstanceName] = true
	}
	// RegisteredWorkerCount is derived from actual Worker records — it's the
	// monitor's job to compute and persist it, not the worker's.
	batch.RegisteredWorkerCount = len(workers)

	// Anomaly 1: more VMs than expected — serious bug, abort immediately.
	if len(gcpVMs) > batch.ExpectedVMCount {
		if err := a.batchAPI.TerminateJob(ctx, batch.JobID); err != nil {
			log.Printf("cluster reconciler: terminate job %s (over-provisioning): %v", batch.JobID, err)
		}
		return a.markBatchTerminated(ctx, ws, batch,
			fmt.Sprintf("Over-provisioning: %d VMs running, expected %d", len(gcpVMs), batch.ExpectedVMCount),
			now)
	}

	// Anomaly 2: startup failure after the grace period.
	// Grace is measured from running_since (not submitted_at) so queued VMs aren't mistaken for failures.
	// Note: if apiStatus is SUCCEEDED here we already returned above; the check below is belt-and-suspenders.
	if batch.RunningSince != nil && now.Sub(*batch.RunningSince) > defaultMaxTimeToStartWorker && apiStatus != BatchJobStatusSucceeded {
		if len(workers) == 0 {
			// No worker ever registered — whole batch is a startup failure.
			if err := a.batchAPI.TerminateJob(ctx, batch.JobID); err != nil {
				log.Printf("cluster reconciler: terminate job %s (no workers): %v", batch.JobID, err)
			}
			return a.markBatchTerminated(ctx, ws, batch,
				fmt.Sprintf("Batch %s: no worker registered within grace period", batch.BatchID),
				now)
		}

		// Some workers registered; surgically terminate the VMs that never did.
		batchDirty := false
		for instanceName, vmInfo := range gcpVMs {
			if !registeredInstances[instanceName] {
				if err := a.batchAPI.TerminateVM(ctx, vmInfo.Zone, instanceName); err != nil {
					log.Printf("cluster reconciler: terminate VM %s: %v", instanceName, err)
				}
				batch.Unhealthy = true
				batchDirty = true
				a.recordIncident(ctx, ws.Pool.WorkpoolID, fmt.Sprintf("VM %s failed to start a worker", instanceName))
			}
		}
		if batchDirty {
			if err := a.batches.Save(ctx, batch); err != nil {
				return fmt.Errorf("save batch: %w", err)
			}
		}
	}

	// Anomaly 3: zombie workers — heartbeat expired but VM still running.
	// heartbeat_expiry is set to current time on both clean shutdown and crash, so
	// vm_shutdown_grace_period applies uniformly from that point.
	gracePeriod := param(ws.Pool.VMShutdownGracePeriod, defaultVMShutdownGracePeriod)
	zombieDeadline := now.Add(-gracePeriod)

	var zombies []*Worker
	for _, w := range workers {
		if w.HeartbeatExpiry.Before(zombieDeadline) {
			if _, running := gcpVMs[w.InstanceName]; running {
				zombies = append(zombies, w)
			}
		}
	}

	maxZombies := param(ws.Pool.MaxZombiesBeforeAbort, defaultMaxZombiesBeforeAbort)

	if len(zombies) > maxZombies {
		if err := a.batchAPI.TerminateJob(ctx, batch.JobID); err != nil {
			log.Printf("cluster reconciler: terminate job %s (too many zombies): %v", batch.JobID, err)
		}
		return a.markBatchTerminated(ctx, ws, batch,
			fmt.Sprintf("Too many zombie workers (%d), aborting batch", len(zombies)),
			now)
	}

	batchDirty := false
	for _, z := range zombies {
		if err := a.batchAPI.TerminateVM(ctx, gcpVMs[z.InstanceName].Zone, z.InstanceName); err != nil {
			log.Printf("cluster reconciler: terminate zombie VM %s: %v", z.InstanceName, err)
		}
		batch.Unhealthy = true
		batchDirty = true
		a.recordIncident(ctx, ws.Pool.WorkpoolID, fmt.Sprintf("Terminated zombie worker %s on %s", z.WorkerID, z.InstanceName))
	}
	if batchDirty {
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
	}

	return nil
}
