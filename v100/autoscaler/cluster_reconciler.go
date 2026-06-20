package autoscaler

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Runs on PubSub notifications for started batches
// or after max_time_between_polls. Makes GCP API calls for both VM list and Batch API status.
func (a *Autoscaler) runClusterReconciler(ctx context.Context) error {
	pools, err := a.pools.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list workpools: %w", err)
	}

	for _, pool := range pools {
		if err := a.reconcileWorkpool(ctx, pool); err != nil {
			log.Printf("tier2: workpool %s: %v", pool.WorkpoolID, err)
		}
	}
	return nil
}

func (a *Autoscaler) reconcileWorkpool(ctx context.Context, pool *WorkPool) error {
	now := a.clock.Now()

	activeBatches, err := a.batches.ListByWorkpool(ctx, pool.WorkpoolID, []BatchStatus{
		BatchStatusPending, BatchStatusStarted,
	})
	if err != nil {
		return fmt.Errorf("list active batches: %w", err)
	}

	for _, batch := range activeBatches {
		done, err := a.runTier2ForBatch(ctx, pool, batch, now)
		if err != nil {
			log.Printf("tier2: batch %s: %v", batch.BatchID, err)
		}
		if done {
			// batch was failed/completed; pool may have been updated — reload for next batch
			// so we don't overwrite a halted status with a stale copy.
			updated, err := a.pools.Get(ctx, pool.WorkpoolID)
			if err != nil {
				return fmt.Errorf("reload workpool after batch %s: %w", batch.BatchID, err)
			}
			pool = updated
		}
	}

	// Transition to idle if no VMs remain anywhere in the workpool.
	workpoolVMs, err := a.batchAPI.ListRunningVMs(ctx, "sparkles-worker-workpool", pool.WorkpoolID)
	if err != nil {
		return fmt.Errorf("list workpool VMs: %w", err)
	}
	if len(workpoolVMs) == 0 && (pool.Status == WorkPoolStatusOK || pool.Status == WorkPoolStatusUnhealthy) {
		pool.Status = WorkPoolStatusIdle
		if err := a.pools.Save(ctx, pool); err != nil {
			return fmt.Errorf("save pool (idle transition): %w", err)
		}
	}

	return nil
}

// runTier2ForBatch processes one batch. Returns (done=true) if the batch was terminated
// and the caller should reload the workpool before continuing.
func (a *Autoscaler) runTier2ForBatch(ctx context.Context, pool *WorkPool, batch *BatchAPIRequest, now time.Time) (done bool, err error) {
	apiStatus, err := a.batchAPI.GetJobStatus(ctx, batch.JobID)
	if err != nil {
		return false, fmt.Errorf("get job status: %w", err)
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
		if err := a.batchAPI.TerminateJob(ctx, batch.JobID); err != nil {
			log.Printf("tier2: terminate job %s: %v", batch.JobID, err)
		}
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(pool, fmt.Sprintf("Batch job %s reported failure by Batch API", batch.JobID), now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return true, fmt.Errorf("save batch: %w", err)
		}
		if err := a.pools.Save(ctx, pool); err != nil {
			return true, fmt.Errorf("save pool: %w", err)
		}
		return true, a.checkHaltThreshold(ctx, pool)
	}

	if apiStatus == BatchJobStatusSucceeded {
		batch.Status = BatchStatusCompleted
		return true, a.batches.Save(ctx, batch)
	}

	// Job is QUEUED/SCHEDULED/RUNNING — reconcile VMs against Firestore.
	return false, a.reconcileVMs(ctx, pool, batch, apiStatus, now)
}

func (a *Autoscaler) reconcileVMs(ctx context.Context, pool *WorkPool, batch *BatchAPIRequest, apiStatus BatchJobStatus, now time.Time) error {
	gcpVMs, err := a.batchAPI.ListRunningVMs(ctx, "sparkles-worker-batch", batch.BatchID)
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

	// Anomaly 1: more VMs than expected — serious bug, abort immediately.
	if len(gcpVMs) > batch.ExpectedVMCount {
		if err := a.batchAPI.TerminateJob(ctx, batch.JobID); err != nil {
			log.Printf("tier2: terminate job %s (over-provisioning): %v", batch.JobID, err)
		}
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(pool,
			fmt.Sprintf("Over-provisioning: %d VMs running, expected %d", len(gcpVMs), batch.ExpectedVMCount),
			now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
		if err := a.pools.Save(ctx, pool); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
		return a.checkHaltThreshold(ctx, pool)
	}

	// Anomaly 2: startup failure after the grace period.
	// Grace is measured from running_since (not submitted_at) so queued VMs aren't mistaken for failures.
	// Note: if apiStatus is SUCCEEDED here we already returned above; the check below is belt-and-suspenders.
	maxTimeToStart := param(pool.MaxTimeToStartWorker, defaultMaxTimeToStartWorker)
	if batch.RunningSince != nil && now.Sub(*batch.RunningSince) > maxTimeToStart && apiStatus != BatchJobStatusSucceeded {
		if batch.RegisteredWorkerCount == 0 {
			// No worker ever registered — whole batch is a startup failure.
			if err := a.batchAPI.TerminateJob(ctx, batch.JobID); err != nil {
				log.Printf("tier2: terminate job %s (no workers): %v", batch.JobID, err)
			}
			batch.Status = BatchStatusFailed
			batch.Unhealthy = true
			recordIncident(pool,
				fmt.Sprintf("Batch %s: no worker registered within grace period", batch.BatchID),
				now)
			if err := a.batches.Save(ctx, batch); err != nil {
				return fmt.Errorf("save batch: %w", err)
			}
			if err := a.pools.Save(ctx, pool); err != nil {
				return fmt.Errorf("save pool: %w", err)
			}
			return a.checkHaltThreshold(ctx, pool)
		}

		// Some workers registered; surgically terminate the VMs that never did.
		batchDirty, poolDirty := false, false
		for instanceName, vmInfo := range gcpVMs {
			if !registeredInstances[instanceName] {
				if err := a.batchAPI.TerminateVM(ctx, vmInfo.Zone, instanceName); err != nil {
					log.Printf("tier2: terminate VM %s: %v", instanceName, err)
				}
				batch.Unhealthy = true
				batchDirty = true
				recordIncident(pool, fmt.Sprintf("VM %s failed to start a worker", instanceName), now)
				poolDirty = true
			}
		}
		if batchDirty {
			if err := a.batches.Save(ctx, batch); err != nil {
				return fmt.Errorf("save batch: %w", err)
			}
		}
		if poolDirty {
			if err := a.pools.Save(ctx, pool); err != nil {
				return fmt.Errorf("save pool: %w", err)
			}
		}
	}

	// Anomaly 3: zombie workers — heartbeat expired but VM still running.
	// heartbeat_expiry is set to current time on both clean shutdown and crash, so
	// vm_shutdown_grace_period applies uniformly from that point.
	gracePeriod := param(pool.VMShutdownGracePeriod, defaultVMShutdownGracePeriod)
	zombieDeadline := now.Add(-gracePeriod)

	var zombies []*Worker
	for _, w := range workers {
		if w.HeartbeatExpiry.Before(zombieDeadline) {
			if _, running := gcpVMs[w.InstanceName]; running {
				zombies = append(zombies, w)
			}
		}
	}

	maxZombies := param(pool.MaxZombiesBeforeAbort, defaultMaxZombiesBeforeAbort)

	if len(zombies) > maxZombies {
		if err := a.batchAPI.TerminateJob(ctx, batch.JobID); err != nil {
			log.Printf("tier2: terminate job %s (too many zombies): %v", batch.JobID, err)
		}
		batch.Status = BatchStatusFailed
		batch.Unhealthy = true
		recordIncident(pool,
			fmt.Sprintf("Too many zombie workers (%d), aborting batch", len(zombies)),
			now)
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
		if err := a.pools.Save(ctx, pool); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
		return a.checkHaltThreshold(ctx, pool)
	}

	batchDirty, poolDirty := false, false
	for _, z := range zombies {
		if err := a.batchAPI.TerminateVM(ctx, gcpVMs[z.InstanceName].Zone, z.InstanceName); err != nil {
			log.Printf("tier2: terminate zombie VM %s: %v", z.InstanceName, err)
		}
		batch.Unhealthy = true
		batchDirty = true
		recordIncident(pool,
			fmt.Sprintf("Terminated zombie worker %s on %s", z.WorkerID, z.InstanceName),
			now)
		poolDirty = true
	}
	if batchDirty {
		if err := a.batches.Save(ctx, batch); err != nil {
			return fmt.Errorf("save batch: %w", err)
		}
	}
	if poolDirty {
		if err := a.pools.Save(ctx, pool); err != nil {
			return fmt.Errorf("save pool: %w", err)
		}
	}

	return nil
}
