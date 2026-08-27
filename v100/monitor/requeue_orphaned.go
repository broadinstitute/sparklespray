package monitor

import (
	"context"
	"fmt"
	"log"
)

// runRequeueOrphanedTasks is the task recovery poller. Runs every 30s with no GCP API calls.
// For every worker whose heartbeat has expired, it orphans any active tasks back to pending
// so they can be picked up by a healthy worker.
func (a *Monitor) runRequeueOrphanedTasks(ctx context.Context) error {
	now := a.clock.Now()

	expired, err := a.workers.ListExpired(ctx, now)
	if err != nil {
		return fmt.Errorf("list expired workers: %w", err)
	}

	for _, w := range expired {
		// A heartbeat expiring without a clean shutdown means the worker
		// crashed, was preempted, or otherwise stopped responding — distinct
		// from "stopped" (a clean shutdown), and worth recording as an
		// incident.
		if err := a.workers.MarkZombie(ctx, w.WorkerID); err != nil {
			log.Printf("task recovery: mark worker %s zombie: %v", w.WorkerID, err)
		}
		a.recordIncident(ctx, w.WorkpoolID, fmt.Sprintf("Worker %s stopped responding (heartbeat expired)", w.WorkerID))

		tasks, err := a.tasks.ListByWorker(ctx, w.WorkerID, activeTasks)
		if err != nil {
			// Log and continue so one bad worker doesn't block the rest.
			log.Printf("task recovery: list tasks for worker %s: %v", w.WorkerID, err)
			continue
		}

		for _, t := range tasks {
			if err := a.tasks.ResetToPending(ctx, t.TaskID, t.JobID, t.Status); err != nil {
				log.Printf("task recovery: reset task %s to pending: %v", t.TaskID, err)
			}
		}
	}
	return nil
}
