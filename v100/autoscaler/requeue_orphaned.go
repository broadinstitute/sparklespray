package autoscaler

import (
	"context"
	"fmt"
	"log"
)

// runRequeueOrphanedTasks is the Task Recovery tier. Runs every 30s with no GCP API calls.
// For every worker whose heartbeat has expired, it orphans any active tasks back to pending
// so they can be picked up by a healthy worker.
func (a *Autoscaler) runRequeueOrphanedTasks(ctx context.Context) error {
	now := a.clock.Now()

	expired, err := a.workers.ListExpired(ctx, now)
	if err != nil {
		return fmt.Errorf("list expired workers: %w", err)
	}

	for _, w := range expired {
		tasks, err := a.tasks.ListByWorker(ctx, w.WorkerID, activeTasks)
		if err != nil {
			// Log and continue so one bad worker doesn't block the rest.
			log.Printf("tier1: list tasks for worker %s: %v", w.WorkerID, err)
			continue
		}

		for _, t := range tasks {
			if err := a.tasks.ResetToPending(ctx, t.TaskID); err != nil {
				log.Printf("tier1: reset task %s to pending: %v", t.TaskID, err)
			}
		}
	}
	return nil
}
