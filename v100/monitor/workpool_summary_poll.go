package monitor

import (
	"context"
	"log"
	"time"
)

// runWorkPoolSummaryPoll recomputes WorkPoolSummary for every workpool and
// writes both the current document and a history snapshot to Firestore.
func (a *Monitor) runWorkPoolSummaryPoll(ctx context.Context) error {
	pools, err := a.pools.ListAll(ctx)
	if err != nil {
		return err
	}
	for _, ws := range pools {
		if err := a.updateWorkPoolSummary(ctx, ws); err != nil {
			log.Printf("workpool summary poll: pool %s: %v", ws.Pool.WorkpoolID, err)
		}
	}
	return nil
}

func (a *Monitor) updateWorkPoolSummary(ctx context.Context, ws *WorkPoolWithState) error {
	pool := ws.Pool
	state := ws.State
	now := time.Now()

	// Collect all batches and derive counts.
	batches, err := a.batches.ListAllByWorkpool(ctx, pool.WorkpoolID)
	if err != nil {
		return err
	}

	batchStateCounts := make(map[string]int)
	preemptibleBatchIDs := make(map[string]bool)
	var expectedPreemptible, expectedNonpreemptible, unhealthyCount int
	for _, b := range batches {
		batchStateCounts[string(b.Status)]++
		if b.Unhealthy {
			unhealthyCount++
		}
		active := b.Status == BatchStatusPending || b.Status == BatchStatusStarted
		if b.Preemptible {
			preemptibleBatchIDs[b.BatchID] = true
			if active {
				expectedPreemptible += b.ExpectedVMCount
			}
		} else if active {
			expectedNonpreemptible += b.ExpectedVMCount
		}
	}

	// Split workers into preemptible and non-preemptible using the batch map.
	workers, err := a.workers.ListAllForWorkpool(ctx, pool.WorkpoolID)
	if err != nil {
		return err
	}
	preemptibleWorkerCounts := make(map[string]int)
	nonpreemptibleWorkerCounts := make(map[string]int)
	for _, w := range workers {
		if preemptibleBatchIDs[w.BatchID] {
			preemptibleWorkerCounts[w.Status]++
		} else {
			nonpreemptibleWorkerCounts[w.Status]++
		}
	}

	// Task counts across all statuses for this workpool.
	taskCounts, err := a.tasks.CountByWorkpool(ctx, pool.WorkpoolID)
	if err != nil {
		return err
	}

	batchCounts := taskCountsFromMap(batchStateCounts)
	preemptibleWorkers := taskCountsFromMap(preemptibleWorkerCounts)
	nonpreemptibleWorkers := taskCountsFromMap(nonpreemptibleWorkerCounts)
	tasks := taskCountsFromMap(taskCounts)

	summary := &WorkPoolSummary{
		WorkpoolID:                    pool.WorkpoolID,
		MachineType:                   pool.MachineType,
		MaxPreemptibleWorkerAttempts:  pool.MaxPreemptibleWorkerAttempts,
		Expiry:                        now.Add(7 * 24 * time.Hour),
		LastUpdated:                   now,
		State:                         state.State,
		StateMessage:                  state.StateMessage,
		LastIncidentAt:                state.LastIncidentAt,
		IncidentCount:                 state.IncidentCount,
		ExpectedPreemptibleWorkers:    expectedPreemptible,
		ExpectedNonpreemptibleWorkers: expectedNonpreemptible,
		UnhealthyBatchCount:           unhealthyCount,
		BatchAPIRequestCounts:         batchCounts,
		PreemptibleWorkers:            preemptibleWorkers,
		NonpreemptibleWorkers:         nonpreemptibleWorkers,
		Tasks:                         tasks,
	}
	if err := a.workPoolSummaries.Save(ctx, summary); err != nil {
		return err
	}

	history := &WorkPoolSummaryHistory{
		WorkpoolID:                    pool.WorkpoolID,
		Timestamp:                     now,
		Expiry:                        now.Add(7 * 24 * time.Hour),
		State:                         state.State,
		StateMessage:                  state.StateMessage,
		LastIncidentAt:                state.LastIncidentAt,
		IncidentCount:                 state.IncidentCount,
		ExpectedPreemptibleWorkers:    expectedPreemptible,
		ExpectedNonpreemptibleWorkers: expectedNonpreemptible,
		UnhealthyBatchCount:           unhealthyCount,
		BatchAPIRequestCounts:         batchCounts,
		PreemptibleWorkers:            preemptibleWorkers,
		NonpreemptibleWorkers:         nonpreemptibleWorkers,
		Tasks:                         tasks,
	}
	return a.workPoolSummaries.SaveHistory(ctx, history)
}

// RunWorkPoolSummaryPoll runs one workpool-summary pass. Exported for functional tests.
func (a *Monitor) RunWorkPoolSummaryPoll(ctx context.Context) error {
	return a.runWorkPoolSummaryPoll(ctx)
}
