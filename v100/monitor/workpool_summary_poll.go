package monitor

import (
	"context"
	"log"
	"time"
)

// runWorkPoolSummaryPoll recomputes WorkPoolSummary for every workpool and
// writes both the current document and a history snapshot to Firestore.
// It also queries the Events collection for new job_created events since the
// last poll and transitions any idle workpool with a new job to ok.
func (a *Monitor) runWorkPoolSummaryPoll(ctx context.Context) error {
	// Collect workpool IDs that have received a new job since the last poll.
	// TODO: This could be reorganized to make more efficient queries by:
	// 1. fetch only "active" workpools (ie: only workpools which are not "idle")
	// 2. add to that any workpools which are referenced by new job events
	// That gives us a minimal list of pools which we can recompute from scratch
	poolsWithNewJob := make(map[string]bool)
	if a.events != nil {
		events, err := a.events.ListJobCreatedSince(ctx, a.lastEventTime)
		if err != nil {
			log.Printf("workpool summary poll: list job_created events: %v", err)
		} else {
			for _, e := range events {
				poolsWithNewJob[e.WorkpoolID] = true
				if e.Timestamp.After(a.lastEventTime) {
					a.lastEventTime = e.Timestamp
				}
			}
		}
	}

	pools, err := a.pools.ListAll(ctx)
	if err != nil {
		return err
	}
	for _, ws := range pools {
		if err := a.updateWorkPoolSummary(ctx, ws, poolsWithNewJob); err != nil {
			log.Printf("workpool summary poll: pool %s: %v", ws.Pool.WorkpoolID, err)
		}
	}
	return nil
}

func (a *Monitor) updateWorkPoolSummary(ctx context.Context, ws *WorkPoolWithState, poolsWithNewJob map[string]bool) error {
	pool := ws.Pool
	state := ws.State
	if state.State == "" {
		state.State = WorkPoolStatusIdle
	}

	// Idle → ok when a new job has arrived for this pool.
	if state.State == WorkPoolStatusIdle && poolsWithNewJob[pool.WorkpoolID] {
		state.State = WorkPoolStatusOK
		state.StateMessage = ""
		if err := a.saveState(ctx, state); err != nil {
			return err
		}
	}
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

	// Halted → idle when no non-terminal tasks remain, so the pool is ready
	// to accept new work without being stuck in halted forever.
	if state.State == WorkPoolStatusHalted {
		nonTerminal := 0
		for s, n := range taskCounts {
			if s != "success" && s != "error" && s != "failed" && s != "killed" {
				nonTerminal += n
			}
		}
		if nonTerminal == 0 {
			state.State = WorkPoolStatusIdle
			state.StateMessage = ""
			if err := a.saveState(ctx, state); err != nil {
				return err
			}
		}
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
