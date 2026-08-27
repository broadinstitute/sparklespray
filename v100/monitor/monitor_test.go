package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- idle→ok via runWorkPoolSummaryPoll + EventStore ----

func TestSummaryPoll_JobCreated_IdleToOK(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusIdle})
	// A job_created event is always accompanied by the task(s) it creates.
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusPending})
	w.Events.AddEvent("pool-1", epoch.Add(1*time.Minute))

	err := w.A.runWorkPoolSummaryPoll(context.Background())
	require.NoError(t, err)

	got := w.Pools.MustGetState("pool-1")
	assert.Equal(t, WorkPoolStatusOK, got.State)
}

func TestSummaryPoll_JobCreated_HaltedUnchanged(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusHalted})
	// Non-terminal (started) worker prevents the halted→idle transition so we can
	// test that a job_created event alone doesn't unblock a halted pool.
	w.Workers.Add(&Worker{WorkerID: "w1", WorkpoolID: "pool-1", Status: "started"})
	w.Events.AddEvent("pool-1", epoch.Add(1*time.Minute))

	err := w.A.runWorkPoolSummaryPoll(context.Background())
	require.NoError(t, err)

	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

func TestSummaryPoll_JobCreated_OKUnchanged(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusOK})
	// A job_created event is always accompanied by the task(s) it creates.
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusPending})
	w.Events.AddEvent("pool-1", epoch.Add(1*time.Minute))

	err := w.A.runWorkPoolSummaryPoll(context.Background())
	require.NoError(t, err)

	assert.Equal(t, WorkPoolStatusOK, w.Pools.MustGetState("pool-1").State)
}

func TestSummaryPoll_EventCursorAdvances(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusIdle})
	// A job_created event is always accompanied by the task(s) it creates.
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusPending})
	w.Events.AddEvent("pool-1", epoch.Add(1*time.Minute))

	// First poll: sees the event, transitions idle → ok.
	err := w.A.runWorkPoolSummaryPoll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusOK, w.Pools.MustGetState("pool-1").State)

	// Reset to idle manually to check the cursor.
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusIdle})

	// Second poll: event is before the cursor, so no transition.
	err = w.A.runWorkPoolSummaryPoll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusIdle, w.Pools.MustGetState("pool-1").State)
}

// ---- checkHaltThreshold ----

func TestHaltThreshold_NConsecutiveFailed_Halted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)
	state := defaultState("pool-1")
	w.Pools.AddState(state)

	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(1*time.Minute))
	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(2*time.Minute))

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

func TestHaltThreshold_NMinusOneFailures_NotHalted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 3
	w.Pools.Add(pool)
	state := defaultState("pool-1")
	w.Pools.AddState(state)

	// Only 2 failed, threshold is 3.
	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(1*time.Minute))
	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(2*time.Minute))

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	assert.NotEqual(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

func TestHaltThreshold_PatternBrokenBySucceeded_NotHalted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)
	state := defaultState("pool-1")
	w.Pools.AddState(state)

	// Most recent outcome is a success → streak broken.
	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(1*time.Minute))
	w.Events.AddBatchOutcome("pool-1", false, epoch.Add(2*time.Minute))

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	assert.NotEqual(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

func TestHaltThreshold_OutcomesOutsideWindowExcluded(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)
	state := defaultState("pool-1")
	w.Pools.AddState(state)

	// Two failures within the last hour, plus an older one outside the window.
	// The older one should not count, so we still only have 2 counted outcomes.
	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(-2*time.Hour))
	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(1*time.Minute))
	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(2*time.Minute))

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	// 2 most recent within-window outcomes are both failures → halt.
	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

func TestHaltThreshold_AlreadyHaltedStaysHalted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)
	state := &WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusHalted}
	w.Pools.AddState(state)

	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(1*time.Minute))
	w.Events.AddBatchOutcome("pool-1", true, epoch.Add(2*time.Minute))

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

// ---- recordIncident helper ----

// recordIncident is purely a log-to-Events operation now; it doesn't touch
// WorkPoolState at all. The ok/unhealthy transition is decided by the
// WorkPool summary poll instead — see TestSummaryPoll_RecentIncident_* below.
func TestRecordIncident_PublishesIncident(t *testing.T) {
	w := newWorld()

	w.A.recordIncident(context.Background(), "pool-1", "something went wrong")

	require.Len(t, w.Events.WorkpoolIncidents, 1)
	incident := w.Events.WorkpoolIncidents[0]
	assert.Equal(t, "pool-1", incident.WorkpoolID)
	assert.Equal(t, "something went wrong", incident.Message)
	assert.Equal(t, epoch, incident.Timestamp)
}

// TestIncident_FlowsThroughToWorkPoolSummary is an end-to-end trace: an
// incident recorded by the cluster reconciler (tier 2) is published as a
// workpool_incident event, and the next WorkPool summary poll derives
// StateMessage/LastIncidentAt/IncidentCount from that event rather than from
// any field persisted on WorkPoolState.
func TestIncident_FlowsThroughToWorkPoolSummary(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Workers.Add(&Worker{WorkerID: "w1", WorkpoolID: "pool-1", Status: "started"})
	w.Batches.Add(&BatchAPIRequest{
		BatchID:    "b1",
		JobID:      "job-1",
		WorkpoolID: "pool-1",
		Status:     BatchStatusStarted,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusFailed)

	ctx := context.Background()
	require.NoError(t, w.A.runClusterReconciler(ctx))

	// The cluster reconciler only logs the incident to the Events log — it no
	// longer flips WorkPoolState.State itself. That transition is owned by
	// the WorkPool summary poll.
	require.Len(t, w.Events.WorkpoolIncidents, 1)

	require.NoError(t, w.A.runWorkPoolSummaryPoll(ctx))

	var summary *WorkPoolSummary
	for _, s := range w.WorkPoolSummaries.Summaries {
		if s.WorkpoolID == "pool-1" {
			summary = s
		}
	}
	require.NotNil(t, summary)
	// The summary poll derives both the banner state and the
	// message/count/timestamp from the same recent-incidents query, so they
	// agree: a recent incident makes the workpool unhealthy, not just a
	// stale display field.
	assert.Equal(t, WorkPoolStatusUnhealthy, summary.State)
	assert.Equal(t, WorkPoolStatusUnhealthy, w.Pools.MustGetState("pool-1").State)
	assert.Equal(t, 1, summary.IncidentCount)
	assert.Equal(t, w.Events.WorkpoolIncidents[0].Message, summary.StateMessage)
	assert.Equal(t, w.Events.WorkpoolIncidents[0].Timestamp, summary.LastIncidentAt)
}
