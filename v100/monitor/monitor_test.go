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
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusIdle, StateMessage: "no tasks"})
	// A job_created event is always accompanied by the task(s) it creates.
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusPending})
	w.Events.AddEvent("pool-1", epoch.Add(1*time.Minute))

	err := w.A.runWorkPoolSummaryPoll(context.Background())
	require.NoError(t, err)

	got := w.Pools.MustGetState("pool-1")
	assert.Equal(t, WorkPoolStatusOK, got.State)
	assert.Empty(t, got.StateMessage)
}

func TestSummaryPoll_JobCreated_HaltedUnchanged(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusHalted, StateMessage: "consecutive failures"})
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

	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(2 * time.Minute)})

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
	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(2 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	assert.NotEqual(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

func TestHaltThreshold_PatternBrokenByStarted_NotHalted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)
	state := defaultState("pool-1")
	w.Pools.AddState(state)

	// Most recent batch is started (not failed) → streak broken.
	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusStarted, SubmittedAt: epoch.Add(2 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	assert.NotEqual(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

func TestHaltThreshold_PendingBatchesExcluded(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)
	state := defaultState("pool-1")
	w.Pools.AddState(state)

	// Two failed + one pending. The pending should not count.
	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(2 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b3", WorkpoolID: "pool-1", Status: BatchStatusPending, SubmittedAt: epoch.Add(3 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	// 2 most recent classified are both failed → halt.
	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

func TestHaltThreshold_AlreadyHaltedStaysHalted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)
	state := &WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusHalted}
	w.Pools.AddState(state)

	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(2 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool, state)
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

// ---- recordIncident helper ----

func TestRecordIncident_SetsUnhealthyAndCounts(t *testing.T) {
	state := defaultState("pool-1")

	recordIncident(state, "something went wrong", epoch)

	assert.Equal(t, WorkPoolStatusUnhealthy, state.State)
	assert.Equal(t, "something went wrong", state.StateMessage)
	assert.Equal(t, epoch, state.LastIncidentAt)
	assert.Equal(t, 1, state.IncidentCount)
}

func TestRecordIncident_DoesNotOverwriteHalted(t *testing.T) {
	state := &WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusHalted}

	recordIncident(state, "another problem", epoch)

	// Status stays halted; message and count still updated.
	assert.Equal(t, WorkPoolStatusHalted, state.State)
	assert.Equal(t, "another problem", state.StateMessage)
}
