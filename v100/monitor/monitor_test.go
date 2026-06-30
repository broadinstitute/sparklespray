package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- RunJobSubmission ----

func TestJobSubmission_IdleToOK(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusIdle, StateMessage: "no tasks", IncidentCount: 2})

	err := w.A.RunJobSubmission(context.Background(), "pool-1")
	require.NoError(t, err)

	got := w.Pools.MustGetState("pool-1")
	assert.Equal(t, WorkPoolStatusOK, got.State)
	assert.Empty(t, got.StateMessage)
	assert.Equal(t, 0, got.IncidentCount)
}

func TestJobSubmission_HaltedToOK(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusHalted, StateMessage: "consecutive failures", IncidentCount: 5})

	err := w.A.RunJobSubmission(context.Background(), "pool-1")
	require.NoError(t, err)

	got := w.Pools.MustGetState("pool-1")
	assert.Equal(t, WorkPoolStatusOK, got.State)
	assert.Empty(t, got.StateMessage)
	assert.Equal(t, 0, got.IncidentCount)
}

func TestJobSubmission_OKUnchanged(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusOK})

	err := w.A.RunJobSubmission(context.Background(), "pool-1")
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusOK, w.Pools.MustGetState("pool-1").State)
}

func TestJobSubmission_UnhealthyUnchanged(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusUnhealthy})

	err := w.A.RunJobSubmission(context.Background(), "pool-1")
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusUnhealthy, w.Pools.MustGetState("pool-1").State)
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
