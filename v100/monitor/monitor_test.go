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
	pool.Status = WorkPoolStatusIdle
	pool.StatusMessage = "no tasks"
	pool.IncidentCount = 2
	w.Pools.Add(pool)

	err := w.A.RunJobSubmission(context.Background(), "pool-1")
	require.NoError(t, err)

	got := w.Pools.MustGet("pool-1")
	assert.Equal(t, WorkPoolStatusOK, got.Status)
	assert.Empty(t, got.StatusMessage)
	assert.Equal(t, 0, got.IncidentCount)
}

func TestJobSubmission_HaltedToOK(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.Status = WorkPoolStatusHalted
	pool.StatusMessage = "consecutive failures"
	pool.IncidentCount = 5
	w.Pools.Add(pool)

	err := w.A.RunJobSubmission(context.Background(), "pool-1")
	require.NoError(t, err)

	got := w.Pools.MustGet("pool-1")
	assert.Equal(t, WorkPoolStatusOK, got.Status)
	assert.Empty(t, got.StatusMessage)
	assert.Equal(t, 0, got.IncidentCount)
}

func TestJobSubmission_OKUnchanged(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.Status = WorkPoolStatusOK
	w.Pools.Add(pool)

	err := w.A.RunJobSubmission(context.Background(), "pool-1")
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusOK, w.Pools.MustGet("pool-1").Status)
}

func TestJobSubmission_UnhealthyUnchanged(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.Status = WorkPoolStatusUnhealthy
	w.Pools.Add(pool)

	err := w.A.RunJobSubmission(context.Background(), "pool-1")
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusUnhealthy, w.Pools.MustGet("pool-1").Status)
}

// ---- checkHaltThreshold ----

func TestHaltThreshold_NConsecutiveFailed_Halted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)

	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(2 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool)
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGet("pool-1").Status)
}

func TestHaltThreshold_NMinusOneFailures_NotHalted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 3
	w.Pools.Add(pool)

	// Only 2 failed, threshold is 3.
	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(2 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool)
	require.NoError(t, err)
	assert.NotEqual(t, WorkPoolStatusHalted, w.Pools.MustGet("pool-1").Status)
}

func TestHaltThreshold_PatternBrokenByStarted_NotHalted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)

	// Most recent batch is started (not failed) → streak broken.
	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusStarted, SubmittedAt: epoch.Add(2 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool)
	require.NoError(t, err)
	assert.NotEqual(t, WorkPoolStatusHalted, w.Pools.MustGet("pool-1").Status)
}

func TestHaltThreshold_PendingBatchesExcluded(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)

	// Two failed + one pending. The pending should not count.
	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(2 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b3", WorkpoolID: "pool-1", Status: BatchStatusPending, SubmittedAt: epoch.Add(3 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool)
	require.NoError(t, err)
	// 2 most recent classified are both failed → halt.
	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGet("pool-1").Status)
}

func TestHaltThreshold_AlreadyHaltedStaysHalted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.Status = WorkPoolStatusHalted
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)

	w.Batches.Add(&BatchAPIRequest{BatchID: "b1", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(1 * time.Minute)})
	w.Batches.Add(&BatchAPIRequest{BatchID: "b2", WorkpoolID: "pool-1", Status: BatchStatusFailed, SubmittedAt: epoch.Add(2 * time.Minute)})

	err := w.A.checkHaltThreshold(context.Background(), pool)
	require.NoError(t, err)
	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGet("pool-1").Status)
}

// ---- recordIncident helper ----

func TestRecordIncident_SetsUnhealthyAndCounts(t *testing.T) {
	pool := defaultPool("pool-1")
	pool.Status = WorkPoolStatusOK

	recordIncident(pool, "something went wrong", epoch)

	assert.Equal(t, WorkPoolStatusUnhealthy, pool.Status)
	assert.Equal(t, "something went wrong", pool.StatusMessage)
	assert.Equal(t, epoch, pool.LastIncidentAt)
	assert.Equal(t, 1, pool.IncidentCount)
}

func TestRecordIncident_DoesNotOverwriteHalted(t *testing.T) {
	pool := defaultPool("pool-1")
	pool.Status = WorkPoolStatusHalted

	recordIncident(pool, "another problem", epoch)

	// Status stays halted; message and count still updated.
	assert.Equal(t, WorkPoolStatusHalted, pool.Status)
	assert.Equal(t, "another problem", pool.StatusMessage)
}
