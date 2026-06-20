package autoscaler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Tier 3: Batch Startup Monitor ----

func TestTier3_RunningJob_StampsRunningSince(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	w.Batches.Add(&BatchAPIRequest{
		BatchID:      "b1",
		JobID:        "job-1",
		WorkpoolID:   "pool-1",
		Status:       BatchStatusPending,
		SubmittedAt:  epoch,
		RunningSince: nil,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusRunning)

	err := w.A.runBatchStartupMonitor(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	require.NotNil(t, b.RunningSince)
	assert.Equal(t, epoch, *b.RunningSince)
}

func TestTier3_RunningSinceNotOverwritten(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	original := epoch.Add(-3 * time.Minute)
	w.Batches.Add(&BatchAPIRequest{
		BatchID:      "b1",
		JobID:        "job-1",
		WorkpoolID:   "pool-1",
		Status:       BatchStatusPending,
		SubmittedAt:  epoch.Add(-5 * time.Minute),
		RunningSince: &original,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusRunning)

	err := w.A.runBatchStartupMonitor(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	require.NotNil(t, b.RunningSince)
	assert.Equal(t, original, *b.RunningSince)
}

func TestTier3_FailedJob_MarksFailedAndUnhealthy(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	w.Batches.Add(&BatchAPIRequest{
		BatchID:     "b1",
		JobID:       "job-1",
		WorkpoolID:  "pool-1",
		Status:      BatchStatusPending,
		SubmittedAt: epoch,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusFailed)

	err := w.A.runBatchStartupMonitor(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
	assert.True(t, b.Unhealthy)
	assert.Equal(t, WorkPoolStatusUnhealthy, w.Pools.MustGet("pool-1").Status)
}

func TestTier3_SucceededWithNoWorkers_MarksFailedAndUnhealthy(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		Status:                BatchStatusPending,
		RegisteredWorkerCount: 0,
		SubmittedAt:           epoch,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusSucceeded)

	err := w.A.runBatchStartupMonitor(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
	assert.True(t, b.Unhealthy)
	assert.Equal(t, WorkPoolStatusUnhealthy, w.Pools.MustGet("pool-1").Status)
}

func TestTier3_WorkerRegistered_PromotesToStarted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	runningSince := epoch
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		Status:                BatchStatusPending,
		RegisteredWorkerCount: 1, // at least one worker registered
		SubmittedAt:           epoch,
		RunningSince:          &runningSince,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusRunning)

	err := w.A.runBatchStartupMonitor(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusStarted, b.Status)
}

func TestTier3_StuckInQueue_MarksFailedAfterMaxTimeInQueue(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxTimeInQueue = 15 * time.Minute
	w.Pools.Add(pool)

	// Submitted 20 min ago, never reached RUNNING.
	w.Batches.Add(&BatchAPIRequest{
		BatchID:      "b1",
		JobID:        "job-1",
		WorkpoolID:   "pool-1",
		Status:       BatchStatusPending,
		SubmittedAt:  epoch.Add(-20 * time.Minute),
		RunningSince: nil,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusQueued)

	err := w.A.runBatchStartupMonitor(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
	assert.True(t, b.Unhealthy)
}

func TestTier3_NotYetPastMaxTimeInQueue_NoAction(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxTimeInQueue = 15 * time.Minute
	w.Pools.Add(pool)

	// Submitted only 5 min ago — well inside the queue deadline.
	w.Batches.Add(&BatchAPIRequest{
		BatchID:      "b1",
		JobID:        "job-1",
		WorkpoolID:   "pool-1",
		Status:       BatchStatusPending,
		SubmittedAt:  epoch.Add(-5 * time.Minute),
		RunningSince: nil,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusQueued)

	err := w.A.runBatchStartupMonitor(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusPending, b.Status)
}
