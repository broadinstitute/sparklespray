package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Batch Startup Monitor ----

func TestBatchStartupMonitor_RunningJob_StampsRunningSince(t *testing.T) {
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

func TestBatchStartupMonitor_RunningSinceNotOverwritten(t *testing.T) {
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

func TestBatchStartupMonitor_FailedJob_MarksFailedAndUnhealthy(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	// A pending task means the workpool isn't idle — only the summarizer
	// (not the batch startup monitor) transitions workpool state now.
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusPending})

	w.Batches.Add(&BatchAPIRequest{
		BatchID:     "b1",
		JobID:       "job-1",
		WorkpoolID:  "pool-1",
		Status:      BatchStatusPending,
		SubmittedAt: epoch,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusFailed)

	ctx := context.Background()
	err := w.A.runBatchStartupMonitor(ctx)
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
	assert.True(t, b.Unhealthy)
	// The batch startup monitor only logs the incident to the Events log
	// now; the WorkPool summary poll is what decides the unhealthy transition.
	assert.NotEmpty(t, w.Events.WorkpoolIncidents)

	require.NoError(t, w.A.runWorkPoolSummaryPoll(ctx))
	assert.Equal(t, WorkPoolStatusUnhealthy, w.Pools.MustGetState("pool-1").State)
}

func TestBatchStartupMonitor_SucceededWithNoWorkers_MarksFailedAndUnhealthy(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusPending})

	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		Status:                BatchStatusPending,
		RegisteredWorkerCount: 0,
		SubmittedAt:           epoch,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusSucceeded)

	ctx := context.Background()
	err := w.A.runBatchStartupMonitor(ctx)
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
	assert.True(t, b.Unhealthy)
	assert.NotEmpty(t, w.Events.WorkpoolIncidents)

	require.NoError(t, w.A.runWorkPoolSummaryPoll(ctx))
	assert.Equal(t, WorkPoolStatusUnhealthy, w.Pools.MustGetState("pool-1").State)
}

func TestBatchStartupMonitor_WorkerRegistered_PromotesToStarted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	runningSince := epoch
	w.Batches.Add(&BatchAPIRequest{
		BatchID:      "b1",
		JobID:        "job-1",
		WorkpoolID:   "pool-1",
		Status:       BatchStatusPending,
		SubmittedAt:  epoch,
		RunningSince: &runningSince,
	})
	// At least one worker registered.
	w.Workers.Add(&Worker{WorkerID: "w1", WorkpoolID: "pool-1", BatchID: "b1", Status: "started"})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusRunning)

	err := w.A.runBatchStartupMonitor(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusStarted, b.Status)
}

func TestBatchStartupMonitor_StuckInQueue_MarksFailedAfterMaxTimeInQueue(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
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

func TestBatchStartupMonitor_NotYetPastMaxTimeInQueue_NoAction(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
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
