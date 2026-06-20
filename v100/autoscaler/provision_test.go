package autoscaler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvision_NoPendingTasks_NoBatchCreated(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	assert.Empty(t, w.BatchAPI.CreatedJobs)
	assert.Equal(t, 0, w.Batches.Count())
}

func TestProvision_DemandMetByRequestedVMs_NoBatchCreated(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	// 5 pending tasks, a started batch covering all 5 — workers still starting up.
	for i := 0; i < 5; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}
	w.Batches.Add(&BatchAPIRequest{
		BatchID:         "existing-batch",
		WorkpoolID:      "pool-1",
		ExpectedVMCount: 5,
		Status:          BatchStatusStarted,
	})

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	assert.Empty(t, w.BatchAPI.CreatedJobs)
}

func TestProvision_CreatesBatchForDemand(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	// 0 preemptible budget → all non-preemptible
	pool.MaxPreemptibleWorkerAttempts = 0
	w.Pools.Add(pool)
	for i := 0; i < 10; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.Equal(t, 10, w.BatchAPI.CreatedJobs[0].VMCount)
	assert.False(t, w.BatchAPI.CreatedJobs[0].Preemptible)
	assert.Equal(t, 1, w.Batches.Count())
}

func TestProvision_CapsAtMaxWorkerCount(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxWorkerCount = 50
	pool.MaxPreemptibleWorkerAttempts = 0
	w.Pools.Add(pool)
	for i := 0; i < 200; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.Equal(t, 50, w.BatchAPI.CreatedJobs[0].VMCount)
}

func TestProvision_CapsAtMaxWorkersPerRequest(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxWorkerCount = 200
	pool.MaxWorkersPerRequest = 100
	pool.MaxPreemptibleWorkerAttempts = 0
	w.Pools.Add(pool)
	for i := 0; i < 200; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.Equal(t, 100, w.BatchAPI.CreatedJobs[0].VMCount)
}

func TestProvision_FullPreemptibleBudget_AllPreemptible(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxPreemptibleWorkerAttempts = 200
	w.Pools.Add(pool)
	for i := 0; i < 20; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.Equal(t, 20, w.BatchAPI.CreatedJobs[0].VMCount)
	assert.True(t, w.BatchAPI.CreatedJobs[0].Preemptible)
}

func TestProvision_ExhaustedPreemptibleBudget_AllNonPreemptible(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxPreemptibleWorkerAttempts = 10
	w.Pools.Add(pool)
	// Simulate 10 preemptible VMs already submitted in a previous batch.
	w.Batches.Add(&BatchAPIRequest{
		BatchID:         "prior-batch",
		WorkpoolID:      "pool-1",
		Preemptible:     true,
		ExpectedVMCount: 10,
		Status:          BatchStatusStarted,
	})
	for i := 0; i < 20; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.False(t, w.BatchAPI.CreatedJobs[0].Preemptible)
}

func TestProvision_PartialPreemptibleBudget_SplitBatches(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxPreemptibleWorkerAttempts = 5
	pool.MaxWorkersPerRequest = 100
	w.Pools.Add(pool)
	for i := 0; i < 20; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	// Expect two batches: 5 preemptible + 15 non-preemptible
	require.Len(t, w.BatchAPI.CreatedJobs, 2)
	var preemptibleCount, nonPreemptibleCount int
	for _, j := range w.BatchAPI.CreatedJobs {
		if j.Preemptible {
			preemptibleCount += j.VMCount
		} else {
			nonPreemptibleCount += j.VMCount
		}
	}
	assert.Equal(t, 5, preemptibleCount)
	assert.Equal(t, 15, nonPreemptibleCount)
}

func TestProvision_HaltedWorkpool_NoProvisioning(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.Status = WorkPoolStatusHalted
	w.Pools.Add(pool)
	for i := 0; i < 10; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	assert.Empty(t, w.BatchAPI.CreatedJobs)
}

func TestProvision_IdleToOkTransitionOnFirstBatch(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.Status = WorkPoolStatusIdle
	pool.MaxPreemptibleWorkerAttempts = 0
	w.Pools.Add(pool)
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusPending})

	err := w.A.runAutoscalerPoll(context.Background())
	require.NoError(t, err)
	assert.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.Equal(t, WorkPoolStatusOK, w.Pools.MustGet("pool-1").Status)
}

// ---- helpers ----

func taskID(n int) string  { return fmt.Sprintf("task-%d", n) }
func workerID(n int) string { return fmt.Sprintf("worker-%d", n) }
