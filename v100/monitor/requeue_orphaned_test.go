package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTier1_ExpiredWorkerTasksReset(t *testing.T) {
	w := newWorld()

	w.Workers.Add(&Worker{
		WorkerID:        "w1",
		WorkpoolID:      "pool-1",
		HeartbeatExpiry: epoch.Add(-1 * time.Second), // already expired
	})
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusClaimed, OwningWorkerID: "w1"})
	w.Tasks.Add(&Task{TaskID: "t2", WorkpoolID: "pool-1", Status: TaskStatusRunning, OwningWorkerID: "w1"})
	w.Tasks.Add(&Task{TaskID: "t3", WorkpoolID: "pool-1", Status: TaskStatusWriting, OwningWorkerID: "w1"})

	err := w.A.runRequeueOrphanedTasks(context.Background())
	require.NoError(t, err)

	for _, id := range []string{"t1", "t2", "t3"} {
		assert.Equal(t, TaskStatusPending, w.Tasks.MustGet(id).Status, "task %s should be pending", id)
		assert.Empty(t, w.Tasks.MustGet(id).OwningWorkerID, "task %s should have no owner", id)
	}
}

func TestTier1_ExpiredWorkerNoTasks_NoChange(t *testing.T) {
	w := newWorld()

	w.Workers.Add(&Worker{
		WorkerID:        "w1",
		WorkpoolID:      "pool-1",
		HeartbeatExpiry: epoch.Add(-1 * time.Second),
	})

	err := w.A.runRequeueOrphanedTasks(context.Background())
	require.NoError(t, err)
	// Nothing to assert — just no crash.
}

func TestTier1_UnexpiredWorker_NoChange(t *testing.T) {
	w := newWorld()

	w.Workers.Add(&Worker{
		WorkerID:        "w1",
		WorkpoolID:      "pool-1",
		HeartbeatExpiry: epoch.Add(10 * time.Minute), // well in the future
	})
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusRunning, OwningWorkerID: "w1"})

	err := w.A.runRequeueOrphanedTasks(context.Background())
	require.NoError(t, err)
	assert.Equal(t, TaskStatusRunning, w.Tasks.MustGet("t1").Status)
}

func TestTier1_MixedWorkers_OnlyExpiredWorkerAffected(t *testing.T) {
	w := newWorld()

	// Expired worker with a task.
	w.Workers.Add(&Worker{
		WorkerID:        "expired",
		WorkpoolID:      "pool-1",
		HeartbeatExpiry: epoch.Add(-1 * time.Second),
	})
	w.Tasks.Add(&Task{TaskID: "t-expired", WorkpoolID: "pool-1", Status: TaskStatusRunning, OwningWorkerID: "expired"})

	// Healthy worker with a task.
	w.Workers.Add(&Worker{
		WorkerID:        "healthy",
		WorkpoolID:      "pool-1",
		HeartbeatExpiry: epoch.Add(10 * time.Minute),
	})
	w.Tasks.Add(&Task{TaskID: "t-healthy", WorkpoolID: "pool-1", Status: TaskStatusRunning, OwningWorkerID: "healthy"})

	err := w.A.runRequeueOrphanedTasks(context.Background())
	require.NoError(t, err)

	assert.Equal(t, TaskStatusPending, w.Tasks.MustGet("t-expired").Status)
	assert.Equal(t, TaskStatusRunning, w.Tasks.MustGet("t-healthy").Status)
}

func TestTier1_PendingTaskNotReset(t *testing.T) {
	w := newWorld()

	// Expired worker, but its task is already pending (no owning worker).
	w.Workers.Add(&Worker{
		WorkerID:        "w1",
		WorkpoolID:      "pool-1",
		HeartbeatExpiry: epoch.Add(-1 * time.Second),
	})
	// A pending task not owned by the expired worker.
	w.Tasks.Add(&Task{TaskID: "t1", WorkpoolID: "pool-1", Status: TaskStatusPending, OwningWorkerID: ""})

	err := w.A.runRequeueOrphanedTasks(context.Background())
	require.NoError(t, err)
	// t1 has no owner, so it won't be returned by ListByWorker and won't be touched.
	assert.Equal(t, TaskStatusPending, w.Tasks.MustGet("t1").Status)
}
