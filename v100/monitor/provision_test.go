package monitor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvision_NoPendingTasks_NoBatchCreated(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)
	assert.Empty(t, w.BatchAPI.CreatedJobs)
	assert.Equal(t, 0, w.Batches.Count())
}

func TestProvision_CopiesPoolLabelsOntoJobAndBatchRequest(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.Labels = []Label{{Name: "team", Value: "alice"}}
	pool.MaxPreemptibleWorkerAttempts = 0 // force non-preemptible for a single batch
	w.Pools.Add(pool)
	for i := 0; i < 3; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)

	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.Equal(t, []Label{{Name: "team", Value: "alice"}}, w.BatchAPI.CreatedJobs[0].Labels)

	require.Equal(t, 1, w.Batches.Count())
	batchID := w.BatchAPI.CreatedJobs[0].BatchID
	assert.Equal(t, []Label{{Name: "team", Value: "alice"}}, w.Batches.MustGet(batchID).Labels)
}

func TestProvision_CopiesPoolProjectIDOntoJobAndBatchRequest(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.ProjectID = "workload-project"
	pool.MaxPreemptibleWorkerAttempts = 0 // force non-preemptible for a single batch
	w.Pools.Add(pool)
	for i := 0; i < 3; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)

	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.Equal(t, "workload-project", w.BatchAPI.CreatedJobs[0].ProjectID)

	// Pinned on the batch record too, so reconciliation targets the project the
	// VMs are actually in even if the workpool spec is later overwritten.
	require.Equal(t, 1, w.Batches.Count())
	batchID := w.BatchAPI.CreatedJobs[0].BatchID
	assert.Equal(t, "workload-project", w.Batches.MustGet(batchID).ProjectID)
}

func TestProvision_NoPoolProjectID_LeavesProjectEmpty(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxPreemptibleWorkerAttempts = 0
	w.Pools.Add(pool)
	for i := 0; i < 3; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)

	// Empty means "the client's own project" — the pre-existing behavior.
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.Empty(t, w.BatchAPI.CreatedJobs[0].ProjectID)
	assert.Empty(t, w.Batches.MustGet(w.BatchAPI.CreatedJobs[0].BatchID).ProjectID)
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

	err := w.A.runProvisioningPoll(context.Background())
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

	err := w.A.runProvisioningPoll(context.Background())
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

	err := w.A.runProvisioningPoll(context.Background())
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

	err := w.A.runProvisioningPoll(context.Background())
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

	err := w.A.runProvisioningPoll(context.Background())
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
	// Simulate 10 zombie incidents (our proxy for preemption) recorded
	// within the last hour.
	for i := 0; i < 10; i++ {
		w.Events.AddWorkpoolIncident("pool-1", IncidentTypeZombie, "worker stopped responding", w.Clock.Now())
	}
	for i := 0; i < 20; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.False(t, w.BatchAPI.CreatedJobs[0].Preemptible)
}

func TestProvision_NoRecentZombies_TwoJobsBothGetFullPreemptibleBudget(t *testing.T) {
	// Regression test: max_preemptible_worker_attempts used to be enforced
	// as a lifetime total of preemptible VMs ever requested for the
	// workpool, so a second job submitted to a workpool that had already
	// used up its "budget" via a first job would be forced entirely
	// on-demand — even with no worker ever actually preempted. It's now a
	// rolling count of recent zombie incidents, so each job independently
	// gets its full preemptible allowance when nothing has gone zombie.
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxPreemptibleWorkerAttempts = 5
	w.Pools.Add(pool)

	// Job 1: 5 tasks.
	for i := 0; i < 5; i++ {
		w.Tasks.Add(&Task{TaskID: "job1-" + taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}
	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.True(t, w.BatchAPI.CreatedJobs[0].Preemptible)
	assert.Equal(t, 5, w.BatchAPI.CreatedJobs[0].VMCount)

	// Job 2: another 5 tasks submitted to the same workpool, no zombies
	// recorded in between — should still get a full preemptible batch.
	for i := 0; i < 5; i++ {
		w.Tasks.Add(&Task{TaskID: "job2-" + taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}
	err = w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 2)
	assert.True(t, w.BatchAPI.CreatedJobs[1].Preemptible)
	assert.Equal(t, 5, w.BatchAPI.CreatedJobs[1].VMCount)
}

func TestProvision_ZombiesOutsideWindow_FullBudgetAvailable(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxPreemptibleWorkerAttempts = 5
	w.Pools.Add(pool)
	// 5 zombies recorded more than an hour ago should not count against
	// the budget.
	for i := 0; i < 5; i++ {
		w.Events.AddWorkpoolIncident("pool-1", IncidentTypeZombie, "worker stopped responding", w.Clock.Now().Add(-2*time.Hour))
	}
	for i := 0; i < 5; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.True(t, w.BatchAPI.CreatedJobs[0].Preemptible)
	assert.Equal(t, 5, w.BatchAPI.CreatedJobs[0].VMCount)
}

func TestProvision_NonZombieIncidents_DoNotConsumePreemptibleBudget(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxPreemptibleWorkerAttempts = 5
	w.Pools.Add(pool)
	// Unrelated incidents (not zombies) recorded recently should not count
	// against the preemptible budget.
	for i := 0; i < 5; i++ {
		w.Events.AddWorkpoolIncident("pool-1", IncidentTypeOverProvisioned, "too many VMs running", w.Clock.Now())
	}
	for i := 0; i < 5; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)
	require.Len(t, w.BatchAPI.CreatedJobs, 1)
	assert.True(t, w.BatchAPI.CreatedJobs[0].Preemptible)
	assert.Equal(t, 5, w.BatchAPI.CreatedJobs[0].VMCount)
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

	err := w.A.runProvisioningPoll(context.Background())
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
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusHalted})
	for i := 0; i < 10; i++ {
		w.Tasks.Add(&Task{TaskID: taskID(i), WorkpoolID: "pool-1", Status: TaskStatusPending})
	}

	err := w.A.runProvisioningPoll(context.Background())
	require.NoError(t, err)
	assert.Empty(t, w.BatchAPI.CreatedJobs)
}

// ---- helpers ----

func taskID(n int) string  { return fmt.Sprintf("task-%d", n) }
func workerID(n int) string { return fmt.Sprintf("worker-%d", n) }
