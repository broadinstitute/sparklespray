package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Batch API lifecycle ----

func TestTier2_FailedBatch_MarksFailedAndUnhealthy(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	w.Batches.Add(&BatchAPIRequest{
		BatchID:    "b1",
		JobID:      "job-1",
		WorkpoolID: "pool-1",
		Status:     BatchStatusStarted,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusFailed)

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
	assert.True(t, b.Unhealthy)
	assert.Contains(t, w.BatchAPI.TerminatedJobs, "job-1")
	ps := w.Pools.MustGetState("pool-1")
	// After termination VMs are gone → idle transition fires on top of unhealthy.
	assert.Equal(t, WorkPoolStatusIdle, ps.State)
	// IncidentCount proves recordIncident was called even though status is now idle.
	assert.Greater(t, ps.IncidentCount, 0)
}

func TestTier2_SucceededBatch_MarksCompleted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)
	w.Pools.AddState(&WorkPoolState{WorkpoolID: "pool-1", State: WorkPoolStatusOK})

	w.Batches.Add(&BatchAPIRequest{
		BatchID:    "b1",
		JobID:      "job-1",
		WorkpoolID: "pool-1",
		Status:     BatchStatusStarted,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusSucceeded)

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusCompleted, b.Status)
	// Succeeded job clears VMs → idle transition fires.
	assert.Equal(t, WorkPoolStatusIdle, w.Pools.MustGetState("pool-1").State)
}

func TestTier2_FailedBatchTriggersHaltThreshold(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.MaxConsecutiveFailedBatches = 2
	w.Pools.Add(pool)

	// Two prior failed batches.
	for i, id := range []string{"b0", "b1"} {
		w.Batches.Add(&BatchAPIRequest{
			BatchID:     id,
			JobID:       "job-" + id,
			WorkpoolID:  "pool-1",
			Status:      BatchStatusFailed,
			SubmittedAt: epoch.Add(time.Duration(i) * time.Minute),
		})
		w.BatchAPI.AddJob("job-"+id, id, "pool-1", 1, BatchJobStatusFailed)
	}
	// Third batch now failing → should halt.
	w.Batches.Add(&BatchAPIRequest{
		BatchID:     "b2",
		JobID:       "job-b2",
		WorkpoolID:  "pool-1",
		Status:      BatchStatusStarted,
		SubmittedAt: epoch.Add(2 * time.Minute),
	})
	w.BatchAPI.AddJob("job-b2", "b2", "pool-1", 1, BatchJobStatusFailed)

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	assert.Equal(t, WorkPoolStatusHalted, w.Pools.MustGetState("pool-1").State)
}

// ---- Anomaly 1: over-provisioning ----

func TestTier2_Anomaly1_MoreVMsThanExpected_AbortBatch(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	runningSince := epoch
	w.Batches.Add(&BatchAPIRequest{
		BatchID:         "b1",
		JobID:           "job-1",
		WorkpoolID:      "pool-1",
		ExpectedVMCount: 2,
		Status:          BatchStatusStarted,
		RunningSince:    &runningSince,
	})
	// GCP has 3 VMs for a batch that requested 2.
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 3, BatchJobStatusRunning)

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
	assert.True(t, b.Unhealthy)
	assert.Contains(t, w.BatchAPI.TerminatedJobs, "job-1")
	ps := w.Pools.MustGetState("pool-1")
	// TerminateJob clears VMs → idle transition follows.
	assert.Equal(t, WorkPoolStatusIdle, ps.State)
	assert.Greater(t, ps.IncidentCount, 0)
}

func TestTier2_Anomaly1_ExactVMCount_NoTermination(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	runningSince := epoch
	w.Batches.Add(&BatchAPIRequest{
		BatchID:         "b1",
		JobID:           "job-1",
		WorkpoolID:      "pool-1",
		ExpectedVMCount: 2,
		Status:          BatchStatusStarted,
		RunningSince:    &runningSince,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusRunning)

	// Two registered workers.
	w.Workers.Add(&Worker{WorkerID: "w1", BatchID: "b1", InstanceName: vmName("b1", 0), HeartbeatExpiry: epoch.Add(time.Hour)})
	w.Workers.Add(&Worker{WorkerID: "w2", BatchID: "b1", InstanceName: vmName("b1", 1), HeartbeatExpiry: epoch.Add(time.Hour)})

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	assert.Empty(t, w.BatchAPI.TerminatedJobs)
	assert.Empty(t, w.BatchAPI.TerminatedVMs)
}

// ---- Anomaly 2: startup failure ----

func TestTier2_Anomaly2_BeforeGracePeriod_NoAction(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	runningSince := epoch.Add(-1 * time.Minute) // 1 min ago, well inside grace period
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		ExpectedVMCount:       2,
		RegisteredWorkerCount: 0,
		Status:                BatchStatusStarted,
		RunningSince:          &runningSince,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusRunning)

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	assert.Empty(t, w.BatchAPI.TerminatedJobs)
	assert.Empty(t, w.BatchAPI.TerminatedVMs)
}

func TestTier2_Anomaly2_NoWorkersAfterGrace_WholeBatchTerminated(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	runningSince := epoch.Add(-6 * time.Minute) // past grace period
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		ExpectedVMCount:       2,
		RegisteredWorkerCount: 0, // no workers ever registered
		Status:                BatchStatusStarted,
		RunningSince:          &runningSince,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusRunning)

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
	assert.True(t, b.Unhealthy)
	assert.Contains(t, w.BatchAPI.TerminatedJobs, "job-1")
}

func TestTier2_Anomaly2_SomeWorkersRegistered_SurgicalTermination(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	runningSince := epoch.Add(-6 * time.Minute)
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		ExpectedVMCount:       3,
		RegisteredWorkerCount: 1, // 1 of 3 registered
		Status:                BatchStatusStarted,
		RunningSince:          &runningSince,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 3, BatchJobStatusRunning)
	// Only vm-b1-0 has a registered worker.
	w.Workers.Add(&Worker{WorkerID: "w1", BatchID: "b1", InstanceName: vmName("b1", 0), HeartbeatExpiry: epoch.Add(time.Hour)})

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	// Job should NOT be terminated; only the 2 unregistered VMs.
	assert.Empty(t, w.BatchAPI.TerminatedJobs)
	assert.Len(t, w.BatchAPI.TerminatedVMs, 2)
	b := w.Batches.MustGet("b1")
	assert.True(t, b.Unhealthy)
}

func TestTier2_Anomaly2_RunningSinceNil_Skipped(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	// Batch submitted long ago but never saw RUNNING — still QUEUED.
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		ExpectedVMCount:       2,
		RegisteredWorkerCount: 0,
		Status:                BatchStatusStarted,
		RunningSince:          nil, // never reached RUNNING
		SubmittedAt:           epoch.Add(-30 * time.Minute),
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusQueued)

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	// Anomaly 2 skipped because running_since is nil.
	assert.Empty(t, w.BatchAPI.TerminatedJobs)
	assert.Empty(t, w.BatchAPI.TerminatedVMs)
}

// ---- Anomaly 3: zombie workers ----

func TestTier2_Anomaly3_ZombieBelowThreshold_SurgicalTermination(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.VMShutdownGracePeriod = 1 * time.Minute
	pool.MaxZombiesBeforeAbort = 3
	w.Pools.Add(pool)

	runningSince := epoch.Add(-10 * time.Minute)
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		ExpectedVMCount:       3,
		RegisteredWorkerCount: 3,
		Status:                BatchStatusStarted,
		RunningSince:          &runningSince,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 3, BatchJobStatusRunning)
	// One zombie: heartbeat expired 2min ago, past the 1min grace period.
	w.Workers.Add(&Worker{WorkerID: "w1", BatchID: "b1", InstanceName: vmName("b1", 0), HeartbeatExpiry: epoch.Add(-2 * time.Minute)})
	// Two healthy workers.
	w.Workers.Add(&Worker{WorkerID: "w2", BatchID: "b1", InstanceName: vmName("b1", 1), HeartbeatExpiry: epoch.Add(time.Hour)})
	w.Workers.Add(&Worker{WorkerID: "w3", BatchID: "b1", InstanceName: vmName("b1", 2), HeartbeatExpiry: epoch.Add(time.Hour)})

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	// Only the zombie VM terminated, job preserved.
	assert.Len(t, w.BatchAPI.TerminatedVMs, 1)
	assert.Contains(t, w.BatchAPI.TerminatedVMs, vmName("b1", 0))
	assert.Empty(t, w.BatchAPI.TerminatedJobs)
	b := w.Batches.MustGet("b1")
	assert.True(t, b.Unhealthy)
}

func TestTier2_Anomaly3_ZombieAboveThreshold_WholeBatchAborted(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.VMShutdownGracePeriod = 1 * time.Minute
	pool.MaxZombiesBeforeAbort = 2
	w.Pools.Add(pool)

	runningSince := epoch.Add(-10 * time.Minute)
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		ExpectedVMCount:       3,
		RegisteredWorkerCount: 3,
		Status:                BatchStatusStarted,
		RunningSince:          &runningSince,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 3, BatchJobStatusRunning)
	// Three zombies, threshold is 2.
	for i := 0; i < 3; i++ {
		w.Workers.Add(&Worker{WorkerID: workerID(i), BatchID: "b1", InstanceName: vmName("b1", i), HeartbeatExpiry: epoch.Add(-2 * time.Minute)})
	}

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	assert.Contains(t, w.BatchAPI.TerminatedJobs, "job-1")
	b := w.Batches.MustGet("b1")
	assert.Equal(t, BatchStatusFailed, b.Status)
}

func TestTier2_Anomaly3_ZombieVMGone_NoTermination(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.VMShutdownGracePeriod = 1 * time.Minute
	pool.MaxZombiesBeforeAbort = 3
	w.Pools.Add(pool)

	runningSince := epoch.Add(-10 * time.Minute)
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		ExpectedVMCount:       1,
		RegisteredWorkerCount: 1,
		Status:                BatchStatusStarted,
		RunningSince:          &runningSince,
	})
	// Job in RUNNING state but the specific VM is already gone.
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 0, BatchJobStatusRunning)
	// Worker with expired heartbeat, but its VM is not in GCP (already terminated cleanly).
	w.Workers.Add(&Worker{WorkerID: "w1", BatchID: "b1", InstanceName: "vm-b1-0", HeartbeatExpiry: epoch.Add(-2 * time.Minute)})

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	assert.Empty(t, w.BatchAPI.TerminatedVMs)
	assert.Empty(t, w.BatchAPI.TerminatedJobs)
}

func TestTier2_Anomaly3_WithinGracePeriod_NoAction(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	pool.VMShutdownGracePeriod = 5 * time.Minute
	pool.MaxZombiesBeforeAbort = 3
	w.Pools.Add(pool)

	runningSince := epoch.Add(-10 * time.Minute)
	w.Batches.Add(&BatchAPIRequest{
		BatchID:               "b1",
		JobID:                 "job-1",
		WorkpoolID:            "pool-1",
		ExpectedVMCount:       2,
		RegisteredWorkerCount: 2,
		Status:                BatchStatusStarted,
		RunningSince:          &runningSince,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusRunning)
	// HeartbeatExpiry 2min ago, but grace period is 5min → not a zombie yet.
	w.Workers.Add(&Worker{WorkerID: "w1", BatchID: "b1", InstanceName: vmName("b1", 0), HeartbeatExpiry: epoch.Add(-2 * time.Minute)})
	w.Workers.Add(&Worker{WorkerID: "w2", BatchID: "b1", InstanceName: vmName("b1", 1), HeartbeatExpiry: epoch.Add(time.Hour)})

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	assert.Empty(t, w.BatchAPI.TerminatedVMs)
	assert.Empty(t, w.BatchAPI.TerminatedJobs)
}

// ---- running_since stamping via Tier 2 ----

func TestTier2_StampsRunningSince(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	w.Batches.Add(&BatchAPIRequest{
		BatchID:         "b1",
		JobID:           "job-1",
		WorkpoolID:      "pool-1",
		ExpectedVMCount: 1,
		Status:          BatchStatusStarted,
		RunningSince:    nil, // not yet stamped
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 1, BatchJobStatusRunning)

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	require.NotNil(t, b.RunningSince, "running_since should be stamped")
	assert.Equal(t, epoch, *b.RunningSince)
}

func TestTier2_RunningSinceNotOverwritten(t *testing.T) {
	w := newWorld()
	pool := defaultPool("pool-1")
	w.Pools.Add(pool)

	original := epoch.Add(-5 * time.Minute)
	w.Batches.Add(&BatchAPIRequest{
		BatchID:         "b1",
		JobID:           "job-1",
		WorkpoolID:      "pool-1",
		ExpectedVMCount: 2,
		Status:          BatchStatusStarted,
		RunningSince:    &original,
	})
	w.BatchAPI.AddJob("job-1", "b1", "pool-1", 2, BatchJobStatusRunning)
	w.Workers.Add(&Worker{WorkerID: "w1", BatchID: "b1", InstanceName: vmName("b1", 0), HeartbeatExpiry: epoch.Add(time.Hour)})
	w.Workers.Add(&Worker{WorkerID: "w2", BatchID: "b1", InstanceName: vmName("b1", 1), HeartbeatExpiry: epoch.Add(time.Hour)})

	err := w.A.runClusterReconciler(context.Background())
	require.NoError(t, err)

	b := w.Batches.MustGet("b1")
	require.NotNil(t, b.RunningSince)
	assert.Equal(t, original, *b.RunningSince, "running_since should not be overwritten")
}
