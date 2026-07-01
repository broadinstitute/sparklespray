package functest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	monitor "github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/broadinstitute/sparklespray/v100/scheduler"
)

// TestAdapterRoundTrips exercises each Firestore adapter store's read/write/query
// methods against the real emulator to confirm field names, queries, and ordering
// are correct.
func TestAdapterRoundTrips(t *testing.T) {
	startFirestoreEmulator(t)
	ctx := context.Background()
	fs := newFirestoreClient(t, ctx)

	t.Run("WorkPoolStore", func(t *testing.T) {
		pools := monitor.NewFirestoreWorkPoolStore(fs)
		poolID := "pool-" + randomID()

		writeWorkPool(t, ctx, fs, &fsWorkPoolDoc{
			WorkpoolID: poolID,
			Region:     "us-central1",
		})

		ws, err := pools.Get(ctx, poolID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if ws.Pool.WorkpoolID != poolID {
			t.Errorf("WorkpoolID: want %s, got %s", poolID, ws.Pool.WorkpoolID)
		}
		if ws.Pool.Region != "us-central1" {
			t.Errorf("Region: want us-central1, got %s", ws.Pool.Region)
		}

		all, err := pools.ListAll(ctx)
		if err != nil {
			t.Fatalf("ListAll: %v", err)
		}
		found := false
		for _, p := range all {
			if p.Pool.WorkpoolID == poolID {
				found = true
			}
		}
		if !found {
			t.Errorf("ListAll: pool %s not found", poolID)
		}

		if err := pools.SaveState(ctx, &monitor.WorkPoolState{
			WorkpoolID:   poolID,
			State:        monitor.WorkPoolStatusOK,
			StateMessage: "all good",
			IncidentCount: 3,
		}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		updated, err := pools.Get(ctx, poolID)
		if err != nil {
			t.Fatalf("Get after SaveState: %v", err)
		}
		if updated.State.State != monitor.WorkPoolStatusOK {
			t.Errorf("State after SaveState: want ok, got %s", updated.State.State)
		}
		if updated.State.IncidentCount != 3 {
			t.Errorf("IncidentCount after SaveState: want 3, got %d", updated.State.IncidentCount)
		}
	})

	t.Run("BatchRequestStore", func(t *testing.T) {
		batches := monitor.NewFirestoreBatchRequestStore(fs)
		batchID := "batch-" + randomID()
		jobID := "gcpjob-" + randomID()
		poolID := "pool-" + randomID()
		now := time.Now().UTC().Truncate(time.Millisecond)

		batch := &monitor.BatchAPIRequest{
			BatchID:         batchID,
			JobID:           jobID,
			WorkpoolID:      poolID,
			ExpectedVMCount: 2,
			Preemptible:     true,
			SubmittedAt:     now,
			Status:          monitor.BatchStatusPending,
		}
		if err := batches.Create(ctx, batch); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := batches.Get(ctx, batchID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.ExpectedVMCount != 2 {
			t.Errorf("ExpectedVMCount: want 2, got %d", got.ExpectedVMCount)
		}
		if !got.Preemptible {
			t.Error("Preemptible: want true")
		}

		byJob, err := batches.GetByJobID(ctx, jobID)
		if err != nil {
			t.Fatalf("GetByJobID: %v", err)
		}
		if byJob == nil || byJob.BatchID != batchID {
			t.Errorf("GetByJobID: want batch %s, got %v", batchID, byJob)
		}

		// Second batch in the same pool — failed, non-preemptible.
		batch2 := &monitor.BatchAPIRequest{
			BatchID:         "batch-" + randomID(),
			WorkpoolID:      poolID,
			ExpectedVMCount: 3,
			Preemptible:     false,
			SubmittedAt:     now.Add(time.Second),
			Status:          monitor.BatchStatusFailed,
		}
		if err := batches.Create(ctx, batch2); err != nil {
			t.Fatalf("Create batch2: %v", err)
		}

		list, err := batches.ListByWorkpool(ctx, poolID, []monitor.BatchStatus{
			monitor.BatchStatusPending, monitor.BatchStatusFailed,
		})
		if err != nil {
			t.Fatalf("ListByWorkpool: %v", err)
		}
		if len(list) != 2 {
			t.Errorf("ListByWorkpool: want 2, got %d", len(list))
		}
		// DESC by submitted_at: batch2 first.
		if list[0].BatchID != batch2.BatchID {
			t.Errorf("ListByWorkpool order: want %s first, got %s", batch2.BatchID, list[0].BatchID)
		}

		sum, err := batches.SumPreemptibleVMCount(ctx, poolID)
		if err != nil {
			t.Fatalf("SumPreemptibleVMCount: %v", err)
		}
		if sum != 2 {
			t.Errorf("SumPreemptibleVMCount: want 2, got %d", sum)
		}

		// Save: update status.
		got.Status = monitor.BatchStatusStarted
		if err := batches.Save(ctx, got); err != nil {
			t.Fatalf("Save: %v", err)
		}
		reread, _ := batches.Get(ctx, batchID)
		if reread.Status != monitor.BatchStatusStarted {
			t.Errorf("Status after Save: want started, got %s", reread.Status)
		}
	})

	t.Run("WorkerStore", func(t *testing.T) {
		workers := monitor.NewFirestoreWorkerStore(fs)
		now := time.Now().UTC()
		batchID := "batch-" + randomID()
		poolID := "pool-" + randomID()

		expiredWorker := &fsWorkerDoc{
			WorkerID:        "worker-" + randomID(),
			WorkpoolID:      poolID,
			BatchID:         batchID,
			InstanceName:    "vm-expired",
			Status:          "started",
			HeartbeatExpiry: now.Add(-5 * time.Minute),
		}
		aliveWorker := &fsWorkerDoc{
			WorkerID:        "worker-" + randomID(),
			WorkpoolID:      poolID,
			BatchID:         batchID,
			InstanceName:    "vm-alive",
			HeartbeatExpiry: now.Add(5 * time.Minute),
		}
		writeWorker(t, ctx, fs, expiredWorker)
		writeWorker(t, ctx, fs, aliveWorker)

		expired, err := workers.ListExpired(ctx, now)
		if err != nil {
			t.Fatalf("ListExpired: %v", err)
		}
		foundExpired := false
		for _, w := range expired {
			if w.WorkerID == expiredWorker.WorkerID {
				foundExpired = true
			}
			if w.WorkerID == aliveWorker.WorkerID {
				t.Errorf("ListExpired: alive worker %s should not appear", aliveWorker.WorkerID)
			}
		}
		if !foundExpired {
			t.Errorf("ListExpired: expired worker not returned")
		}

		byBatch, err := workers.ListByBatch(ctx, batchID)
		if err != nil {
			t.Fatalf("ListByBatch: %v", err)
		}
		if len(byBatch) < 2 {
			t.Errorf("ListByBatch: want >= 2, got %d", len(byBatch))
		}

		count, err := workers.CountActive(ctx, poolID, now)
		if err != nil {
			t.Fatalf("CountActive: %v", err)
		}
		if count != 1 {
			t.Errorf("CountActive: want 1, got %d", count)
		}
	})

	t.Run("TaskStore", func(t *testing.T) {
		tasks := monitor.NewFirestoreTaskStore(fs, nil)
		workerID := "worker-" + randomID()
		jobID := "job-" + randomID()
		poolID := "pool-" + randomID()

		claimedTask := &fsTaskDoc{
			TaskID:         "task-" + randomID(),
			JobID:          jobID,
			WorkpoolID:     poolID,
			Status:         "claimed",
			OwningWorkerID: workerID,
		}
		runningTask := &fsTaskDoc{
			TaskID:         "task-" + randomID(),
			JobID:          jobID,
			WorkpoolID:     poolID,
			Status:         "running",
			OwningWorkerID: workerID,
		}
		pendingTask := &fsTaskDoc{
			TaskID:     "task-" + randomID(),
			JobID:      jobID,
			WorkpoolID: poolID,
			Status:     "pending",
		}
		writeTask(t, ctx, fs, claimedTask)
		writeTask(t, ctx, fs, runningTask)
		writeTask(t, ctx, fs, pendingTask)

		byWorker, err := tasks.ListByWorker(ctx, workerID,
			[]monitor.TaskStatus{monitor.TaskStatusClaimed, monitor.TaskStatusRunning})
		if err != nil {
			t.Fatalf("ListByWorker: %v", err)
		}
		if len(byWorker) != 2 {
			t.Errorf("ListByWorker: want 2, got %d", len(byWorker))
		}

		count, err := tasks.CountPending(ctx, poolID)
		if err != nil {
			t.Fatalf("CountPending: %v", err)
		}
		if count != 1 {
			t.Errorf("CountPending: want 1, got %d", count)
		}

		if err := tasks.ResetToPending(ctx, claimedTask.TaskID, jobID, monitor.TaskStatusClaimed); err != nil {
			t.Fatalf("ResetToPending: %v", err)
		}
		snap, err := fs.Collection(monitor.CollectionTasks).Doc(claimedTask.TaskID).Get(ctx)
		if err != nil {
			t.Fatalf("reading task after reset: %v", err)
		}
		var after fsTaskDoc
		snap.DataTo(&after)
		if after.Status != "pending" {
			t.Errorf("status after ResetToPending: want pending, got %s", after.Status)
		}
		if after.OwningWorkerID != "" {
			t.Errorf("owning_worker_id after ResetToPending: want empty, got %s", after.OwningWorkerID)
		}

		counts, err := tasks.CountByJob(ctx, jobID)
		if err != nil {
			t.Fatalf("CountByJob: %v", err)
		}
		// claimed was reset to pending above, so: pending=2, running=1
		if counts["pending"] != 2 {
			t.Errorf("CountByJob pending: want 2, got %d", counts["pending"])
		}
		if counts["running"] != 1 {
			t.Errorf("CountByJob running: want 1, got %d", counts["running"])
		}
	})

	t.Run("JobSummaryStore", func(t *testing.T) {
		summaries := monitor.NewFirestoreJobSummaryStore(fs)
		jobID := "job-" + randomID()

		summary := &monitor.JobSummary{
			JobID:      jobID,
			WorkpoolID: "pool-test",
			State:     monitor.JobStatusPending,
			Expiry:     time.Now().Add(24 * time.Hour),
		}
		if err := summaries.Create(ctx, summary); err != nil {
			t.Fatalf("Create: %v", err)
		}

		list, err := summaries.ListNonTerminal(ctx)
		if err != nil {
			t.Fatalf("ListNonTerminal: %v", err)
		}
		found := false
		for _, s := range list {
			if s.JobID == jobID {
				found = true
			}
		}
		if !found {
			t.Errorf("ListNonTerminal: job %s not found", jobID)
		}

		summary.State = monitor.JobStatusSuccess
		if err := summaries.Save(ctx, summary); err != nil {
			t.Fatalf("Save: %v", err)
		}
		// After becoming terminal, ListNonTerminal should no longer include it.
		list2, err := summaries.ListNonTerminal(ctx)
		if err != nil {
			t.Fatalf("ListNonTerminal after terminal: %v", err)
		}
		for _, s := range list2 {
			if s.JobID == jobID {
				t.Errorf("ListNonTerminal: terminal job %s should not appear", jobID)
			}
		}

		history := &monitor.JobSummaryHistory{
			JobID:      jobID,
			WorkpoolID: "pool-test",
			Timestamp:  time.Now(),
			State:     monitor.JobStatusSuccess,
		}
		if err := summaries.SaveHistory(ctx, history); err != nil {
			t.Fatalf("SaveHistory: %v", err)
		}
	})
}

// TestOrphanRequeue verifies that RunRequeueOrphanedTasks resets tasks owned by
// an expired worker back to pending while leaving tasks owned by an alive worker
// unchanged.
func TestOrphanRequeue(t *testing.T) {
	startFirestoreEmulator(t)
	ctx := context.Background()
	fs := newFirestoreClient(t, ctx)

	now := time.Now().UTC()
	clock := scheduler.NewFakeClock(now)

	expiredWorkerID := "worker-expired-" + randomID()
	aliveWorkerID := "worker-alive-" + randomID()
	poolID := "pool-" + randomID()

	writeWorker(t, ctx, fs, &fsWorkerDoc{
		WorkerID:        expiredWorkerID,
		WorkpoolID:      poolID,
		Status:          "started",
		HeartbeatExpiry: now.Add(-10 * time.Minute),
	})
	writeWorker(t, ctx, fs, &fsWorkerDoc{
		WorkerID:        aliveWorkerID,
		WorkpoolID:      poolID,
		HeartbeatExpiry: now.Add(10 * time.Minute),
	})

	claimedTask := &fsTaskDoc{
		TaskID:         "task-" + randomID(),
		WorkpoolID:     poolID,
		Status:         "claimed",
		OwningWorkerID: expiredWorkerID,
	}
	runningTask := &fsTaskDoc{
		TaskID:         "task-" + randomID(),
		WorkpoolID:     poolID,
		Status:         "running",
		OwningWorkerID: expiredWorkerID,
	}
	aliveTask := &fsTaskDoc{
		TaskID:         "task-" + randomID(),
		WorkpoolID:     poolID,
		Status:         "claimed",
		OwningWorkerID: aliveWorkerID,
	}
	writeTask(t, ctx, fs, claimedTask)
	writeTask(t, ctx, fs, runningTask)
	writeTask(t, ctx, fs, aliveTask)

	workers := monitor.NewFirestoreWorkerStore(fs)
	tasks := monitor.NewFirestoreTaskStore(fs, nil)
	m, _ := newMonitorWithStores(
		clock,
		monitor.NewFirestoreWorkPoolStore(fs),
		monitor.NewFirestoreBatchRequestStore(fs),
		workers,
		tasks,
	)

	if err := m.RunRequeueOrphanedTasks(ctx); err != nil {
		t.Fatalf("RunRequeueOrphanedTasks: %v", err)
	}

	checkStatus := func(taskID, wantStatus string) {
		t.Helper()
		snap, err := fs.Collection(monitor.CollectionTasks).Doc(taskID).Get(ctx)
		if err != nil {
			t.Fatalf("reading task %s: %v", taskID, err)
		}
		var doc fsTaskDoc
		snap.DataTo(&doc)
		if doc.Status != wantStatus {
			t.Errorf("task %s: want status=%s, got %s", taskID, wantStatus, doc.Status)
		}
		if wantStatus == "pending" && doc.OwningWorkerID != "" {
			t.Errorf("task %s: want owning_worker_id cleared, got %s", taskID, doc.OwningWorkerID)
		}
	}

	checkStatus(claimedTask.TaskID, "pending")
	checkStatus(runningTask.TaskID, "pending")
	checkStatus(aliveTask.TaskID, "claimed") // alive worker's task must not be touched
}

// TestJobSummaryPollPublishesTerminationEvent verifies that RunJobSummaryPoll
// updates a JobSummary to the correct terminal status and publishes a
// job_terminated event exactly once.
func TestJobSummaryPollPublishesTerminationEvent(t *testing.T) {
	startFirestoreEmulator(t)
	ctx := context.Background()
	fs := newFirestoreClient(t, ctx)

	clock := scheduler.NewFakeClock(time.Now().UTC())

	jobID := "job-" + randomID()
	poolID := "pool-" + randomID()

	// Write 3 tasks all in success state.
	for i := range 3 {
		writeTask(t, ctx, fs, &fsTaskDoc{
			TaskID:     fmt.Sprintf("task-%s-%d", randomID(), i),
			JobID:      jobID,
			WorkpoolID: poolID,
			Status:     "success",
		})
	}

	// Create a JobSummary in pending state.
	summaries := monitor.NewFirestoreJobSummaryStore(fs)
	if err := summaries.Create(ctx, &monitor.JobSummary{
		JobID:      jobID,
		WorkpoolID: poolID,
		State:     monitor.JobStatusPending,
		Expiry:     time.Now().Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("Create JobSummary: %v", err)
	}

	tasks := monitor.NewFirestoreTaskStore(fs, nil)
	m, pub := newMonitorWithStores(
		clock,
		monitor.NewFirestoreWorkPoolStore(fs),
		monitor.NewFirestoreBatchRequestStore(fs),
		monitor.NewFirestoreWorkerStore(fs),
		tasks,
	)
	m.SetJobSummaryStore(summaries)

	if err := m.RunJobSummaryPoll(ctx); err != nil {
		t.Fatalf("RunJobSummaryPoll: %v", err)
	}

	// Verify the JobSummary was updated to success.
	snap, err := fs.Collection(monitor.CollectionJobSummary).Doc(jobID).Get(ctx)
	if err != nil {
		t.Fatalf("reading JobSummary: %v", err)
	}
	var got monitor.JobSummary
	if err := snap.DataTo(&got); err != nil {
		t.Fatalf("decoding JobSummary: %v", err)
	}
	if got.State != monitor.JobStatusSuccess {
		t.Errorf("JobSummary.State: want %s, got %s", monitor.JobStatusSuccess, got.State)
	}

	// Verify exactly one job_terminated event was published for this job.
	events := pub.Events()
	if len(events) != 1 {
		t.Errorf("job_terminated events: want 1, got %d", len(events))
	} else if events[0] != jobID {
		t.Errorf("job_terminated jobID: want %s, got %s", jobID, events[0])
	}
}
