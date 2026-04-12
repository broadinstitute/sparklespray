package autoscaler

import (
	"context"
	"errors"
	"testing"

	"github.com/broadinstitute/sparklesworker/backend"
)

func nullCreateEventPublisher(topic string) backend.EventPublisher {
	return &backend.NullEventPublisher{}
}

// ---- determineBatchJobsToCreate ----

func TestDetermineNodesToCreate(t *testing.T) {
	type tc struct {
		name                          string
		nonCompleteTaskCount          int
		maxInstanceCount              int
		maxPreemptableAttempts        int
		usedPreemptableAttempts       int
		currentRequestedInstanceCount int
		// expected fields on result
		wantNil      bool
		wantRequests []backend.BatchJobsToSubmit
	}

	tests := []tc{
		{
			name:                 "empty queue",
			nonCompleteTaskCount: 0,
			maxInstanceCount:     10,
			wantNil:              true,
		},
		{
			name:                          "cold start budget available",
			nonCompleteTaskCount:          3,
			maxInstanceCount:              10,
			maxPreemptableAttempts:        5,
			usedPreemptableAttempts:       0,
			currentRequestedInstanceCount: 0,
			wantRequests: []backend.BatchJobsToSubmit{
				{InstanceCount: 1, IsPreemptable: true, ShouldLinger: true},
				{InstanceCount: 2, IsPreemptable: true, ShouldLinger: false},
			},
		},
		{
			name:                          "cold start budget resets even when fully used",
			nonCompleteTaskCount:          3,
			maxInstanceCount:              10,
			maxPreemptableAttempts:        5,
			usedPreemptableAttempts:       5,
			currentRequestedInstanceCount: 0,
			wantRequests: []backend.BatchJobsToSubmit{
				{InstanceCount: 1, IsPreemptable: true, ShouldLinger: true},
				{InstanceCount: 2, IsPreemptable: true, ShouldLinger: false},
			},
		},
		{
			name:                          "cold start maxPreempt=0 lingering node non-preemptable",
			nonCompleteTaskCount:          3,
			maxInstanceCount:              10,
			maxPreemptableAttempts:        0,
			usedPreemptableAttempts:       0,
			currentRequestedInstanceCount: 0,
			wantRequests: []backend.BatchJobsToSubmit{
				{InstanceCount: 1, IsPreemptable: false, ShouldLinger: true},
				{InstanceCount: 2, IsPreemptable: false, ShouldLinger: false},
			},
		},
		{
			name:                          "cold start budget=1 lingering uses budget rest non-preemptable",
			nonCompleteTaskCount:          3,
			maxInstanceCount:              10,
			maxPreemptableAttempts:        1,
			usedPreemptableAttempts:       0,
			currentRequestedInstanceCount: 0,
			wantRequests: []backend.BatchJobsToSubmit{
				{InstanceCount: 1, IsPreemptable: true, ShouldLinger: true},
				{InstanceCount: 2, IsPreemptable: false, ShouldLinger: false},
			},
		},
		{
			name:                          "already at max capacity",
			nonCompleteTaskCount:          10,
			maxInstanceCount:              5,
			maxPreemptableAttempts:        10,
			usedPreemptableAttempts:       0,
			currentRequestedInstanceCount: 5,
			wantNil:                       true,
		},
		{
			name:                          "warm nodes running budget remaining fills gap",
			nonCompleteTaskCount:          5,
			maxInstanceCount:              10,
			maxPreemptableAttempts:        10,
			usedPreemptableAttempts:       2,
			currentRequestedInstanceCount: 2,
			wantRequests: []backend.BatchJobsToSubmit{
				{InstanceCount: 3, IsPreemptable: true, ShouldLinger: false},
			},
		},
		{
			name:                          "queue smaller than max capped at nonComplete",
			nonCompleteTaskCount:          2,
			maxInstanceCount:              10,
			maxPreemptableAttempts:        10,
			usedPreemptableAttempts:       0,
			currentRequestedInstanceCount: 0,
			wantRequests: []backend.BatchJobsToSubmit{
				{InstanceCount: 1, IsPreemptable: true, ShouldLinger: true},
				{InstanceCount: 1, IsPreemptable: true, ShouldLinger: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := determineBatchJobsToCreate(
				tt.nonCompleteTaskCount,
				tt.maxInstanceCount,
				tt.maxPreemptableAttempts,
				tt.usedPreemptableAttempts,
				tt.currentRequestedInstanceCount,
			)

			if tt.wantNil {
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
				return
			}

			if len(got) != len(tt.wantRequests) {
				t.Fatalf("expected %d requests, got %d: %+v", len(tt.wantRequests), len(got), got)
			}
			for i, want := range tt.wantRequests {
				g := got[i]
				if g.InstanceCount != want.InstanceCount || g.IsPreemptable != want.IsPreemptable || g.ShouldLinger != want.ShouldLinger {
					t.Errorf("request[%d]: want %+v, got %+v", i, want, *g)
				}
			}
		})
	}
}

// ---- findOrphanedTasks ----

func TestFindOrphanedTasks(t *testing.T) {
	task := func(id, owner string) *backend.Task {
		return &backend.Task{TaskID: id, OwnedByWorkerID: owner}
	}

	tests := []struct {
		name             string
		claimed          []*backend.Task
		runningInstances []string
		wantIDs          []string
	}{
		{
			name:             "all tasks have running owners",
			claimed:          []*backend.Task{task("t1", "i1"), task("t2", "i2")},
			runningInstances: []string{"i1", "i2"},
			wantIDs:          nil,
		},
		{
			name:             "all tasks have gone owners",
			claimed:          []*backend.Task{task("t1", "i1"), task("t2", "i2")},
			runningInstances: []string{},
			wantIDs:          []string{"t1", "t2"},
		},
		{
			name:             "mixed one running one gone",
			claimed:          []*backend.Task{task("t1", "i1"), task("t2", "i2")},
			runningInstances: []string{"i1"},
			wantIDs:          []string{"t2"},
		},
		{
			name:             "empty claimed tasks",
			claimed:          []*backend.Task{},
			runningInstances: []string{"i1"},
			wantIDs:          nil,
		},
		{
			name:             "empty running instances all orphaned",
			claimed:          []*backend.Task{task("t1", "i1")},
			runningInstances: []string{},
			wantIDs:          []string{"t1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findOrphanedTasks(tt.claimed, tt.runningInstances)
			if len(got) != len(tt.wantIDs) {
				t.Fatalf("expected %d orphans, got %d", len(tt.wantIDs), len(got))
			}
			for i, wantID := range tt.wantIDs {
				if got[i].TaskID != wantID {
					t.Errorf("orphan[%d]: want id %q, got %q", i, wantID, got[i].TaskID)
				}
			}
		})
	}
}

// ---- calcSetDiff ----

func TestCalcSetDiff(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want []string
	}{
		{"both empty", []string{}, []string{}, nil},
		{"b empty returns a", []string{"1", "2", "3"}, []string{}, []string{"1", "2", "3"}},
		{"a empty returns empty", []string{}, []string{"1", "2", "3"}, nil},
		{"equal sets returns empty", []string{"1", "2", "3"}, []string{"1", "2", "3"}, nil},
		{"removes subset", []string{"1", "2", "3"}, []string{"2"}, []string{"1", "3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calcSetDiff(tt.a, tt.b)
			if len(got) != len(tt.want) {
				t.Fatalf("want %v, got %v", tt.want, got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("index %d: want %q, got %q", i, tt.want[i], got[i])
				}
			}
		})
	}
}

// ---- checkClusterHealth ----

func makeCloud(listBatchJobsFn func(region, clusterID string) ([]*backend.BatchJob, error)) *mockCloud {
	return &mockCloud{
		listBatchJobsFn: listBatchJobsFn,
	}
}

func TestCheckClusterHealth(t *testing.T) {
	t.Run("pending=0 returns empty result without calling listBatchJobs", func(t *testing.T) {
		listCalled := false
		cloud := makeCloud(func(_, clusterID string) ([]*backend.BatchJob, error) {
			listCalled = true
			return nil, nil
		})
		sparkles := &mockSparkles{pendingTaskCount: 0}

		result, err := checkClusterHealth(cloud, sparkles, "cluster1", "us-central1", nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if result.suspiciouslyFailedToRun != 0 || len(result.newlyCompletedJobIDs) != 0 {
			t.Errorf("expected empty result, got %+v", result)
		}
		if listCalled {
			t.Error("listBatchJobs should not have been called")
		}
	})

	t.Run("newly completed job with 0 tasks is suspicious", func(t *testing.T) {
		cloud := makeCloud(func(_, clusterID string) ([]*backend.BatchJob, error) {
			return []*backend.BatchJob{{ID: "j1", State: backend.Complete}}, nil
		})
		sparkles := &mockSparkles{
			pendingTaskCount: 1,
			tasksCompletedBy: map[string]int{"j1": 0},
		}

		result, err := checkClusterHealth(cloud, sparkles, "cluster1", "us-central1", nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		if result.suspiciouslyFailedToRun != 1 {
			t.Errorf("want 1 suspicious failure, got %d", result.suspiciouslyFailedToRun)
		}
	})

	t.Run("previously seen completed job not re-evaluated", func(t *testing.T) {
		cloud := makeCloud(func(_, clusterID string) ([]*backend.BatchJob, error) {
			return []*backend.BatchJob{{ID: "j1", State: backend.Complete}}, nil
		})
		sparkles := &mockSparkles{
			pendingTaskCount: 1,
			tasksCompletedBy: map[string]int{"j1": 0},
		}

		result, err := checkClusterHealth(cloud, sparkles, "cluster1", "us-central1", []string{"j1"}, 1)
		if err != nil {
			t.Fatal(err)
		}
		if result.suspiciouslyFailedToRun != 0 {
			t.Errorf("previously seen job should not count as suspicious; got %d", result.suspiciouslyFailedToRun)
		}
		if len(result.newlyCompletedJobIDs) != 0 {
			t.Errorf("previously seen job should not appear in newlyCompleted; got %v", result.newlyCompletedJobIDs)
		}
	})

}

// ---- Poll (integration-style) ----

func defaultCloud() *mockCloud {
	return &mockCloud{
		listRunningInstancesFn: func(clusterID string, region string) ([]string, error) {
			return []string{}, nil
		},
		listBatchJobsFn: func(region, clusterID string) ([]*backend.BatchJob, error) {
			return []*backend.BatchJob{}, nil
		},
		submitBatchJobsFn: func(callback backend.CreateWorkerCommandCallback, cluster *backend.Cluster, clusterID string, requests []*backend.BatchJobsToSubmit) ([]string, error) {
			return nil, nil
		},
		deleteAllBatchJobsFn: func(region, clusterID string) error {
			return nil
		},
	}
}

func defaultSparkles() *mockSparkles {
	return &mockSparkles{
		clusterConfig: &backend.Cluster{
			MaxInstanceCount:       10,
			MaxPreemptableAttempts: 5,
			MaxSuspiciousFailures:  3,
		},
		pendingTaskCount:     0,
		nonCompleteTaskCount: 0,
		claimedTasks:         []*backend.Task{},
		tasksCompletedBy:     map[string]int{},
	}
}

func TestPoll(t *testing.T) {
	t.Run("happy path tasks pending nodes launched state updated", func(t *testing.T) {
		cloud := defaultCloud()
		var launched []*backend.BatchJobsToSubmit
		cloud.submitBatchJobsFn = func(callback backend.CreateWorkerCommandCallback, _ *backend.Cluster, _ string, requests []*backend.BatchJobsToSubmit) ([]string, error) {
			launched = requests
			return nil, nil
		}

		sparkles := defaultSparkles()
		sparkles.nonCompleteTaskCount = 3

		err := Poll(context.Background(), "cluster1", cloud, sparkles, sparkles, nil, nullCreateEventPublisher)
		if err != nil {
			t.Fatal(err)
		}
		if len(launched) == 0 {
			t.Error("expected nodes to be launched")
		}
		if sparkles.savedState == nil {
			t.Error("expected updateClusterMonitorState to be called")
		}
		if sparkles.savedState.BatchJobRequests == 0 {
			t.Error("expected batchJobRequests to be incremented")
		}
	})

	t.Run("getClusterConfig error returns immediately", func(t *testing.T) {
		cloud := defaultCloud()
		sparkles := defaultSparkles()
		sparkles.getClusterConfigErr = errors.New("config fetch failed")

		err := Poll(context.Background(), "cluster1", cloud, sparkles, sparkles, nil, nullCreateEventPublisher)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("orphaned task marked pending", func(t *testing.T) {
		cloud := defaultCloud()
		cloud.listRunningInstancesFn = func(clusterID string, region string) ([]string, error) {
			return []string{"i-running"}, nil
		}

		sparkles := defaultSparkles()
		sparkles.claimedTasks = []*backend.Task{
			{TaskID: "t1", OwnedByWorkerID: "i-gone"},
			{TaskID: "t2", OwnedByWorkerID: "i-running"},
		}

		err := Poll(context.Background(), "cluster1", cloud, sparkles, sparkles, nil, nullCreateEventPublisher)
		if err != nil {
			t.Fatal(err)
		}
		if len(sparkles.markTasksPendingCalled) != 1 {
			t.Fatalf("expected 1 task marked pending, got %d", len(sparkles.markTasksPendingCalled))
		}
		if sparkles.markTasksPendingCalled[0].TaskID != "t1" {
			t.Errorf("expected task t1 to be orphaned, got %s", sparkles.markTasksPendingCalled[0].TaskID)
		}
	})

	t.Run("no work to do submitBatchJobs not called", func(t *testing.T) {
		cloud := defaultCloud()
		launchCalled := false
		cloud.submitBatchJobsFn = func(callback backend.CreateWorkerCommandCallback, _ *backend.Cluster, _ string, requests []*backend.BatchJobsToSubmit) ([]string, error) {
			if len(requests) > 0 {
				launchCalled = true
			}
			return nil, nil
		}

		sparkles := defaultSparkles()
		sparkles.nonCompleteTaskCount = 0

		err := Poll(context.Background(), "cluster1", cloud, sparkles, sparkles, nil, nullCreateEventPublisher)
		if err != nil {
			t.Fatal(err)
		}
		if launchCalled {
			t.Error("submitBatchJobs should not have been called with work to do")
		}
	})

	t.Run("state persistence batchJobRequests reflects all updates", func(t *testing.T) {
		cloud := defaultCloud()

		sparkles := defaultSparkles()
		sparkles.nonCompleteTaskCount = 5

		err := Poll(context.Background(), "cluster1", cloud, sparkles, sparkles, nil, nullCreateEventPublisher)
		if err != nil {
			t.Fatal(err)
		}
		if sparkles.savedState == nil {
			t.Fatal("expected state to be saved")
		}
		// We launched nodes, so batchJobRequests should reflect that
		if sparkles.savedState.BatchJobRequests <= 0 {
			t.Errorf("expected batchJobRequests > 0, got %d", sparkles.savedState.BatchJobRequests)
		}
	})
}
