package autoscaler

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
)

func Poll(clusterID string, compute backend.WorkerPool, cluster backend.ClusterStore, tasks backend.TaskStore, createWorkerCommand backend.CreateWorkerCommandCallback) error {
	clusterConfig, err := cluster.GetClusterConfig(clusterID)
	if err != nil {
		return fmt.Errorf("Failed fetching cluster config: %s", err)
	}

	needsUpdateState := false
	lastState, err := clusterConfig.GetMonitorState()
	if err != nil {
		return fmt.Errorf("Failed retreiving monitor state: %s", err)
	}

	// phase 0: check for failing batch jobs that are a sign that the cluster configuration is in a broken state
	badStateResult, err := checkClusterHealth(compute, tasks, clusterID, clusterConfig.Region, lastState.SuspiciouslyFailingJobIds, lastState.BatchJobRequests)
	if err != nil {
		return fmt.Errorf("Failed while checking for bad state: %s", err)
	}

	// update last state with the additional new completions
	for _, completedJobID := range badStateResult.newlyCompletedJobIDs {
		lastState.SuspiciouslyFailingJobIds = append(lastState.SuspiciouslyFailingJobIds, completedJobID)
		log.Printf("Updated list of suspicious Batch JobIDs: %s", strings.Join(lastState.SuspiciouslyFailingJobIds, ","))
		needsUpdateState = true

		// update the count of suspicious failures and decide whether there's a problem or not
		if len(lastState.SuspiciouslyFailingJobIds) > clusterConfig.MaxSuspiciousFailures {
			// something wrong is going on. Shut everything down.
			msg := fmt.Sprintf("Found %d nodes had shut down without doing any work. (max allowed: %d) Aborting to avoid infinitely starting broken nodes.", len(lastState.SuspiciouslyFailingJobIds), clusterConfig.MaxSuspiciousFailures)
			log.Print(msg)
			return fmt.Errorf("Too many suspicious failures -- aborting")
		}
	}

	// phase 1: Identify orphaned tasks
	claimedTasks, err := tasks.GetClaimedTasks(clusterID)
	if err != nil {
		return fmt.Errorf("Could not quey claimed tasks: %s", err)
	}

	runningInstances, err := compute.ListRunningInstances(clusterID, clusterConfig.Region)
	if err != nil {
		return fmt.Errorf("Failed to query running instances: %s", err)
	}
	orphaned := findOrphanedTasks(claimedTasks, runningInstances)

	if len(orphaned) > 0 {
		log.Printf("Found %d orphaned tasks, resetting their state to 'pending'", len(orphaned))

		err = tasks.MarkTasksPending(orphaned)
		if err != nil {
			return fmt.Errorf("Could not mark orphaned tasks as pending: %s", err)
		}
	}

	// phase 2: Resize cluster as needed

	nonCompleteTaskCount, err := tasks.GetNonCompleteTaskCount(clusterID)
	if err != nil {
		return fmt.Errorf("Could not query noncomplete tasks: %s", err)
	}

	activeBatchJobs, err := compute.ListBatchJobs(clusterConfig.Region, clusterID)
	if err != nil {
		return fmt.Errorf("Could not query current requested instances: %s", err)
	}
	currentRequestedInstanceCount := 0
	for _, job := range activeBatchJobs {
		if job.State == backend.Pending || job.State == backend.Running {
			currentRequestedInstanceCount += job.RequestedInstances
		}
	}

	newBatchJobs := determineBatchJobsToCreate(
		nonCompleteTaskCount,
		clusterConfig.MaxInstanceCount,
		clusterConfig.MaxPreemptableAttempts,
		clusterConfig.UsedPreemptableAttempts,
		currentRequestedInstanceCount,
	)

	if len(newBatchJobs) > 0 {
		log.Printf("Requesting %d new batches of nodes", len(newBatchJobs))
		err = compute.SubmitBatchJobs(createWorkerCommand, clusterConfig, clusterID, newBatchJobs)
		if err != nil {
			return fmt.Errorf("Could not create nodes: %s", err)
		}
		lastState.BatchJobRequests += len(newBatchJobs)
		needsUpdateState = true
	}

	if needsUpdateState {
		// only update if state actually changed
		err = cluster.UpdateClusterMonitorState(clusterID, lastState)
		if err != nil {
			return fmt.Errorf("failed to update monitor state: %s", err)
		}
	}

	return nil
}

func determineBatchJobsToCreate(
	nonCompleteTaskCount int,
	maxInstanceCount int,
	maxPreemptableAttempts int,
	usedPreemptableAttempts int,
	currentRequestedInstanceCount int,
) []*backend.BatchJobsToSubmit {

	targetCount := min(maxInstanceCount, nonCompleteTaskCount)
	batchJobsToRequest := targetCount - currentRequestedInstanceCount
	if batchJobsToRequest <= 0 {
		return nil
	}

	remainingPreemptableAttempts := maxPreemptableAttempts - usedPreemptableAttempts

	// Cold start: reset preemptable budget so each new run starts fresh
	if currentRequestedInstanceCount == 0 {
		remainingPreemptableAttempts = maxPreemptableAttempts
	}

	var requests []*backend.BatchJobsToSubmit

	// Cold start: create one lingering node first
	if currentRequestedInstanceCount == 0 {
		isPreemptable := remainingPreemptableAttempts > 0
		if isPreemptable {
			remainingPreemptableAttempts--
		}
		requests = append(requests, &backend.BatchJobsToSubmit{
			InstanceCount: 1,
			IsPreemptable: isPreemptable,
			ShouldLinger:  true,
		})
		batchJobsToRequest--
	}

	// Fill remaining slots with preemptable nodes while budget allows
	if remainingPreemptableAttempts > 0 && batchJobsToRequest > 0 {
		instanceCount := min(remainingPreemptableAttempts, batchJobsToRequest)
		requests = append(requests, &backend.BatchJobsToSubmit{
			InstanceCount: instanceCount,
			IsPreemptable: true,
			ShouldLinger:  false,
		})
		batchJobsToRequest -= instanceCount
	}

	// Any remaining slots are non-preemptable
	if batchJobsToRequest > 0 {
		requests = append(requests, &backend.BatchJobsToSubmit{
			InstanceCount: batchJobsToRequest,
			IsPreemptable: false,
			ShouldLinger:  false,
		})
	}

	return requests
}

func findOrphanedTasks(claimedTasks []*backend.Task, runningInstances []string) []*backend.Task {
	running := make(map[string]struct{}, len(runningInstances))
	for _, id := range runningInstances {
		running[id] = struct{}{}
	}

	var orphaned []*backend.Task
	for _, task := range claimedTasks {
		if task.OwnedByWorkerID == "localhost" {
			// special case: tasks which are running not on a GCP node. Mostly for
			// testing, but there's no way for us to be sure whether these are running
			// or not, so just assume they're fine.
			continue
		}

		if _, ok := running[task.OwnedByWorkerID]; !ok {
			orphaned = append(orphaned, task)
		}
	}
	return orphaned
}

type HealthCheckResult struct {
	suspiciouslyFailedToRun int
	newlyCompletedJobIDs    []string
}

func checkClusterHealth(compute backend.WorkerPool, tasks backend.TaskStore, clusterID string, region string, previouslyCompletedJobIDs []string, expectedJobCount int) (*HealthCheckResult, error) {
	// the heuristic we're using is: a bad job is one where it started and stopped without doing anything. That's a sign
	// that if we turn on another one, the same thing might happen.
	// but first, rule out the trivial case which isn't a problem: there's no work left to do

	pendingTaskCount, err := tasks.GetPendingTaskCount(clusterID)
	if err != nil {
		return nil, fmt.Errorf("Could not quey pending task count: %s", err)
	}

	if pendingTaskCount == 0 {
		// no more work to do means we certainly won't be trying to turn on any new nodes and it's normal that nodes should shut down without doing any work
		return &HealthCheckResult{}, nil
	}

	// now what jobs have completed
	jobs, err := compute.ListBatchJobs(region, clusterID)
	if err != nil {
		return nil, fmt.Errorf("Could not quey completed batch jobs: %s", err)
	}

	suspiciouslyFailedToRun := 0

	// subset to just the completed jobs which appear to have immediately exited
	susCompletedJobIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if (job.State == backend.Failed || job.State == backend.Complete) && job.RunDuration < 10*time.Second {
			susCompletedJobIDs = append(susCompletedJobIDs, job.ID)
		}
	}

	// compare that with the previously completed jobs to figure out which ones are new completions
	newSusJobIDs := calcSetDiff(susCompletedJobIDs, previouslyCompletedJobIDs)

	// // I believe I've seen that querying for batch jobs does not immediately return new jobs. So, let's confirm we see the expected number of jobs. If we don't
	// // we probably want to hold off doing anything more for the moment
	// if len(jobs) != expectedJobCount {
	// 	return nil, fmt.Errorf("Expected to find %d Batch jobs associated with cluster %s, but found %d instead", expectedJobCount, clusterID, len(jobs))
	// }

	// for each check: did we successfully complete any jobs before shutting down?
	// for _, newlyCompletedJobID := range newlyCompletedJobIDs {
	// 	count := tasks.GetTasksCompletedBy(newlyCompletedJobID)
	// 	if count == 0 {
	// 		suspiciouslyFailedToRun += 1
	// 	}
	// }

	// count up the number of jobs which failed to do any real work
	suspiciouslyFailedToRun += len(newSusJobIDs)

	return &HealthCheckResult{suspiciouslyFailedToRun: suspiciouslyFailedToRun, newlyCompletedJobIDs: newSusJobIDs}, nil
}

func calcSetDiff(a []string, b []string) []string {
	bSet := make(map[string]struct{}, len(b))
	for _, s := range b {
		bSet[s] = struct{}{}
	}
	var diff []string
	for _, s := range a {
		if _, ok := bSet[s]; !ok {
			diff = append(diff, s)
		}
	}
	return diff
}
