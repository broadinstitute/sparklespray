package autoscaler

import (
	"fmt"
	"log"

	"github.com/broadinstitute/sparklesworker/task_queue"
)

func Poll(clusterID string, gshim CloudMethodsForPoll, sshim SparklesMethodsForPoll) error {
	clusterConfig, err := sshim.getClusterConfig(clusterID)
	if err != nil {
		return fmt.Errorf("Failed fetching cluster config: %s", err)
	}

	lastState, err := clusterConfig.getMonitorState()
	if err != nil {
		return fmt.Errorf("Failed retreiving monitor state: %s", err)
	}

	// phase 0: check for failing batch jobs that are a sign that the cluster configuration is in a broken state
	badStateResult, err := checkClusterHealth(gshim, sshim, clusterID, clusterConfig.Region, lastState.completedJobIds, lastState.batchJobRequests)
	if err != nil {
		return fmt.Errorf("Failed while checking for bad state: %s", err)
	}

	// update last state with the additional new completions
	for _, completedJobID := range badStateResult.newlyCompletedJobIDs {
		lastState.completedJobIds = append(lastState.completedJobIds, completedJobID)
	}

	// update the count of suspicious failures and decide whether there's a problem or not
	lastState.suspiciouslyFailedToRun = badStateResult.suspiciouslyFailedToRun + lastState.suspiciouslyFailedToRun
	if lastState.suspiciouslyFailedToRun > clusterConfig.MaxSuspiciousFailures {
		// something wrong is going on. Shut everything down.
		msg := fmt.Sprintf("Found %d nodes had shut down without doing any work. (max allowed: %d) Shut down entire cluster in to avoid infinitely starting broken nodes.", lastState.suspiciouslyFailedToRun, clusterConfig.MaxSuspiciousFailures)
		log.Print(msg)
		gshim.deleteAllBatchJobs(clusterConfig.Region, clusterID)
		return fmt.Errorf("%s", msg)
	}

	// phase 1: Identify orphaned tasks

	claimedTasks, err := sshim.getClaimedTasks(clusterID)
	if err != nil {
		return fmt.Errorf("Could not quey claimed tasks: %s", err)
	}

	runningInstances, err := gshim.listRunningInstances(clusterConfig.Zones, clusterID)
	if err != nil {
		return fmt.Errorf("Failed to query running instances: %s", err)
	}
	orphaned := findOrphanedTasks(claimedTasks, runningInstances)

	err = sshim.markTasksPending(orphaned)
	if err != nil {
		return fmt.Errorf("Could not mark orphaned tasks as pending: %s", err)
	}

	// phase 2: Resize cluster as needed

	nonCompleteTaskCount, err := sshim.getNonCompleteTaskCount(clusterID)
	if err != nil {
		return fmt.Errorf("Could not query noncomplete tasks: %s", err)
	}

	activeBatchJobs, err := gshim.listBatchJobs(clusterConfig.Region, clusterID)
	if err != nil {
		return fmt.Errorf("Could not query current requested instances: %s", err)
	}
	currentRequestedInstanceCount := 0
	for _, job := range activeBatchJobs {
		if job.State == Pending || job.State == Running {
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

	err = gshim.submitBatchJobs(clusterConfig, clusterID, newBatchJobs)
	if err != nil {
		return fmt.Errorf("Could not create nodes: %s", err)
	}

	lastState.batchJobRequests += len(newBatchJobs)

	err = sshim.updateClusterMonitorState(clusterID, lastState)
	if err != nil {
		return fmt.Errorf("failed to update monitor state: %s", err)
	}

	return nil
}

func determineBatchJobsToCreate(
	nonCompleteTaskCount int,
	maxInstanceCount int,
	maxPreemptableAttempts int,
	usedPreemptableAttempts int,
	currentRequestedInstanceCount int,
) []*BatchJobsToSubmit {

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

	var requests []*BatchJobsToSubmit

	// Cold start: create one lingering node first
	if currentRequestedInstanceCount == 0 {
		isPreemptable := remainingPreemptableAttempts > 0
		if isPreemptable {
			remainingPreemptableAttempts--
		}
		requests = append(requests, &BatchJobsToSubmit{
			instanceCount: 1,
			isPreemptable: isPreemptable,
			shouldLinger:  true,
		})
		batchJobsToRequest--
	}

	// Fill remaining slots with preemptable nodes while budget allows
	if remainingPreemptableAttempts > 0 && batchJobsToRequest > 0 {
		instanceCount := min(remainingPreemptableAttempts, batchJobsToRequest)
		requests = append(requests, &BatchJobsToSubmit{
			instanceCount: instanceCount,
			isPreemptable: true,
			shouldLinger:  false,
		})
		batchJobsToRequest -= instanceCount
	}

	// Any remaining slots are non-preemptable
	if batchJobsToRequest > 0 {
		requests = append(requests, &BatchJobsToSubmit{
			instanceCount: batchJobsToRequest,
			isPreemptable: false,
			shouldLinger:  false,
		})
	}

	return requests
}

func findOrphanedTasks(claimedTasks []*task_queue.Task, runningInstances []string) []*task_queue.Task {
	running := make(map[string]struct{}, len(runningInstances))
	for _, id := range runningInstances {
		running[id] = struct{}{}
	}

	var orphaned []*task_queue.Task
	for _, task := range claimedTasks {
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

func checkClusterHealth(gshim CloudMethodsForPoll, sshim SparklesMethodsForPoll, clusterID string, region string, previouslyCompletedJobIDs []string, expectedJobCount int) (*HealthCheckResult, error) {
	// the heuristic we're using is: a bad job is one where it started and stopped without doing anything. That's a sign
	// that if we turn on another one, the same thing might happen.
	// but first, rule out the trivial case which isn't a problem: there's no work left to do

	pendingTaskCount, err := sshim.getPendingTaskCount(clusterID)
	if err != nil {
		return nil, fmt.Errorf("Could not quey pending task count: %s", err)
	}

	if pendingTaskCount == 0 {
		// no more work to do means we certainly won't be trying to turn on any new nodes and it's normal that nodes should shut down without doing any work
		return &HealthCheckResult{}, nil
	}

	// now what jobs have completed
	jobs, err := gshim.listBatchJobs(region, clusterID)
	if err != nil {
		return nil, fmt.Errorf("Could not quey completed batch jobs: %s", err)
	}

	// I believe I've seen that querying for batch jobs does not immediately return new jobs. So, let's confirm we see the expected number of jobs. If we don't
	// we probably want to hold off doing anything more for the moment
	if len(jobs) != expectedJobCount {
		return nil, fmt.Errorf("Expected to find %d Batch jobs associated with cluster %s, but found %d instead", expectedJobCount, clusterID, len(jobs))
	}

	// subset to just the completed jobs
	completedJobIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if job.State == Failed || job.State == Complete {
			completedJobIDs = append(completedJobIDs, job.ID)
		}
	}

	// compare that with the previously completed jobs to figure out which ones are new completions
	newlyCompletedJobIDs := calcSetDiff(completedJobIDs, previouslyCompletedJobIDs)

	// for each check: did we successfully complete any jobs before shutting down?
	suspiciouslyFailedToRun := 0
	for _, newlyCompletedJobID := range newlyCompletedJobIDs {
		count := sshim.getTasksCompletedBy(newlyCompletedJobID)
		if count == 0 {
			suspiciouslyFailedToRun += 1
		}
	}

	return &HealthCheckResult{suspiciouslyFailedToRun: suspiciouslyFailedToRun, newlyCompletedJobIDs: newlyCompletedJobIDs}, nil
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
