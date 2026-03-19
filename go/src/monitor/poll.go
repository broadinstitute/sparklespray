package monitor

import (
	"context"
	"fmt"
	"log"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/api/iterator"
)

type NodesToRequest struct {
	nodeCount     int
	isPreemptable bool
	shouldLinger  bool
}

func determineNodesToCreate(
	nonCompleteTaskCount int,
	maxInstanceCount int,
	maxPreemptableAttempts int,
	usedPreemptableAttempts int,
	currentRequestedInstanceCount int,
) []*NodesToRequest {

	targetCount := min(maxInstanceCount, nonCompleteTaskCount)
	nodesToRequest := targetCount - currentRequestedInstanceCount
	if nodesToRequest <= 0 {
		return nil
	}

	remainingPreemptableAttempts := maxPreemptableAttempts - usedPreemptableAttempts
	var requests []*NodesToRequest

	// Cold start: create one lingering node first
	if currentRequestedInstanceCount == 0 {
		isPreemptable := remainingPreemptableAttempts > 0
		if isPreemptable {
			remainingPreemptableAttempts--
		}
		requests = append(requests, &NodesToRequest{
			nodeCount:     1,
			isPreemptable: isPreemptable,
			shouldLinger:  true,
		})
		nodesToRequest--
	}

	// Fill remaining slots with preemptable nodes while budget allows
	if remainingPreemptableAttempts > 0 && nodesToRequest > 0 {
		nodeCount := min(remainingPreemptableAttempts, nodesToRequest)
		requests = append(requests, &NodesToRequest{
			nodeCount:     nodeCount,
			isPreemptable: true,
			shouldLinger:  false,
		})
		nodesToRequest -= nodeCount
	}

	// Any remaining slots are non-preemptable
	if nodesToRequest > 0 {
		requests = append(requests, &NodesToRequest{
			nodeCount:     nodesToRequest,
			isPreemptable: false,
			shouldLinger:  false,
		})
	}

	return requests
}

type Task struct {
	id      string
	ownedBy string // always non-empty for claimed tasks
}

func findOrphanedTasks(claimedTasks []*Task, runningInstances []string) []*Task {
	running := make(map[string]struct{}, len(runningInstances))
	for _, id := range runningInstances {
		running[id] = struct{}{}
	}

	var orphaned []*Task
	for _, task := range claimedTasks {
		if _, ok := running[task.ownedBy]; !ok {
			orphaned = append(orphaned, task)
		}
	}
	return orphaned
}

func queryCompletedJobs(clusterID string) ([]string, error) {
	//	req := &batchpb.ListJobsRequest{
	//	    Parent: fmt.Sprintf("projects/%s/locations/%s", projectID, region),
	//	    Filter: `labels.sparkles-cluster = "some-uuid"`,
	//	}
	panic("unimplemente")
}

type BadStateCheckResult struct {
	suspiciouslyFailedToRun int
	newlyCompletedJobIDs    []string
}

func detectBadState(sshim *SparklesShim, previouslyCompletedJobIDs []string) (*BadStateCheckResult, error) {
	// the heuristic we're using is: a bad job is one where it started and stopped without doing anything. That's a sign
	// that if we turn on another one, the same thing might happen.
	// but first, rule out the trivial case which isn't a problem: there's no work left to do

	pendingTaskCount, err := sshim.getPendingTaskCount()
	if err != nil {
		return nil, fmt.Errorf("Could not quey pending task count: %s", err)
	}

	if pendingTaskCount == 0 {
		// no more work to do means we certainly won't be trying to turn on any new nodes and it's normal that nodes should shut down without doing any work
		return &BadStateCheckResult{}, nil
	}

	// now what jobs have completed
	completedJobIDs, err := queryCompletedJobs(clusterID)
	if err != nil {
		return nil, fmt.Errorf("Could not quey completed batch jobs: %s", err)
	}

	// compare that with the previously completed jobs
	newlyCompletedJobIDs := calcSetDiff(completedJobIDs, previouslyCompletedJobIDs)

	suspiciouslyFailedToRun := 0

	// for each check: did we successfully complete any jobs before shutting down?
	for _, newlyCompletedJobID := range newlyCompletedJobIDs {
		count := sshim.getTasksCompletedBy(newlyCompletedJobID)
		if count == 0 {
			suspiciouslyFailedToRun += 1
		}
	}

	return &BadStateCheckResult{suspiciouslyFailedToRun: suspiciouslyFailedToRun, newlyCompletedJobIDs: newlyCompletedJobIDs}, nil
}

type LastState struct {
	suspiciouslyFailedToRun int
	completedJobIds         []string
}

func poll(projectID string, clusterID string, gshim *GCPShim, sshim *SparklesShim) error {
	clusterConfig, err := sshim.getClusterConfig(projectID, clusterID)

	lastState, err := clusterConfig.getLastState()

	// phase 0: check for failing
	badStateResult, err := detectBadState(sshim, lastState.completedJobIds)
	if err != nil {
		return fmt.Errorf("Failed while checking for bad state: %s", err)
	}

	for _, completedJobID := range badStateResult.newlyCompletedJobIDs {
		lastState.completedJobIds = append(lastState.completedJobIds, completedJobID)
	}

	lastState.suspiciouslyFailedToRun = badStateResult.suspiciouslyFailedToRun + lastState.suspiciouslyFailedToRun

	if lastState.suspiciouslyFailedToRun > clusterConfig.maxSuspiciousFailures {
		// something wrong is going on. Shut everything down.
		msg := fmt.Sprintf("Found %d nodes had shut down without doing any work. (max allowed: %d) Shut down entire cluster in to avoid infinitely starting broken nodes.", lastState.suspiciouslyFailedToRun, clusterConfig.maxSuspiciousFailures)
		log.Printf(msg)
		terminateCluster()
		return fmt.Errorf(msg)
	}

	// phase 1: Identify orphaned tasks

	claimedTasks, err := sshim.getClaimedTasks()
	if err != nil {
		return fmt.Errorf("Could not quey claimed tasks: %s", err)
	}

	runningInstances, err := gshim.queryRunningInstances(context.Background(), projectID, clusterConfig.zones, clusterID)
	if err != nil {
		return fmt.Errorf("Failed to query running instances: %s", err)
	}
	orphaned := findOrphanedTasks(claimedTasks, runningInstances)

	err = sshim.markTasksPending(orphaned)
	if err != nil {
		return fmt.Errorf("Could not mark orphaned tasks as pending: %s", err)
	}

	// phase 2: Resize cluster as needed

	nonCompleteTaskCount, err := sshim.getNonCompleteTaskCount()
	if err != nil {
		return fmt.Errorf("Could not query noncomplete tasks: %s", err)
	}

	currentRequestedInstanceCount, err := gshim.getCurrentRequestedInstanceCount()
	if err != nil {
		return fmt.Errorf("Could not query current requested instances: %s", err)
	}

	newNodes := determineNodesToCreate(
		nonCompleteTaskCount,
		clusterConfig.maxInstanceCount,
		clusterConfig.maxPreemptableAttempts,
		clusterConfig.usedPreemptableAttempts,
		currentRequestedInstanceCount,
	)

	err = gshim.createNodes(newNodes)
	if err != nil {
		return fmt.Errorf("Could not create nodes: %s", err)
	}

}

type GCPShim struct {
	instancesClient *compute.InstancesClient
}

type SparklesShim struct {
}

func (s *SparklesShim) getNonCompleteTaskCount() (int, error) {
	panic("unimplemented")
}

func (s *SparklesShim) getClaimedTasks() ([]*Task, error) {
	panic("unimplemented")
}

func (s *SparklesShim) markTasksPending(tasks []*Task) error {
	panic("unimplemented")
}

func (s *SparklesShim) getClusterConfig(projectID string, clusterID string) (Cluster, error) {
	panic("unimplemented")
}

func (s *SparklesShim) getPendingTaskCount() (int, error) {
	panic("unimplemented")
}

func (s *SparklesShim) getTasksCompletedBy(jobID string) int {
	panic("unimplemented")
}

func calcSetDiff(a []string, b []string) []string {
	panic("unimplemented")
}

func terminateCluster() {
	panic("unimplemented")
}

func (g *GCPShim) queryRunningInstances(ctx context.Context, projectID string, zones []string, clusterUUID string) ([]string, error) {
	var instanceNames []string

	filter := proto.String(fmt.Sprintf(`labels.sparkles-cluster-uuid = "%s"`, clusterUUID))

	for _, zone := range zones {
		req := &computepb.ListInstancesRequest{
			Project: projectID,
			Zone:    zone,
			Filter:  filter,
		}

		it := g.instancesClient.List(ctx, req)
		for {
			instance, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("listing instances in zone %s: %w", zone, err)
			}
			instanceNames = append(instanceNames, instance.GetName())
		}
	}

	return instanceNames, nil
}

func (g *GCPShim) createNodes(requests []*NodesToRequest) error {
	panic("unimplemented")
}

func (g *GCPShim) getCurrentRequestedInstanceCount() (int, error) {
	panic("unimplemented")
}

func (g *GCPShim) getClusterConfig(projectID string, clusterID string) (Cluster, error) {
	panic("unimplemented")
}
