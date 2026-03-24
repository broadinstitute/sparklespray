package autoscaler

import "github.com/broadinstitute/sparklesworker/task_queue"

type mockCloud struct {
	listRunningInstancesFn func(zones []string, clusterID string) ([]string, error)
	listBatchJobsFn        func(region, clusterID string) ([]*BatchJob, error)
	submitBatchJobsFn         func(cluster Cluster, clusterID string, requests []*BatchJobsToSubmit) error
	deleteAllBatchJobsFn      func(region, clusterID string) error
}

func (m *mockCloud) listRunningInstances(zones []string, clusterID string) ([]string, error) {
	return m.listRunningInstancesFn(zones, clusterID)
}

func (m *mockCloud) listBatchJobs(region, clusterID string) ([]*BatchJob, error) {
	return m.listBatchJobsFn(region, clusterID)
}

func (m *mockCloud) submitBatchJobs(cluster Cluster, clusterID string, requests []*BatchJobsToSubmit) error {
	return m.submitBatchJobsFn(cluster, clusterID, requests)
}

func (m *mockCloud) deleteAllBatchJobs(region, clusterID string) error {
	return m.deleteAllBatchJobsFn(region, clusterID)
}

type mockSparkles struct {
	clusterConfig        Cluster
	getClusterConfigErr  error
	pendingTaskCount     int
	nonCompleteTaskCount int
	claimedTasks         []*task_queue.Task
	tasksCompletedBy     map[string]int

	// captured calls
	savedState             *MonitorState
	markTasksPendingCalled []*task_queue.Task
	submitBatchJobsCalled  []*BatchJobsToSubmit
}

func (m *mockSparkles) updateClusterMonitorState(clusterID string, state *MonitorState) error {
	m.savedState = state
	return nil
}

func (m *mockSparkles) getNonCompleteTaskCount(clusterID string) (int, error) {
	return m.nonCompleteTaskCount, nil
}

func (m *mockSparkles) getClaimedTasks(clusterID string) ([]*task_queue.Task, error) {
	return m.claimedTasks, nil
}

func (m *mockSparkles) markTasksPending(tasks []*task_queue.Task) error {
	m.markTasksPendingCalled = append(m.markTasksPendingCalled, tasks...)
	return nil
}

func (m *mockSparkles) getClusterConfig(clusterID string) (Cluster, error) {
	return m.clusterConfig, m.getClusterConfigErr
}

func (m *mockSparkles) getPendingTaskCount(clusterID string) (int, error) {
	return m.pendingTaskCount, nil
}

func (m *mockSparkles) getTasksCompletedBy(batchJobID string) int {
	return m.tasksCompletedBy[batchJobID]
}
