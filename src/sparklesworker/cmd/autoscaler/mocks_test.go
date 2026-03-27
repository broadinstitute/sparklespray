package autoscaler

import (
	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/task_queue"
)

type mockCloud struct {
	listRunningInstancesFn func(zones []string, clusterID string) ([]string, error)
	listBatchJobsFn        func(region, clusterID string) ([]*backend.BatchJob, error)
	submitBatchJobsFn      func(cluster *backend.Cluster, clusterID string, requests []*backend.BatchJobsToSubmit) error
	deleteAllBatchJobsFn   func(region, clusterID string) error
}

func (m *mockCloud) ListRunningInstances(zones []string, clusterID string) ([]string, error) {
	return m.listRunningInstancesFn(zones, clusterID)
}

func (m *mockCloud) ListBatchJobs(region, clusterID string) ([]*backend.BatchJob, error) {
	return m.listBatchJobsFn(region, clusterID)
}

func (m *mockCloud) SubmitBatchJobs(cluster *backend.Cluster, clusterID string, requests []*backend.BatchJobsToSubmit) error {
	return m.submitBatchJobsFn(cluster, clusterID, requests)
}

func (m *mockCloud) DeleteAllBatchJobs(region, clusterID string) error {
	return m.deleteAllBatchJobsFn(region, clusterID)
}

type mockSparkles struct {
	clusterConfig        *backend.Cluster
	getClusterConfigErr  error
	pendingTaskCount     int
	nonCompleteTaskCount int
	claimedTasks         []*task_queue.Task
	tasksCompletedBy     map[string]int

	// captured calls
	savedState             *backend.MonitorState
	markTasksPendingCalled []*task_queue.Task
	submitBatchJobsCalled  []*backend.BatchJobsToSubmit
}

func (m *mockSparkles) UpdateClusterMonitorState(clusterID string, state *backend.MonitorState) error {
	m.savedState = state
	return nil
}

func (m *mockSparkles) GetNonCompleteTaskCount(clusterID string) (int, error) {
	return m.nonCompleteTaskCount, nil
}

func (m *mockSparkles) GetClaimedTasks(clusterID string) ([]*task_queue.Task, error) {
	return m.claimedTasks, nil
}

func (m *mockSparkles) MarkTasksPending(tasks []*task_queue.Task) error {
	m.markTasksPendingCalled = append(m.markTasksPendingCalled, tasks...)
	return nil
}

func (m *mockSparkles) GetClusterConfig(clusterID string) (*backend.Cluster, error) {
	return m.clusterConfig, m.getClusterConfigErr
}

func (m *mockSparkles) GetPendingTaskCount(clusterID string) (int, error) {
	return m.pendingTaskCount, nil
}

func (m *mockSparkles) GetTasksCompletedBy(batchJobID string) int {
	return m.tasksCompletedBy[batchJobID]
}

func (m *mockSparkles) GetActiveClusterIDs() ([]string, error) {
	return nil, nil
}

func (m *mockSparkles) SetClusterConfig(clusterID string, cluster backend.Cluster) error {
	return nil
}
