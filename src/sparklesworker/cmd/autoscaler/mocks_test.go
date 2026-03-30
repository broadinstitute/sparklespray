package autoscaler

import (
	"context"

	"github.com/broadinstitute/sparklesworker/backend"
)

type mockCloud struct {
	listRunningInstancesFn    func(clusterID string, region string) ([]string, error)
	listBatchJobsFn           func(region, clusterID string) ([]*backend.BatchJob, error)
	getBatchJobByNameFn       func(name string) (*backend.BatchJob, error)
	putSingletonBatchJobFn    func(name, region, machineType string, bootVolumeInGB int64, bootVolumeType, dockerImage string, cmd []string) error
	submitBatchJobsFn         func(callback backend.CreateWorkerCommandCallback, cluster *backend.Cluster, clusterID string, requests []*backend.BatchJobsToSubmit) error
	deleteAllBatchJobsFn      func(region, clusterID string) error
}

func (m *mockCloud) ListRunningInstances(clusterID string, region string) ([]string, error) {
	return m.listRunningInstancesFn(clusterID, region)
}

func (m *mockCloud) ListBatchJobs(region, clusterID string) ([]*backend.BatchJob, error) {
	return m.listBatchJobsFn(region, clusterID)
}

func (m *mockCloud) GetBatchJobByName(name string) (*backend.BatchJob, error) {
	return m.getBatchJobByNameFn(name)
}

func (m *mockCloud) PutSingletonBatchJob(name, region, machineType string, bootVolumeInGB int64, bootVolumeType, dockerImage string, cmd []string) error {
	return m.putSingletonBatchJobFn(name, region, machineType, bootVolumeInGB, bootVolumeType, dockerImage, cmd)
}

func (m *mockCloud) SubmitBatchJobs(callback backend.CreateWorkerCommandCallback, cluster *backend.Cluster, clusterID string, requests []*backend.BatchJobsToSubmit) error {
	return m.submitBatchJobsFn(callback, cluster, clusterID, requests)
}

func (m *mockCloud) DeleteAllBatchJobs(region, clusterID string) error {
	return m.deleteAllBatchJobsFn(region, clusterID)
}

// mockSparkles implements both ClusterStore and TaskStore.
type mockSparkles struct {
	clusterConfig        *backend.Cluster
	getClusterConfigErr  error
	pendingTaskCount     int
	nonCompleteTaskCount int
	claimedTasks         []*backend.Task
	tasksCompletedBy     map[string]int

	// captured calls
	savedState             *backend.MonitorState
	markTasksPendingCalled []*backend.Task
	submitBatchJobsCalled  []*backend.BatchJobsToSubmit
}

// ---- ClusterStore methods ----

func (m *mockSparkles) UpdateClusterMonitorState(clusterID string, state *backend.MonitorState) error {
	m.savedState = state
	return nil
}

func (m *mockSparkles) GetClusterConfig(clusterID string) (*backend.Cluster, error) {
	return m.clusterConfig, m.getClusterConfigErr
}

func (m *mockSparkles) SetClusterConfig(clusterID string, cluster backend.Cluster) error {
	return nil
}

// ---- TaskStore methods ----

func (m *mockSparkles) GetClusterIDsFromActiveTasks() ([]string, error) {
	return nil, nil
}

func (m *mockSparkles) GetNonCompleteTaskCount(clusterID string) (int, error) {
	return m.nonCompleteTaskCount, nil
}

func (m *mockSparkles) GetClaimedTasks(clusterID string) ([]*backend.Task, error) {
	return m.claimedTasks, nil
}

func (m *mockSparkles) MarkTasksPending(tasks []*backend.Task) error {
	m.markTasksPendingCalled = append(m.markTasksPendingCalled, tasks...)
	return nil
}

func (m *mockSparkles) GetPendingTaskCount(clusterID string) (int, error) {
	return m.pendingTaskCount, nil
}

func (m *mockSparkles) GetTasksCompletedBy(batchJobID string) int {
	return m.tasksCompletedBy[batchJobID]
}

func (m *mockSparkles) ClaimTask(ctx context.Context, clusterID string) (*backend.Task, error) {
	return nil, nil
}

func (m *mockSparkles) IsJobKilled(ctx context.Context, jobID string) (bool, error) {
	return false, nil
}

func (m *mockSparkles) AtomicUpdateTask(ctx context.Context, taskID string, mutateTaskCallback func(task *backend.Task) bool) (*backend.Task, error) {
	return nil, nil
}

func (m *mockSparkles) GetTask(ctx context.Context, taskID string) (*backend.Task, error) {
	return nil, nil
}

func (m *mockSparkles) AddJob(ctx context.Context, job *backend.Job, tasks []*backend.Task) error {
	return nil
}
