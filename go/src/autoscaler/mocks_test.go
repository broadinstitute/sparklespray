package monitor

type mockCloud struct {
	listRunningInstancesFn func(zones []string, clusterID string) ([]string, error)
	countRequestedInstancesFn     func(region string, clusterID string) (int, error)
	listBatchJobsFn        func(clusterID string) ([]*BatchJob, error)
	submitBatchJobsFn      func(requests []*BatchJobsToSubmit) error
	deleteAllBatchJobsFn   func(clusterID string) error
}

func (m *mockCloud) listRunningInstances(zones []string, clusterID string) ([]string, error) {
	return m.listRunningInstancesFn(zones, clusterID)
}

func (m *mockCloud) countRequestedInstances(region string, clusterID string) (int, error) {
	return m.countRequestedInstancesFn(region, clusterID)
}

func (m *mockCloud) listBatchJobs(clusterID string) ([]*BatchJob, error) {
	return m.listBatchJobsFn(clusterID)
}

func (m *mockCloud) submitBatchJobs(requests []*BatchJobsToSubmit) error {
	return m.submitBatchJobsFn(requests)
}

func (m *mockCloud) deleteAllBatchJobs(clusterID string) error {
	return m.deleteAllBatchJobsFn(clusterID)
}

type mockSparkles struct {
	clusterConfig        Cluster
	getClusterConfigErr  error
	pendingTaskCount     int
	nonCompleteTaskCount int
	claimedTasks         []*Task
	tasksCompletedBy     map[string]int

	// captured calls
	savedState             *MonitorState
	markTasksPendingCalled []*Task
	submitBatchJobsCalled  []*BatchJobsToSubmit
}

func (m *mockSparkles) updateClusterMonitorState(clusterID string, state *MonitorState) error {
	m.savedState = state
	return nil
}

func (m *mockSparkles) getNonCompleteTaskCount(clusterID string) (int, error) {
	return m.nonCompleteTaskCount, nil
}

func (m *mockSparkles) getClaimedTasks(clusterID string) ([]*Task, error) {
	return m.claimedTasks, nil
}

func (m *mockSparkles) markTasksPending(tasks []*Task) error {
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
