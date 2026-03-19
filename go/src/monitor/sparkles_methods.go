package monitor

type SparklesMethodsForPoll interface {
	updateClusterMonitorState(clusterID string, state *MonitorState) error
	getNonCompleteTaskCount(clusterID string) (int, error)
	getClaimedTasks(clusterID string) ([]*Task, error)
	markTasksPending(tasks []*Task) error
	getClusterConfig(clusterID string) (Cluster, error)
	getPendingTaskCount(clusterID string) (int, error)
	getTasksCompletedBy(batchJobID string) int
}

type FirestoreSparklesMethodsForPoll struct {
}

func (s *FirestoreSparklesMethodsForPoll) updateClusterMonitorState(clusterID string, state *MonitorState) error {
	panic("unimplemented")
}

func (s *FirestoreSparklesMethodsForPoll) getNonCompleteTaskCount(clusterID string) (int, error) {
	panic("unimplemented")
}

func (s *FirestoreSparklesMethodsForPoll) getClaimedTasks(clusterID string) ([]*Task, error) {
	panic("unimplemented")
}

func (s *FirestoreSparklesMethodsForPoll) markTasksPending(tasks []*Task) error {
	panic("unimplemented")
}

func (s *FirestoreSparklesMethodsForPoll) getClusterConfig(clusterID string) (Cluster, error) {
	panic("unimplemented")
}

func (s *FirestoreSparklesMethodsForPoll) getPendingTaskCount(clusterID string) (int, error) {
	panic("unimplemented")
}

func (s *FirestoreSparklesMethodsForPoll) getTasksCompletedBy(batchJobID string) int {
	panic("unimplemented")
}
