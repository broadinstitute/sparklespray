package backend

import (
	"github.com/broadinstitute/sparklesworker/ext_channel"
	"github.com/broadinstitute/sparklesworker/task_queue"
)

type CloudMethodsForPoll interface {
	ListRunningInstances(zones []string, clusterID string) ([]string, error)
	ListBatchJobs(region, clusterID string) ([]*BatchJob, error)
	SubmitBatchJobs(cluster *Cluster, clusterID string, requests []*BatchJobsToSubmit) error
	DeleteAllBatchJobs(region, clusterID string) error
}

type SparklesMethodsForPoll interface {
	UpdateClusterMonitorState(clusterID string, state *MonitorState) error
	GetNonCompleteTaskCount(clusterID string) (int, error)
	GetClaimedTasks(clusterID string) ([]*task_queue.Task, error)
	MarkTasksPending(tasks []*task_queue.Task) error
	GetClusterConfig(clusterID string) (*Cluster, error)
	GetPendingTaskCount(clusterID string) (int, error)
	GetTasksCompletedBy(batchJobID string) int
}

type ExternalServices struct {
	Channel ext_channel.ExtChannel
	Queue   task_queue.TaskQueue
	Close   func()
	Gshim   CloudMethodsForPoll
	Sshim   SparklesMethodsForPoll
}
