package backend

import (
	"context"

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
	GetActiveClusterIDs() ([]string, error)
}

type ExternalServices struct {
	Channel   ExtChannel
	NewQueue  func(clusterID string) task_queue.TaskQueue
	TaskCache task_queue.TaskCache
	Close     func()
	Gshim     CloudMethodsForPoll
	Sshim     SparklesMethodsForPoll
}

// ExtChannel is an abstract publish/subscribe mechanism. Implementations
// include PubSubChannel (Google Cloud Pub/Sub) and RedisChannel (Redis
// pub/sub), as well as NullChannel for testing/no-op use.
type ExtChannel interface {
	// Subscribe listens on topicName and calls callback for each received
	// message. It blocks until the context is cancelled or an error occurs.
	Subscribe(ctx context.Context, topicName string, callback func(message []byte)) error

	// Publish sends message to topicName.
	Publish(ctx context.Context, topicName string, message []byte) error
}
