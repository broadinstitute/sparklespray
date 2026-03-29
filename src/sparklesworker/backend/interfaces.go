package backend

import (
	"context"
	"errors"

	"github.com/broadinstitute/sparklesworker/task_queue"
)

// NoSuchBatchJob is returned by GetBatchJobByName when the named job does not exist.
var NoSuchBatchJob = errors.New("no such batch job")

type CloudMethodsForPoll interface {
	ListRunningInstances(clusterID string, region string) ([]string, error)
	ListBatchJobs(region, clusterID string) ([]*BatchJob, error)
	GetBatchJobByName(name string) (*BatchJob, error)
	PutSingletonBatchJob(name, region, machineType string, bootVolumeInGB int64, bootVolumeType, dockerImage string, cmd []string) error
	SubmitBatchJobs(CreateWorkerCommand CreateWorkerCommandCallback, cluster *Cluster, clusterID string, requests []*BatchJobsToSubmit) error
	DeleteAllBatchJobs(region, clusterID string) error
}

type SparklesMethodsForPoll interface {
	UpdateClusterMonitorState(clusterID string, state *MonitorState) error
	GetNonCompleteTaskCount(clusterID string) (int, error)
	GetClaimedTasks(clusterID string) ([]*task_queue.Task, error)
	MarkTasksPending(tasks []*task_queue.Task) error
	GetClusterConfig(clusterID string) (*Cluster, error)
	SetClusterConfig(clusterID string, cluster Cluster) error
	GetPendingTaskCount(clusterID string) (int, error)
	GetTasksCompletedBy(batchJobID string) int
	GetActiveClusterIDs() ([]string, error)
}

type CreateWorkerCommandCallback func(clusterID string, shouldLinger bool, aetherConfig *AetherConfig) []string

type ExternalServices struct {
	Channel             ExtChannel
	NewQueue            func(clusterID string) task_queue.TaskQueue
	TaskCache           task_queue.TaskCache
	Close               func()
	Gshim               CloudMethodsForPoll
	Sshim               SparklesMethodsForPoll
	CreateWorkerCommand CreateWorkerCommandCallback
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

type Disk struct {
	Name      string `json:"name"`
	SizeGB    int64  `json:"size_gb"`
	Type      string `json:"type"`
	MountPath string `json:"mount_path"`
}

type GCSBucketMount struct {
	Path         string   `json:"path"`
	RemotePath   string   `json:"remote_path"`
	MountOptions []string `json:"mount_options"`
}
