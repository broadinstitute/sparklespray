package backend

import (
	"context"
	"errors"
)

// NoSuchBatchJob is returned by GetBatchJobByName when the named job does not exist.
var NoSuchBatchJob = errors.New("no such batch job")

// WorkerPool manages compute resources (batch jobs and VM instances).
type WorkerPool interface {
	ListRunningInstances(clusterID string, region string) ([]string, error)
	ListBatchJobs(region, clusterID string) ([]*BatchJob, error)
	GetBatchJobByName(name string) (*BatchJob, error)
	PutSingletonBatchJob(name, region, machineType string, bootVolumeInGB int64, bootVolumeType, dockerImage string, cmd []string) error
	SubmitBatchJobs(CreateWorkerCommand CreateWorkerCommandCallback, cluster *Cluster, clusterID string, requests []*BatchJobsToSubmit) error
	DeleteAllBatchJobs(region, clusterID string) error
	DeleteBatchJob(jobID string) error
}

// ClusterStore manages cluster configuration and monitor state.
type ClusterStore interface {
	UpdateClusterMonitorState(clusterID string, state *MonitorState) error
	GetClusterConfig(clusterID string) (*Cluster, error)
	SetClusterConfig(clusterID string, cluster Cluster) error
}

// TaskStore manages task and job state. It covers both per-cluster worker
// operations (ClaimTask, AtomicUpdateTask, etc.) and cross-cluster monitor
// queries (GetClaimedTasks, GetNonCompleteTaskCount, etc.).
type TaskStore interface {
	// Cross-cluster query methods (clusterID passed as parameter)
	GetClusterIDsFromActiveTasks() ([]string, error)
	GetNonCompleteTaskCount(clusterID string) (int, error)
	GetClaimedTasks(clusterID string) ([]*Task, error)
	MarkTasksPending(tasks []*Task) error
	GetPendingTaskCount(clusterID string) (int, error)
	GetTasksCompletedBy(batchJobID string) int

	// Per-cluster worker methods
	ClaimTask(ctx context.Context, clusterID string) (*Task, error)
	IsJobKilled(ctx context.Context, jobID string) (bool, error)
	AtomicUpdateTask(ctx context.Context, taskID string, mutateTaskCallback func(task *Task) bool) (*Task, error)
	GetTask(ctx context.Context, taskID string) (*Task, error)
	AddJob(ctx context.Context, job *Job, tasks []*Task) error
}

// TaskCache stores and retrieves cached task execution results.
type TaskCache interface {
	GetCachedEntry(ctx context.Context, cacheKey string) (*CachedTaskEntry, error)
	SetCachedEntry(ctx context.Context, entry *CachedTaskEntry) error
}

type CreateWorkerCommandCallback func(cluster *Cluster, shouldLinger bool) []string

func (e *ExternalServices) DeleteBatchJob(jobID string) error {
	return e.Compute.DeleteBatchJob(jobID)
}

// ExternalServices bundles all backend service handles for a deployment
// (either GCP production or local Redis testing).
type ExternalServices struct {
	Channel   MessageBus
	TaskCache TaskCache
	Close               func()
	Compute             WorkerPool
	Cluster             ClusterStore
	Tasks               TaskStore
	CreateWorkerCommand CreateWorkerCommandCallback
}

// MessageBus is an abstract publish/subscribe mechanism. Implementations
// include PubSubChannel (Google Cloud Pub/Sub) and RedisChannel (Redis
// pub/sub), as well as NullChannel for testing/no-op use.
type MessageBus interface {
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
