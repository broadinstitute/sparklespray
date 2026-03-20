package task_queue

import (
	"context"
)

type UploadSpec struct {
	IncludePatterns []string `firestore:"include_patterns" json:"include_patterns"`
	ExcludePatterns []string `firestore:"exclude_patterns" json:"exclude_patterns"`
}

type TaskSpec struct {
	WorkingDir         string            `firestore:"working_dir,omitempty" json:"working_dir,omitempty"`
	PreDownloadScript  string            `firestore:"pre_download_script,omitempty" json:"pre-download-script,omitempty"`
	PostDownloadScript string            `firestore:"post_download_script,omitempty" json:"post-download-script,omitempty"`
	PostExecScript     string            `firestore:"post_exec_script,omitempty" json:"post-exec-script,omitempty"`
	PreExecScript      string            `firestore:"pre_exec_script,omitempty" json:"pre-exec-script,omitempty"`
	Parameters         map[string]string `firestore:"parameters,omitempty" json:"parameters,omitempty"`
	Uploads            *UploadSpec       `firestore:"uploads" json:"uploads"`
	AetherFSRoot       string            `firestore:"aether_fs_root" json:"aether_fs_root"`
	Command            string            `firestore:"command" json:"command"`
	CommandResultURL   string            `firestore:"command_result_url" json:"command_result_url"`
	StdoutURL          string            `firestore:"stdout_url" json:"stdout_url"`
	DockerImage        string            `firestore:"docker_image,omitempty" json:"docker_image,omitempty"`
}

// Task represents a task in the queue
type Task struct {
	TaskID          string         `firestore:"task_id" json:"task_id"`
	TaskIndex       int64          `firestore:"task_index" json:"task_index"`
	JobID           string         `firestore:"job_id" json:"job_id"`
	Status          string         `firestore:"status" json:"status"`
	OwnedByWorkerID string         `firestore:"owned_by_worker_id" json:"owned_by_worker_id"`
	TaskSpec        *TaskSpec      `firestore:"task_spec" json:"task_spec"`
	History         []*TaskHistory `firestore:"history" json:"history"`
	FailureReason   string         `firestore:"failure_reason,omitempty" json:"failure_reason"`
	Version         int32          `firestore:"version" json:"version"`
	ExitCode        string         `firestore:"exit_code" json:"exit_code"`
	Cluster         string         `firestore:"cluster" json:"cluster"`
	MonitorAddress  string         `firestore:"monitor_address" json:"monitor_address"`
	LogURL          string         `firestore:"log_url" json:"log_url"`
	LastUpdated     float64        `firestore:"last_updated" json:"last_updated"`
}

// TaskHistory represents a history entry for a task
type TaskHistory struct {
	Timestamp       float64 `firestore:"timestamp"`
	Status          string  `firestore:"status"`
	FailureReason   string  `firestore:"failure_reason,omitempty"`
	OwnedByWorkerID string  `firestore:"owned_by_worker_id,omitempty"`
}

// Job represents a job containing multiple tasks
type Job struct {
	JobID                  int      `firestore:"job_id"`
	Tasks                  []string `firestore:"tasks"`
	KubeJobSpec            string   `firestore:"kube_job_spec"`
	Metadata               string   `firestore:"metadata"`
	Cluster                string   `firestore:"cluster"`
	Status                 string   `firestore:"status"`
	SubmitTime             float64  `firestore:"submit_time"`
	MaxPreemptableAttempts int32    `firestore:"max_preemptable_attempts"`
	TargetNodeCount        int32    `firestore:"target_node_count"`
}

// Status constants
const (
	StatusClaimed   = "claimed"
	StatusPending   = "pending"
	StatusComplete  = "complete"
	StatusKilled    = "killed"
	StatusFailed    = "failed"
	JobStatusKilled = "killed"
)

// TaskQueue defines the interface for task queue operations
type TaskQueue interface {
	// ClaimTask attempts to claim a pending task from the queue.
	// Returns nil if no tasks are available.
	ClaimTask(ctx context.Context) (*Task, error)

	// IsJobKilled checks if the job has been killed.
	IsJobKilled(ctx context.Context, jobID string) (bool, error)

	// AtomicUpdateTask updates a task atomically using the provided callback.
	// The callback should return true if the update should proceed.
	AtomicUpdateTask(ctx context.Context, taskID string, mutateTaskCallback func(task *Task) bool) (*Task, error)

	// AddTasks inserts tasks into the queue.
	AddTasks(ctx context.Context, tasks []*Task) error
}
