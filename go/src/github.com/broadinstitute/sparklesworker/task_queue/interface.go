package task_queue

import (
	"context"
)

// Task represents a task in the queue
type Task struct {
	TaskID           string         `datastore:"task_id" json:"task_id"`
	TaskIndex        int64          `datastore:"task_index" json:"task_index"`
	JobID            string         `datastore:"job_id" json:"job_id"`
	Status           string         `datastore:"status" json:"status"`
	OwnedByWorkerID  string         `datastore:"owned_by_worker_id" json:"owned_by_worker_id"`
	Args             string         `datastore:"args" json:"args"`
	History          []*TaskHistory `datastore:"history" json:"history"`
	CommandResultURL string         `datastore:"command_result_url" json:"command_result_url"`
	FailureReason    string         `datastore:"failure_reason,omitempty" json:"failure_reason"`
	Version          int32          `datastore:"version" json:"version"`
	ExitCode         string         `datastore:"exit_code" json:"exit_code"`
	Cluster          string         `datastore:"cluster" json:"cluster"`
	MonitorAddress   string         `datastore:"monitor_address" json:"monitor_address"`
	LogURL           string         `datastore:"log_url" json:"log_url"`
	LastUpdated      float64        `datastore:"last_updated" json:"last_updated"`
}

// TaskHistory represents a history entry for a task
type TaskHistory struct {
	Timestamp       float64 `datastore:"timestamp,noindex"`
	Status          string  `datastore:"status,noindex"`
	FailureReason   string  `datastore:"failure_reason,noindex,omitempty"`
	OwnedByWorkerID string  `datastore:"owned_by_worker_id,noindex,omitempty"`
}

// Job represents a job containing multiple tasks
type Job struct {
	JobID                  int      `datastore:"job_id"`
	Tasks                  []string `datastore:"tasks"`
	KubeJobSpec            string   `datastore:"kube_job_spec"`
	Metadata               string   `datastore:"metadata"`
	Cluster                string   `datastore:"cluster"`
	Status                 string   `datastore:"status"`
	SubmitTime             float64  `datastore:"submit_time"`
	MaxPreemptableAttempts int32    `datastore:"max_preemptable_attempts"`
	TargetNodeCount        int32    `datastore:"target_node_count"`
}

// Status constants
const (
	StatusClaimed = "claimed"
	StatusPending = "pending"
	StatusComplete = "complete"
	StatusKilled  = "killed"
	StatusFailed  = "failed"
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
}
