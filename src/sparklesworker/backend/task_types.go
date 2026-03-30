package backend

import "time"

// Collection name constants for Firestore.
const TaskCollection = "V7Task"
const ClusterCollection = "V7Cluster"
const JobCollection = "V7Job"
const CachedTaskEntryCollection = "V7CachedTaskEntry"

const InitialClaimRetryDelay = 1000

// Status constants for tasks and jobs.
const (
	StatusClaimed   = "claimed"
	StatusPending   = "pending"
	StatusComplete  = "complete"
	StatusKilled    = "killed"
	StatusFailed    = "failed"
	JobStatusKilled = "killed"
)

// UploadSpec describes which files to upload after task execution.
type UploadSpec struct {
	IncludePatterns []string `firestore:"include_patterns" json:"include_patterns"`
	ExcludePatterns []string `firestore:"exclude_patterns" json:"exclude_patterns"`
}

// TaskSpec describes the command and environment for a task.
type TaskSpec struct {
	WorkingDir         string            `firestore:"working_dir,omitempty" json:"working_dir,omitempty"`
	PreDownloadScript  string            `firestore:"pre_download_script,omitempty" json:"pre-download-script,omitempty"`
	PostDownloadScript string            `firestore:"post_download_script,omitempty" json:"post-download-script,omitempty"`
	PostExecScript     string            `firestore:"post_exec_script,omitempty" json:"post-exec-script,omitempty"`
	PreExecScript      string            `firestore:"pre_exec_script,omitempty" json:"pre-exec-script,omitempty"`
	Parameters         map[string]string `firestore:"parameters,omitempty" json:"parameters,omitempty"`
	Uploads            *UploadSpec       `firestore:"uploads" json:"uploads"`
	AetherFSRoot       string            `firestore:"aether_fs_root" json:"aether_fs_root"`
	Command            []string          `firestore:"command" json:"command"`
	DockerImage        string            `firestore:"docker_image,omitempty" json:"docker_image,omitempty"`
}

// TaskHistory records a single status transition for a task.
type TaskHistory struct {
	Timestamp       float64 `firestore:"timestamp"`
	Status          string  `firestore:"status"`
	FailureReason   string  `firestore:"failure_reason,omitempty"`
	OwnedByWorkerID string  `firestore:"owned_by_worker_id,omitempty"`
}

// Task represents a task in the queue.
type Task struct {
	TaskID                    string         `firestore:"task_id" json:"task_id"`
	TaskIndex                 int64          `firestore:"task_index" json:"task_index"`
	JobID                     string         `firestore:"job_id" json:"job_id"`
	Status                    string         `firestore:"status" json:"status"`
	OwnedByWorkerID           string         `firestore:"owned_by_worker_id" json:"owned_by_worker_id"`
	OwnedByBatchJobID         string         `firestore:"owned_by_batch_job_id" json:"owned_by_batch_job_id"`
	TaskSpec                  *TaskSpec      `firestore:"task_spec" json:"task_spec"`
	History                   []*TaskHistory `firestore:"history" json:"history"`
	FailureReason             string         `firestore:"failure_reason,omitempty" json:"failure_reason"`
	Version                   int32          `firestore:"version" json:"version"`
	ExitCode                  string         `firestore:"exit_code" json:"exit_code"`
	OutputAetherFSRoot        string         `firestore:"output_aether_fs_root" json:"output_aether_fs_root"`
	LogAetherFSRoot           string         `firestore:"log_aether_fs_root" json:"log_aether_fs_root"`
	ClusterID                 string         `firestore:"cluster_id" json:"cluster_id"`
	LastUpdated               float64        `firestore:"last_updated" json:"last_updated"`
	Expiry                    time.Time      `firestore:"expiry" json:"expiry"`
	UsedCacheResultFromTaskID string         `firestore:"used_cache_result_from_task_id,omitempty" json:"used_cache_result_from_task_id,omitempty"`
}

// Job represents a job containing multiple tasks.
type Job struct {
	Name       string    `firestore:"name"`
	TaskCount  int       `firestore:"task_count"`
	Metadata   string    `firestore:"metadata"`
	ClusterID  string    `firestore:"cluster_id"`
	Status     string    `firestore:"status"`
	SubmitTime float64   `firestore:"submit_time"`
	Expiry     time.Time `firestore:"expiry" json:"expiry"`
}

// CachedTaskEntry stores the output of a completed task keyed by its inputs,
// allowing future identical tasks to skip re-execution.
type CachedTaskEntry struct {
	ID                 string    `firestore:"id" json:"id"`
	TaskID             string    `firestore:"task_id" json:"task_id"`
	OutputAetherFSRoot string    `firestore:"output_aether_fs_root" json:"output_aether_fs_root"`
	LogAetherFSRoot    string    `firestore:"log_aether_fs_root" json:"log_aether_fs_root"`
	Expiry             time.Time `firestore:"expiry" json:"expiry"`
}
