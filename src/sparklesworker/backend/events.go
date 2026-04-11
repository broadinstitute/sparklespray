package backend

import "time"

// Event type constants for the Type field.
const (
	// Task events (written by worker).
	EventTypeTaskClaimed  = "task_claimed"
	EventTypeTaskStaged   = "task_staged"
	EventTypeTaskComplete = "task_complete"
	EventTypeTaskOrphaned = "task_orphaned"
	EventTypeTaskStarted  = "task_started"
	EventTypeTaskExecuted = "task_executed"
	EventTypeTaskFailed   = "task_failed"

	// Worker events.
	EventTypeWorkersRequested      = "workers_requested"           // written by autoscaler
	EventTypeWorkerStarted         = "worker_started"              // written by worker
	EventTypeWorkerCompleted       = "worker_completed"            // written by worker
	EventTypeWorkerStoppedUnexpect = "worker_stopped_unexpectedly" // written by worker
	EventTypeWorkerRequestFailed   = "worker_request_failed"       // written by autoscaler
)

// Event is a unified record written to Firestore and published to Pub/Sub for
// every significant lifecycle transition. Rather than a separate Go type per
// event variant, all variants share this single struct. Fields that are not
// relevant to a given event type are left at their zero value.
//
// Field presence by Type:
//
//	 (written by worker)
//		task_claimed                — TaskID, JobID
//		task_staged                 — TaskID, JobID
//		task_complete               — TaskID, JobID
//		task_started                — TaskID, JobID
//		task_executed               — TaskID, JobID, ExitCode, MaxMemInGB
//		task_failed                 — TaskID, JobID, FailureReason
//		worker_started              — ClusterID, WorkerID
//		worker_completed            — ClusterID, WorkerID
//
//	 (written by autoscaler)
//		task_orphaned               — TaskID, JobID
//		workers_requested           — ClusterID, WorkerReqID, Count
//		worker_stopped_unexpectedly — ClusterID, WorkerID
//		worker_request_failed       — ClusterID, WorkerReqID

// The lifecycle of a task is:
// 0. Intially, all tasks have status "pending" until they are claimed
// by a worker.
// 1. task_claimed: A worker has claimed this task. The task's state has transitioned from "pending" to "claimed. The first step of the worker is first downloading any files which are needed by the task.
// 2. task_staged: The worker has finished downloading and staging
// any required files. The execution of the task then starts.
// 3. task_executed: Once execution of a task has completed, the
// worker starts uploading all new files.
// 4. task_completed: This event occurs after the upload completes and the task is assigned the terminal status of "completed"
//
// Now at any point after the task is claimed, it's possible for the
// worker which had claimed the task to be interrupted, resulting in
// a task_orphaned event which causes the tasks's status to be reset back to pending. It then restarts back at step 0 of the life cycle until the task is claimed again.
//
// Similarly, at any point after a task is claimed, a task_failed
// event might occur. At which time, the task transitions to the
// terminal "failed" state.

type Event struct {
	// Type identifies the event variant (one of the EventType* constants above).
	Type string `firestore:"type" json:"type"`

	// Timestamp is the RFC 3339 wall-clock time at which the event occurred.
	Timestamp string `firestore:"timestamp" json:"timestamp"`

	// Expiry is the RFC 3339 time after which this event record may be purged.
	Expiry string `firestore:"expiry" json:"expiry"`

	// TaskID is the identifier of the task this event relates to.
	// Present on all task_* event types.
	TaskID string `firestore:"task_id,omitempty" json:"task_id,omitempty"`

	// JobID is the identifier of the job that owns the task.
	// Present on all task_* event types.
	JobID string `firestore:"job_id,omitempty" json:"job_id,omitempty"`

	// ExitCode is the process exit code of the executed task.
	// Present on: task_executed.
	ExitCode string `firestore:"exit_code,omitempty" json:"exit_code,omitempty"`

	// MaxMemInGB is the peak resident-set memory used by the task, in gigabytes.
	// Present on: task_executed.
	MaxMemInGB float64 `firestore:"max_mem_in_gb,omitempty" json:"max_mem_in_gb,omitempty"`

	// FailureReason is a human-readable description of why the task failed.
	// Present on: task_failed, worker_request_failed.
	FailureReason string `firestore:"failure_reason,omitempty" json:"failure_reason,omitempty"`

	// ClusterID is the identifier of the cluster involved.
	// Present on all worker_* event types.
	ClusterID string `firestore:"cluster_id,omitempty" json:"cluster_id,omitempty"`

	// WorkerID is the identifier of the individual worker instance.
	// Present on: worker_started, worker_completed, worker_stopped_unexpectedly.
	WorkerID string `firestore:"worker_id,omitempty" json:"worker_id,omitempty"`

	// WorkerReqID is the identifier of the worker launch request issued by the autoscaler.
	// Present on: workers_requested, worker_request_failed.
	WorkerReqID string `firestore:"worker_req_id,omitempty" json:"worker_req_id,omitempty"`

	// Count is the number of workers requested in a single autoscaler request.
	// Present on: workers_requested.
	Count int `firestore:"count,omitempty" json:"count,omitempty"`
}

// NewTaskEvent returns an Event populated with the common task fields and the
// current time. Expiry is stamped by the EventPublisher before writing.
func NewTaskEvent(eventType, taskID, jobID string) Event {
	return Event{
		Type:      eventType,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		TaskID:    taskID,
		JobID:     jobID,
	}
}

// NewWorkerEvent returns an Event populated with the common worker fields and
// the current time. Expiry is stamped by the EventPublisher before writing.
func NewWorkerEvent(eventType, clusterID, workerID string) Event {
	return Event{
		Type:      eventType,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		ClusterID: clusterID,
		WorkerID:  workerID,
	}
}
