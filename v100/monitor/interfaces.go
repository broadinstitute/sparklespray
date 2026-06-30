package monitor

import (
	"context"
	"time"
)

// ----- Status types -----

// WorkPoolStatus is the operational state of a workpool.
type WorkPoolStatus string

const (
	WorkPoolStatusIdle      WorkPoolStatus = "idle"
	WorkPoolStatusOK        WorkPoolStatus = "ok"
	WorkPoolStatusUnhealthy WorkPoolStatus = "unhealthy"
	WorkPoolStatusHalted    WorkPoolStatus = "halted"
)

// BatchStatus is the lifecycle state of a BatchAPIRequest.
type BatchStatus string

const (
	BatchStatusPending   BatchStatus = "pending"
	BatchStatusStarted   BatchStatus = "started"
	BatchStatusFailed    BatchStatus = "failed"
	BatchStatusCompleted BatchStatus = "completed"
	// BatchStatusDeleted marks a batch whose GCP Batch job no longer exists (404 from the API).
	BatchStatusDeleted BatchStatus = "deleted"
)

// BatchJobStatus is the status returned by the GCP Batch API.
type BatchJobStatus string

const (
	BatchJobStatusQueued    BatchJobStatus = "QUEUED"
	BatchJobStatusScheduled BatchJobStatus = "SCHEDULED"
	BatchJobStatusRunning   BatchJobStatus = "RUNNING"
	BatchJobStatusSucceeded BatchJobStatus = "SUCCEEDED"
	BatchJobStatusFailed    BatchJobStatus = "FAILED"
	// BatchJobStatusDeleted is a synthetic status returned when the GCP Batch API responds
	// with 404 — the job no longer exists (e.g. it was manually deleted or expired).
	BatchJobStatusDeleted BatchJobStatus = "DELETED"
)

// TaskStatus mirrors the active task states from datamodel.md.
type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusClaimed TaskStatus = "claimed"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusWriting TaskStatus = "writing"
)

// ----- Data model -----

// WorkPool holds the configuration of a workpool. It corresponds to the immutable
// WorkPools Firestore collection (written once at creation, never updated).
// The mutable runtime state (State, StateMessage, LastIncidentAt, IncidentCount)
// is loaded from WorkPoolSummary and kept here for in-memory convenience during
// a monitor poll cycle; saves go to WorkPoolSummary, not WorkPools.
type WorkPool struct {
	WorkpoolID            string
	Region                string   // GCP region for Batch jobs, e.g. "us-central1"
	Zones                 []string // GCP zones to query for running VMs, e.g. ["us-central1-a", "us-central1-b"]
	MachineType           string   // GCP machine type, e.g. "n1-standard-4"
	RootDir               string
	SparklesWorkerGCSPath string
	EmptyVolumes          []EmptyVolume
	Resources             []ResourceEntry
	ServiceAccount        string

	// Provisioning parameters
	MaxWorkerCount               int
	MaxPreemptibleWorkerAttempts int
	MaxWorkersPerRequest         int // default: 100

	// Watchdog parameters
	VMShutdownGracePeriod       time.Duration // default: 1min
	MaxZombiesBeforeAbort       int           // default: 3
	MaxConsecutiveFailedBatches int           // default: 2

	// State fields — immutable in WorkPools Firestore; loaded from WorkPoolSummary
	// at read time and persisted back to WorkPoolSummary on Save.
	State         WorkPoolStatus
	StateMessage  string
	LastIncidentAt time.Time
	IncidentCount  int
}

// BatchAPIRequest corresponds to the BatchAPIRequest Firestore collection.
type BatchAPIRequest struct {
	BatchID               string
	JobID                 string
	WorkpoolID            string
	ExpectedVMCount       int
	Preemptible           bool
	SubmittedAt           time.Time
	RunningSince          *time.Time // nil until the job first reaches RUNNING
	RegisteredWorkerCount int        // monotonic; incremented at worker registration, never decremented
	Status                BatchStatus
	Unhealthy             bool // sticky; never cleared; independent of Status
}

// Worker is the subset of the Workers Firestore document needed by the monitor.
type Worker struct {
	WorkerID        string
	WorkpoolID      string
	BatchID         string    // which batch submitted this worker
	InstanceName    string    // GCP instance name recorded at startup; enables surgical VM termination
	Status          string    // "started" or "stopped"
	HeartbeatExpiry time.Time // rolling deadline; used to detect crashed/preempted workers
}

// Task is the subset of the Tasks Firestore document needed by the monitor.
type Task struct {
	TaskID         string
	JobID          string
	WorkpoolID     string
	Status         TaskStatus
	OwningWorkerID string
}

// VMInfo holds information about a running GCP VM.
type VMInfo struct {
	InstanceName string
	Zone         string
}

// ResourceEntry is a named float resource requirement. Field names and tags match
// v100.ResourceEntry so Firestore documents round-trip correctly.
type ResourceEntry struct {
	Name  string  `firestore:"name"  json:"name"`
	Value float64 `firestore:"value" json:"value"`
}

// EmptyVolume describes a new scratch disk to attach and mount on each worker VM.
// Field names and tags match v100.EmptyVolume so Firestore documents round-trip correctly.
type EmptyVolume struct {
	MountPoint string `firestore:"mount_point" json:"mountPoint"`
	Type       string `firestore:"type"        json:"type"`
	SizeInGB   int    `firestore:"size_in_gb"  json:"sizeInGB"`
}

// WorkerJobSpec holds all parameters needed to create a GCP Batch job for workers.
type WorkerJobSpec struct {
	WorkpoolID            string
	BatchID               string
	Region                string
	MachineType           string
	VMCount               int
	Preemptible           bool
	SparklesWorkerGCSPath string
	Command               string
	RootDir               string
	EmptyVolumes          []EmptyVolume
	ServiceAccount        string
	DBName                string
	Resources             []ResourceEntry
	LingerTime            time.Duration
}

// ----- External service interfaces -----

// BatchAPIClient wraps the GCP Batch API. All methods receive a context for cancellation.
//
// ListRunningVMs filters by a single GCE label. Pass filterLabelName as either
// "sparkles-worker-batch" (to scope to one batch's VMs) or "sparkles-worker-workpool"
// (to scope to all VMs across a workpool).
type BatchAPIClient interface {
	CreateJob(ctx context.Context, spec *WorkerJobSpec) (jobID string, err error)
	GetJobStatus(ctx context.Context, jobID string) (BatchJobStatus, error)
	ListRunningVMs(ctx context.Context, filterLabelName, filterLabelValue string, zones []string) (map[string]VMInfo, error)
	TerminateVM(ctx context.Context, zone, instanceName string) error
	TerminateJob(ctx context.Context, jobID string) error
	PrintBatchDebuggingInfo(ctx context.Context, jobID string) error
}

// WorkPoolStore reads and writes WorkPool documents.
type WorkPoolStore interface {
	ListAll(ctx context.Context) ([]*WorkPool, error)
	Get(ctx context.Context, workpoolID string) (*WorkPool, error)
	Save(ctx context.Context, pool *WorkPool) error
}

// BatchRequestStore reads and writes BatchAPIRequest documents.
type BatchRequestStore interface {
	Create(ctx context.Context, batch *BatchAPIRequest) error
	Get(ctx context.Context, batchID string) (*BatchAPIRequest, error)
	Save(ctx context.Context, batch *BatchAPIRequest) error
	// GetByJobID returns the BatchAPIRequest whose JobID matches the GCP job name.
	// Returns nil, nil if not found.
	GetByJobID(ctx context.Context, jobID string) (*BatchAPIRequest, error)
	// ListByWorkpool returns batches filtered by status, ordered by SubmittedAt DESC.
	ListByWorkpool(ctx context.Context, workpoolID string, statuses []BatchStatus) ([]*BatchAPIRequest, error)
	// ListAllByWorkpool returns all BatchAPIRequests for a workpool regardless of status.
	ListAllByWorkpool(ctx context.Context, workpoolID string) ([]*BatchAPIRequest, error)
	// SumPreemptibleVMCount returns total ExpectedVMCount across all preemptible batches for the workpool.
	SumPreemptibleVMCount(ctx context.Context, workpoolID string) (int, error)
}

// WorkerStore reads Worker documents.
type WorkerStore interface {
	// ListExpired returns workers whose HeartbeatExpiry is before now.
	ListExpired(ctx context.Context, now time.Time) ([]*Worker, error)
	// ListByBatch returns all workers registered for a given batch.
	ListByBatch(ctx context.Context, batchID string) ([]*Worker, error)
	// CountActive returns workers with HeartbeatExpiry after now.
	CountActive(ctx context.Context, workpoolID string, now time.Time) (int, error)
	// ListAllForWorkpool returns all Workers registered for the given workpool.
	ListAllForWorkpool(ctx context.Context, workpoolID string) ([]*Worker, error)
}

// TaskStore reads and updates Task documents.
type TaskStore interface {
	// ListByWorker returns tasks owned by a worker that are in one of the given statuses.
	ListByWorker(ctx context.Context, workerID string, statuses []TaskStatus) ([]*Task, error)
	// CountPending returns the number of pending tasks for a workpool.
	CountPending(ctx context.Context, workpoolID string) (int, error)
	// ResetToPending sets the task to pending and clears OwningWorkerID.
	ResetToPending(ctx context.Context, taskID string) error
	// CountByJob returns a map from task status string to count for the given job.
	CountByJob(ctx context.Context, jobID string) (map[string]int, error)
	// CountByWorkpool returns a map from task status string to count for the given workpool.
	CountByWorkpool(ctx context.Context, workpoolID string) (map[string]int, error)
}

// ----- WorkPool summary types -----

// StateCount is one entry in a per-state breakdown (used in both WorkPoolSummary and JobSummary).
type StateCount struct {
	State string `firestore:"state" json:"state"`
	Count int    `firestore:"count"  json:"count"`
}

// WorkPoolSummary is the mutable counterpart to the immutable WorkPool document.
// It is owned exclusively by the monitor process, which recomputes it on each
// provisioning poll. It contains a copy of all WorkPool fields (so callers can
// retrieve full workpool information without fetching both documents) plus all
// mutable state written by the monitor.
type WorkPoolSummary struct {
	// Fields copied from WorkPool
	WorkpoolID                    string  `firestore:"workpool_id"`
	MachineType                   string  `firestore:"machine_type"`
	Labels                        []Label `firestore:"labels"`
	MaxPreemptibleWorkerAttempts  int     `firestore:"max_preemptible_worker_attempts"`

	// Monitor-maintained fields
	Expiry                        time.Time      `firestore:"expiry"`
	LastUpdated                   time.Time      `firestore:"last_updated"`
	State                         WorkPoolStatus `firestore:"state"`
	StateMessage                  string         `firestore:"state_message"`
	LastIncidentAt                time.Time      `firestore:"last_incident_at"`
	IncidentCount                 int            `firestore:"incident_count"`
	ExpectedPreemptibleWorkers    int            `firestore:"expected_preemptible_workers"`
	ExpectedNonpreemptibleWorkers int            `firestore:"expected_nonpreemptible_workers"`
	UnhealthyBatchCount           int            `firestore:"unhealthy_batch_count"`
	BatchAPIRequestCounts         []StateCount   `firestore:"batch_api_request_counts"`
	PreemptibleWorkers            []StateCount   `firestore:"preemptible_workers"`
	NonpreemptibleWorkers         []StateCount   `firestore:"nonpreemptible_workers"`
	Tasks                         []StateCount   `firestore:"tasks"`
}

// WorkPoolSummaryHistory is an append-only snapshot written each time the monitor
// updates a WorkPoolSummary.
type WorkPoolSummaryHistory struct {
	WorkpoolID                    string        `firestore:"workpool_id"`
	Timestamp                     time.Time     `firestore:"timestamp"`
	Expiry                        time.Time     `firestore:"expiry"`
	State                         WorkPoolStatus `firestore:"state"`
	StateMessage                  string         `firestore:"state_message"`
	LastIncidentAt                time.Time      `firestore:"last_incident_at"`
	IncidentCount                 int            `firestore:"incident_count"`
	ExpectedPreemptibleWorkers    int            `firestore:"expected_preemptible_workers"`
	ExpectedNonpreemptibleWorkers int            `firestore:"expected_nonpreemptible_workers"`
	UnhealthyBatchCount           int            `firestore:"unhealthy_batch_count"`
	BatchAPIRequestCounts         []StateCount   `firestore:"batch_api_request_counts"`
	PreemptibleWorkers            []StateCount   `firestore:"preemptible_workers"`
	NonpreemptibleWorkers         []StateCount   `firestore:"nonpreemptible_workers"`
	Tasks                         []StateCount   `firestore:"tasks"`
}

// WorkPoolSummaryStore reads and writes WorkPoolSummary and WorkPoolSummaryHistory documents.
type WorkPoolSummaryStore interface {
	Save(ctx context.Context, summary *WorkPoolSummary) error
	SaveHistory(ctx context.Context, history *WorkPoolSummaryHistory) error
}

// ----- Job summary types -----

// JobStatus is the rolled-up lifecycle state of a job.
type JobStatus string

const (
	JobStatusPending               JobStatus = "pending"
	JobStatusInProgress            JobStatus = "in_progress"
	JobStatusInProgressWithError   JobStatus = "in_progress_with_error"
	JobStatusInProgressWithFailure JobStatus = "in_progress_with_failure"
	JobStatusKilled                JobStatus = "killed"
	JobStatusSuccess               JobStatus = "success"
	JobStatusError                 JobStatus = "error"
	JobStatusFailed                JobStatus = "failed"
)

// IsTerminalJobStatus reports whether s is a terminal job status.
func IsTerminalJobStatus(s JobStatus) bool {
	switch s {
	case JobStatusSuccess, JobStatusError, JobStatusFailed, JobStatusKilled:
		return true
	}
	return false
}

// Label is a user-defined key/value tag attached to a job.
type Label struct {
	Name  string `firestore:"name"  json:"name"`
	Value string `firestore:"value" json:"value"`
}

// JobSummary is created at job submission (state=pending) and updated by the
// monitor's job-summary poll as task states change.
type JobSummary struct {
	JobID      string       `firestore:"job_id"`
	WorkpoolID string       `firestore:"workpool_id"`
	CreatedAt  time.Time    `firestore:"created_at"`
	Expiry     time.Time    `firestore:"expiry"`
	State      JobStatus    `firestore:"state"`
	Tasks      []StateCount `firestore:"tasks"`
	Labels     []Label      `firestore:"labels"`
}

// JobSummaryHistory is an append-only snapshot written each time the monitor
// updates a JobSummary.
type JobSummaryHistory struct {
	JobID      string       `firestore:"job_id"`
	WorkpoolID string       `firestore:"workpool_id"`
	CreatedAt  time.Time    `firestore:"created_at"`
	Timestamp  time.Time    `firestore:"timestamp"`
	Expiry     time.Time    `firestore:"expiry"`
	State      JobStatus    `firestore:"state"`
	Tasks      []StateCount `firestore:"tasks"`
	Labels     []Label      `firestore:"labels"`
}

// JobSummaryStore reads and writes JobSummary and JobSummaryHistory documents.
type JobSummaryStore interface {
	// Create writes the initial JobSummary for a new job.
	Create(ctx context.Context, summary *JobSummary) error
	// ListNonTerminal returns all JobSummary documents whose state is not terminal.
	ListNonTerminal(ctx context.Context) ([]*JobSummary, error)
	// Save replaces an existing JobSummary document.
	Save(ctx context.Context, summary *JobSummary) error
	// SaveHistory appends a snapshot to the JobSummaryHistory collection.
	SaveHistory(ctx context.Context, history *JobSummaryHistory) error
}

// JobTerminatedPublisher emits a job_terminated event when a job reaches a
// terminal state. Defined here (not in v100) to avoid an import cycle.
type JobTerminatedPublisher interface {
	PublishJobTerminated(ctx context.Context, jobID, workpoolID string) error
}

// Notification is delivered on the channel returned by PubSubReceiver.Notifications.
// Exactly one of BatchID or Err is set: Err is non-nil when the receive loop fails fatally.
type Notification struct {
	BatchID string
	Err     error
}

// JobNotification is delivered on the channel returned by JobEventReceiver.JobEvents.
// Exactly one of JobID or Err is set.
type JobNotification struct {
	EventType  string // "job_created" or "task_state_update"
	JobID      string
	WorkpoolID string
	Err        error
}

// JobEventReceiver signals when a new job has been submitted.
type JobEventReceiver interface {
	JobEvents() <-chan JobNotification
}

// PubSubReceiver delivers Batch API status-change notifications.
// Each notification carries the BatchID of the batch that changed state,
// or a fatal Err that the monitor should propagate.
type PubSubReceiver interface {
	Notifications() <-chan Notification
}

// ExpiryStore deletes documents whose expiry timestamp is before now.
type ExpiryStore interface {
	// DeleteExpired removes all documents in the named collection with expiry < now.
	// Returns the count of deleted documents.
	DeleteExpired(ctx context.Context, collection string, now time.Time) (int, error)
}
