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
	BatchStatusPending BatchStatus = "pending"
	BatchStatusStarted BatchStatus = "started"
	// BatchStatusFailed marks a batch that GCP itself reported as failed.
	BatchStatusFailed    BatchStatus = "failed"
	BatchStatusCompleted BatchStatus = "completed"
	// BatchStatusDeleted marks a batch whose GCP Batch job no longer exists (404 from the API).
	BatchStatusDeleted BatchStatus = "deleted"
	// BatchStatusTerminated marks a batch the monitor itself decided to kill —
	// GCP hadn't reported any problem with the job; the monitor's own
	// bookkeeping (VM counts, worker registrations, heartbeats) found an
	// anomaly instead. See BatchAPIRequest.TerminationReason.
	BatchStatusTerminated BatchStatus = "terminated"
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

// TaskStatus mirrors the active task states from docs/design/datamodel.md.
type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusClaimed TaskStatus = "claimed"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusWriting TaskStatus = "writing"
)

// ----- Data model -----

// WorkPool holds the immutable configuration of a workpool. It corresponds to the
// WorkPools Firestore collection (written once at creation, never updated).
// Mutable runtime state lives in WorkPoolState, which is stored in WorkPoolSummary.
type WorkPool struct {
	WorkpoolID string
	// ProjectID, if set, is the GCP project this workpool's Batch jobs (and
	// therefore its worker VMs) are created in. Empty means the project the
	// monitor was started with. The control plane (Firestore, Pub/Sub) always
	// stays in the monitor's own project.
	ProjectID             string
	Region                string   // GCP region for Batch jobs, e.g. "us-central1"
	Zones                 []string // GCP zones to query for running VMs, e.g. ["us-central1-a", "us-central1-b"]
	MachineType           string   // GCP machine type, e.g. "n1-standard-4"
	RootDir               string
	SparklesWorkerGCSPath string
	EmptyVolumes          []EmptyVolume
	GCSMounts             []GCSMount
	BootDiskSizeGb        int
	BootDiskType          string
	Resources             []ResourceEntry
	ServiceAccount        string
	Labels                []Label

	// Provisioning parameters
	MaxWorkerCount               int
	MaxPreemptibleWorkerAttempts int
	MaxWorkersPerRequest         int // default: 100

	// Watchdog parameters
	VMShutdownGracePeriod       time.Duration // default: 1min
	MaxZombiesBeforeAbort       int           // default: 3
	MaxConsecutiveFailedBatches int           // default: 2

	// LingerTime is how long a leader worker keeps polling for new tasks
	// after its queue empties before exiting (0 = exit immediately).
	// Forwarded to newly provisioned workers as WorkerJobSpec.LingerTime.
	LingerTime time.Duration
}

// WorkPoolState holds the mutable runtime state for a workpool. It is stored in
// WorkPoolSummary and never written to the immutable WorkPools collection.
//
// StateMessage/LastIncidentAt/IncidentCount are NOT part of this struct —
// unlike State, they aren't persisted mutable state. WorkPoolSummary's
// equivalent fields are derived by querying recent workpool_incident events
// (see EventStore.ListRecentWorkpoolIncidents) each time the WorkPool
// summary poll runs, so they self-heal as old incidents age out of the
// window instead of accumulating forever.
type WorkPoolState struct {
	WorkpoolID string
	State      WorkPoolStatus
}

// WorkPoolWithState pairs an immutable WorkPool config with its current mutable state.
// Returned by WorkPoolStore.Get and WorkPoolStore.ListAll.
type WorkPoolWithState struct {
	Pool  *WorkPool
	State *WorkPoolState
}

// BatchAPIRequest corresponds to the BatchAPIRequest Firestore collection.
type BatchAPIRequest struct {
	BatchID string
	JobID   string
	// ProjectID is the GCP project this batch's job and VMs live in, copied
	// from WorkPool.ProjectID at submission time. Pinned per batch rather than
	// read back from the workpool so that a workpool whose spec is later
	// overwritten with a different project doesn't strand in-flight batches.
	// Empty means the monitor's own project.
	ProjectID             string
	WorkpoolID            string
	ExpectedVMCount       int
	Preemptible           bool
	SubmittedAt           time.Time
	Expiry                time.Time
	RunningSince          *time.Time // nil until the job first reaches RUNNING
	RegisteredWorkerCount int        // monotonic; incremented at worker registration, never decremented
	Status                BatchStatus
	Unhealthy             bool    // sticky; never cleared; independent of Status
	TerminationReason     string  // populated when Status == BatchStatusTerminated; explains why the monitor killed the job
	Labels                []Label // copied from WorkPool.Labels at submission time
}

// Worker is the subset of the Workers Firestore document needed by the monitor.
type Worker struct {
	WorkerID        string
	WorkpoolID      string
	BatchID         string    // which batch submitted this worker
	InstanceName    string    // GCP instance name recorded at startup; enables surgical VM termination
	Status          string    // "started", "stopped" (clean shutdown), or "zombie" (heartbeat expired without a clean shutdown)
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

// GCSMount describes a GCS bucket (or subdirectory) to mount on each worker
// VM via GCP Batch's native GCS volume support. Field names and tags match
// v100.GCSMount so Firestore documents round-trip correctly.
type GCSMount struct {
	MountPath    string   `firestore:"mount_path"    json:"mountPath"`
	GCSPath      string   `firestore:"gcs_path"      json:"gcsPath"`
	MountOptions []string `firestore:"mount_options" json:"mountOptions"`
}

// WorkerJobSpec holds all parameters needed to create a GCP Batch job for workers.
type WorkerJobSpec struct {
	WorkpoolID string
	BatchID    string
	// ProjectID is the GCP project to create the Batch job in. Empty means the
	// BatchAPIClient's own project.
	ProjectID             string
	Region                string
	MachineType           string
	VMCount               int
	Preemptible           bool
	SparklesWorkerGCSPath string
	Command               string
	RootDir               string
	EmptyVolumes          []EmptyVolume
	GCSMounts             []GCSMount
	ServiceAccount        string
	DBName                string
	Resources             []ResourceEntry
	Labels                []Label
	LingerTime            time.Duration
	BootDiskSizeGb        int
	BootDiskType          string
}

// ----- External service interfaces -----

// BatchAPIClient wraps the GCP Batch API. All methods receive a context for cancellation.
//
// The methods that address a project directly (CreateJob via spec.ProjectID,
// and ListRunningVMs/TerminateVM/PrintBatchDebuggingInfo via a projectID
// argument) treat an empty project as "the client's own project", so callers
// with no per-workpool override keep the previous behavior. GetJobStatus and
// TerminateJob need no project: they take a fully-qualified job resource name,
// which already embeds it.
//
// ListRunningVMs filters by a single GCE label; see labelWorkpool in
// batch_api.go for the label actually set on worker VMs.
type BatchAPIClient interface {
	CreateJob(ctx context.Context, spec *WorkerJobSpec) (jobID string, err error)
	GetJobStatus(ctx context.Context, jobID string) (BatchJobStatus, error)
	ListRunningVMs(ctx context.Context, projectID, filterLabelName, filterLabelValue string, zones []string) (map[string]VMInfo, error)
	TerminateVM(ctx context.Context, projectID, zone, instanceName string) error
	TerminateJob(ctx context.Context, jobID string) error
	PrintBatchDebuggingInfo(ctx context.Context, projectID, jobID string) error
}

// WorkPoolStore reads WorkPool config and reads/writes WorkPoolState.
type WorkPoolStore interface {
	ListAll(ctx context.Context) ([]*WorkPoolWithState, error)
	Get(ctx context.Context, workpoolID string) (*WorkPoolWithState, error)
	SaveState(ctx context.Context, state *WorkPoolState) error
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
}

// WorkerStore reads Worker documents.
type WorkerStore interface {
	// ListExpired returns workers with status "started" whose HeartbeatExpiry is before now.
	ListExpired(ctx context.Context, now time.Time) ([]*Worker, error)
	// ListByBatch returns all workers registered for a given batch.
	ListByBatch(ctx context.Context, batchID string) ([]*Worker, error)
	// CountActive returns workers with HeartbeatExpiry after now.
	CountActive(ctx context.Context, workpoolID string, now time.Time) (int, error)
	// ListAllForWorkpool returns all Workers registered for the given workpool.
	ListAllForWorkpool(ctx context.Context, workpoolID string) ([]*Worker, error)
	// MarkZombie sets the worker's status to "zombie" — used by task recovery
	// when a worker's heartbeat expires without a clean shutdown, so it can
	// be distinguished from a worker that stopped cleanly.
	MarkZombie(ctx context.Context, workerID string) error
}

// TaskStore reads and updates Task documents.
type TaskStore interface {
	// ListByWorker returns tasks owned by a worker that are in one of the given statuses.
	ListByWorker(ctx context.Context, workerID string, statuses []TaskStatus) ([]*Task, error)
	// CountPending returns the number of pending tasks for a workpool.
	CountPending(ctx context.Context, workpoolID string) (int, error)
	// ResetToPending sets the task to pending, clears OwningWorkerID, and
	// publishes a task_state_update event. jobID and oldStatus must match the
	// task's current values so the event is accurate.
	ResetToPending(ctx context.Context, taskID, jobID string, oldStatus TaskStatus) error
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
	WorkpoolID                   string  `firestore:"workpool_id"`
	MachineType                  string  `firestore:"machine_type"`
	Labels                       []Label `firestore:"labels"`
	MaxPreemptibleWorkerAttempts int     `firestore:"max_preemptible_worker_attempts"`

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
	WorkpoolID                    string         `firestore:"workpool_id"`
	Timestamp                     time.Time      `firestore:"timestamp"`
	Expiry                        time.Time      `firestore:"expiry"`
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
	// ListIdle returns all WorkPoolSummary documents whose state is idle.
	ListIdle(ctx context.Context) ([]*WorkPoolSummary, error)
	// Delete removes the WorkPoolSummary document for the given workpool.
	Delete(ctx context.Context, workpoolID string) error
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
	JobID       string       `firestore:"job_id"`
	WorkpoolID  string       `firestore:"workpool_id"`
	Name        string       `firestore:"name"`
	CreatedAt   time.Time    `firestore:"created_at"`
	Expiry      time.Time    `firestore:"expiry"`
	LastUpdated time.Time    `firestore:"last_updated"`
	State       JobStatus    `firestore:"state"`
	Tasks       []StateCount `firestore:"tasks"`
	Labels      []Label      `firestore:"labels"`
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

// TaskStatePublisher emits a task_state_update event on every task state
// transition. Defined here (not in v100) to avoid an import cycle.
type TaskStatePublisher interface {
	PublishTaskStateUpdate(ctx context.Context, taskID, jobID, oldState, newState string) error
}

// JobTerminatedPublisher emits a job_terminated event when a job reaches a
// terminal state. Defined here (not in v100) to avoid an import cycle.
type JobTerminatedPublisher interface {
	PublishJobTerminated(ctx context.Context, jobID, workpoolID string) error
}

// WorkpoolStatePublisher emits a workpool_state_change event whenever workpool
// state is persisted. Defined here (not in v100) to avoid an import cycle.
type WorkpoolStatePublisher interface {
	PublishWorkpoolStateChange(ctx context.Context, workpoolID, state, stateMessage string) error
}

// BatchOutcomePublisher emits one event per batch attempt outcome — success
// or failure — used to feed checkHaltThreshold's event-log query. Defined
// here (not in v100) to avoid an import cycle.
type BatchOutcomePublisher interface {
	// PublishBatchFailed records that a batch (or a CreateJob call that never
	// became a batch) failed. reason is a human-readable explanation.
	PublishBatchFailed(ctx context.Context, workpoolID, reason string) error
	// PublishBatchSucceeded records that a batch was confirmed healthy (its
	// first worker registered). Published once per batch.
	PublishBatchSucceeded(ctx context.Context, workpoolID string) error
}

// WorkpoolIncidentPublisher emits a workpool_incident event whenever the
// watchdog detects a batch/worker anomaly for a workpool. Used to feed
// updateWorkPoolSummary's event-log query for StateMessage/LastIncidentAt/
// IncidentCount. Defined here (not in v100) to avoid an import cycle.
type WorkpoolIncidentPublisher interface {
	// PublishWorkpoolIncident records that a watchdog anomaly of the given
	// incidentType occurred for workpoolID. See the IncidentType* constants
	// for the set of recognized types.
	PublishWorkpoolIncident(ctx context.Context, workpoolID, incidentType, reason string) error
}

// WorkerEventPublisher emits a worker_stopped event when a worker's active
// lifecycle ends. Defined here (not in v100) to avoid an import cycle.
type WorkerEventPublisher interface {
	// PublishWorkerStopped records that a worker stopped. cleanlyTerminated
	// is false when the worker was marked a zombie (its heartbeat expired
	// without a clean shutdown), true for a normal, self-reported shutdown.
	PublishWorkerStopped(ctx context.Context, workerID, workpoolID string, cleanlyTerminated bool) error
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

// JobCreatedRecord is a minimal view of an Events document for job_created events.
type JobCreatedRecord struct {
	WorkpoolID string
	Timestamp  time.Time
}

// BatchOutcome is a minimal view of an Events document for batch_failed/batch_succeeded events.
type BatchOutcome struct {
	Failed    bool // true for batch_failed, false for batch_succeeded
	Timestamp time.Time
}

// WorkpoolIncident is a minimal view of an Events document for workpool_incident events.
type WorkpoolIncident struct {
	IncidentType string
	Message      string
	Timestamp    time.Time
}

// EventStore queries the Events collection for job_created, batch outcome,
// and workpool incident events.
type EventStore interface {
	// ListJobCreatedSince returns all job_created events with timestamp > since.
	// If since is zero, all job_created events are returned.
	ListJobCreatedSince(ctx context.Context, since time.Time) ([]JobCreatedRecord, error)
	// ListRecentBatchOutcomes returns batch_failed/batch_succeeded events for
	// workpoolID with timestamp > since, ordered most-recent-first.
	ListRecentBatchOutcomes(ctx context.Context, workpoolID string, since time.Time) ([]BatchOutcome, error)
	// ListRecentWorkpoolIncidents returns workpool_incident events for
	// workpoolID with timestamp > since, ordered most-recent-first.
	ListRecentWorkpoolIncidents(ctx context.Context, workpoolID string, since time.Time) ([]WorkpoolIncident, error)
	// ListRecentWorkpoolIncidentsByType returns workpool_incident events for
	// workpoolID matching incidentType with timestamp > since, ordered
	// most-recent-first. Used by provisioning to count recent zombie
	// incidents (a proxy for preemption) against MaxPreemptibleWorkerAttempts.
	ListRecentWorkpoolIncidentsByType(ctx context.Context, workpoolID, incidentType string, since time.Time) ([]WorkpoolIncident, error)
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
