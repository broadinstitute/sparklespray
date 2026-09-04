package v100

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// Exported collection name constants for use by sub-packages.
const (
	JobCollection      = "Jobs"
	TaskCollection     = "Tasks"
	WorkpoolCollection = "WorkPools"
)

const jobCollection = JobCollection
const taskCollection = TaskCollection
const workpoolCollection = WorkpoolCollection

// Task status values for the active (non-terminal) states.
const (
	StatusPending = "pending"
	StatusClaimed = "claimed"
	StatusRunning = "running"
	StatusWriting = "writing"
)

// Task status values for the terminal states.
const (
	StatusSuccess = "success" // task ran to completion with exit code 0
	StatusError   = "error"   // task ran to completion with a non-zero exit code
	StatusFailed  = "failed"  // task did not run to completion (infrastructure/system failure)
	StatusKilled  = "killed"  // task was administratively terminated
)

// activeStatuses is the set of statuses from which a task can be orphaned back to
// pending. Used by the watchdog (not yet implemented) to reset tasks whose owning
// worker has stopped heartbeating.
var activeStatuses = []string{StatusClaimed, StatusRunning, StatusWriting}

// IsActiveStatus reports whether s is one of the active (non-terminal) task statuses.
func IsActiveStatus(s string) bool {
	for _, a := range activeStatuses {
		if s == a {
			return true
		}
	}
	return false
}

type ResourceEntry struct {
	Name  string  `firestore:"name" json:"name"`
	Value float64 `firestore:"value" json:"value"`
}

type Label struct {
	Name  string `firestore:"name"  json:"name"`
	Value string `firestore:"value" json:"value"`
}

type EmptyVolume struct {
	MountPoint string `firestore:"mount_point" json:"mountPoint"`
	Type       string `firestore:"type"        json:"type"`
	SizeInGB   int    `firestore:"size_in_gb"  json:"sizeInGB"`
}

type WorkPool struct {
	WorkpoolID string `firestore:"workpool_id"`
	// ProjectID, if set, is the GCP project the Batch jobs (and therefore the
	// worker VMs) for this workpool are created in. Empty means the project
	// the monitor was started with. The control plane (Firestore, Pub/Sub)
	// always stays in the monitor's own project.
	ProjectID   string `firestore:"project_id"`
	MachineType string `firestore:"machine_type"`
	RootDir               string          `firestore:"root_dir"`
	SparklesWorkerGCSPath string          `firestore:"sparkles_worker_gcs_path"`
	ServiceAccount        string          `firestore:"service_account"`
	Resources             []ResourceEntry `firestore:"resources"`
	EmptyVolumes          []EmptyVolume   `firestore:"empty_volumes"`
	Labels                []Label         `firestore:"labels"`
	Expiry       time.Time       `firestore:"expiry"`
	Region       string          `firestore:"region"`
	Zones        []string        `firestore:"zones"`

	// WorkpoolSpecHash is the sha256 (hex-encoded) of the canonical JSON of
	// the WorkpoolSpec this record was created from, including Labels.
	// Computed by computeWorkpoolSpecHash (v100/dev/workpool_spec.go) at submission
	// time; the same hash (truncated) is used to derive the workpool ID
	// itself when one isn't explicitly given (see resolveWorkpoolID).
	WorkpoolSpecHash string `firestore:"workpool_spec_hash"`

	// Provisioning parameters
	MaxWorkerCount               int `firestore:"max_worker_count"`
	MaxPreemptibleWorkerAttempts int `firestore:"max_preemptible_worker_attempts"`
	MaxWorkersPerRequest         int `firestore:"max_workers_per_request"`

	// Watchdog parameters (zero value → monitor uses its own defaults)
	VMShutdownGracePeriodSec    int `firestore:"vm_shutdown_grace_period_sec"`
	MaxZombiesBeforeAbort       int `firestore:"max_zombies_before_abort"`
	MaxConsecutiveFailedBatches int `firestore:"max_consecutive_failed_batches"`

	// LingerTimeSec is how long a leader worker keeps polling for new tasks
	// after its queue empties before exiting (0 = exit immediately). Passed
	// to newly provisioned workers as --linger.
	LingerTimeSec int `firestore:"linger_time_sec"`

	// State fields (written by the monitor; stored in WorkPoolSummary collection)
	State         string    `firestore:"state"`
	StateMessage  string    `firestore:"state_message"`
	LastIncidentAt time.Time `firestore:"last_incident_at"`
	IncidentCount  int       `firestore:"incident_count"`
}

type Job struct {
	JobID      string          `firestore:"job_id"`
	Name       string          `firestore:"name"`
	WorkpoolID string          `firestore:"workpool_id"`
	CreatedAt  time.Time       `firestore:"created_at"`
	Expiry     time.Time       `firestore:"expiry"`
	TaskCount  int             `firestore:"task_count"`
	Resources  []ResourceEntry `firestore:"resources"`
	Labels     []Label         `firestore:"labels"`
}

type FileToLocalize struct {
	Source       string `firestore:"source"        json:"source"`
	Destination  string `firestore:"destination"   json:"destination"`
	IsExecutable bool   `firestore:"is_executable" json:"is_executable"`
}

// ResourceUsage holds a summary of resources consumed by a single task execution.
// Fields are zero when the metric could not be collected (e.g. cgroup unavailable).
type ResourceUsage struct {
	StartTime       time.Time `firestore:"start_time"`
	EndTime         time.Time `firestore:"end_time"`
	ElapsedSeconds  float64   `firestore:"elapsed_seconds"`
	MaxMemoryBytes  int64     `firestore:"max_memory_bytes"`
	CPUUserUSec     int64     `firestore:"cpu_user_usec"`
	CPUSystemUSec   int64     `firestore:"cpu_system_usec"`
	BlockReadBytes  int64     `firestore:"block_read_bytes"`
	BlockWriteBytes int64     `firestore:"block_write_bytes"`
	ExitCode        int       `firestore:"exit_code"`
	OOMKilled       bool      `firestore:"oom_killed"`
}

type Task struct {
	JobID       string   `firestore:"job_id"`
	TaskID      string   `firestore:"task_id"`
	TaskIndex   int      `firestore:"task_index"`
	WorkpoolID  string   `firestore:"workpool_id"`
	Status      string   `firestore:"status"`
	Command     []string `firestore:"command"`
	DockerImage string   `firestore:"docker_image"`
	ResultPath  string   `firestore:"result_path"`
	LogPath     string   `firestore:"log_path"`
	// either FilesToLocalizeManifest or FilesToLocalize will be populated. If there's a small
	// number of files, we can just store them in the task, but if we have a large number of files
	// write them to cloud storage as a manifest and read them from there instead.
	FilesToLocalizeManifest string           `firestore:"files_to_localize_manifest"`
	FilesToLocalize         []FileToLocalize `firestore:"files_to_localize"`
	Labels                  []Label          `firestore:"labels"`
	OwningWorkerID          string           `firestore:"owning_worker_id"`
	FailureReason           string           `firestore:"failure_reason"`
	ExitCode                int              `firestore:"exit_code"`
	ResourceUsage           *ResourceUsage   `firestore:"resource_usage"`
	LastUpdated             time.Time        `firestore:"last_updated"`
	Expiry                  time.Time        `firestore:"expiry"`
}

type TaskQueue interface {
	GetFirstPendingTask(ctx context.Context, workpoolID string) (*Task, error)
	GetJob(ctx context.Context, jobID string) (*Job, error)
	ClaimTask(ctx context.Context, jobID string, workerID string) (*Task, error)
	UpdateState(ctx context.Context, taskID string, oldState string, newState string) error
	RecordError(ctx context.Context, taskID string, exitCode int) error
	RecordFailed(ctx context.Context, taskID string, failureReason string, oldState string) error
	// RecordKilled marks a task as killed. If onlyIfPending is true, the update
	// is skipped (with an error) if the task is no longer in StatusPending — this
	// handles the race where a task is claimed between the CLI's query and kill.
	// Pass onlyIfPending=false from the worker, where the task is known to be active.
	RecordKilled(ctx context.Context, taskID string, onlyIfPending bool) error
	// RecordResourceUsage persists a ResourceUsage summary. Best-effort: callers
	// should log but not fail on error.
	RecordResourceUsage(ctx context.Context, taskID string, ru *ResourceUsage) error
}

type FirestoreTaskQueue struct {
	fs        *firestore.Client
	publisher *EventPublisher
}

func NewFirestoreTaskQueue(fs *firestore.Client, publisher *EventPublisher) *FirestoreTaskQueue {
	return &FirestoreTaskQueue{fs: fs, publisher: publisher}
}

func (q *FirestoreTaskQueue) taskDoc(taskID string) *firestore.DocumentRef {
	return q.fs.Collection(taskCollection).Doc(taskID)
}

// GetFirstPendingTask returns the first pending task in the given workpool, or nil if none exist.
func (q *FirestoreTaskQueue) GetFirstPendingTask(ctx context.Context, workpoolID string) (*Task, error) {
	iter := q.fs.Collection(taskCollection).
		Where("workpool_id", "==", workpoolID).
		Where("status", "==", StatusPending).
		Limit(1).
		Documents(ctx)
	defer iter.Stop()

	doc, err := iter.Next()
	if err == iterator.Done {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var task Task
	if err := doc.DataTo(&task); err != nil {
		return nil, err
	}
	return &task, nil
}

// GetJob fetches a job by ID.
func (q *FirestoreTaskQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	doc, err := q.fs.Collection(jobCollection).Doc(jobID).Get(ctx)
	if err != nil {
		return nil, err
	}
	var job Job
	if err := doc.DataTo(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

// ClaimTask atomically claims a pending task from the given job for the given worker.
// It fetches up to 100 pending tasks, shuffles them to reduce contention, and retries
// until a claim succeeds or no pending tasks remain.
func (q *FirestoreTaskQueue) ClaimTask(ctx context.Context, jobID string, workerID string) (*Task, error) {
	for {
		docs, err := q.fs.Collection(taskCollection).
			Where("job_id", "==", jobID).
			Where("status", "==", StatusPending).
			Limit(100).
			Documents(ctx).
			GetAll()
		if err != nil {
			return nil, err
		}
		if len(docs) == 0 {
			return nil, nil
		}

		rand.Shuffle(len(docs), func(i, j int) { docs[i], docs[j] = docs[j], docs[i] })

		for _, snap := range docs {
			var claimed *Task
			err := q.fs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
				doc, err := tx.Get(snap.Ref)
				if err != nil {
					return err
				}
				var t Task
				if err := doc.DataTo(&t); err != nil {
					return err
				}
				if t.Status != StatusPending {
					return nil
				}
				t.Status = StatusClaimed
				t.OwningWorkerID = workerID
				t.LastUpdated = time.Now()
				claimed = &t
				return tx.Set(snap.Ref, t)
			})
			if err != nil {
				return nil, err
			}
			if claimed != nil {
				if err := q.publisher.PublishTaskStateUpdate(ctx, claimed.TaskID, claimed.JobID, StatusPending, StatusClaimed); err != nil {
					return nil, err
				}
				return claimed, nil
			}
		}
		// All 100 candidates were claimed by other workers; try another batch.
	}
}

// UpdateState transitions a task to newState. It clears owning_worker_id when
// moving out of an active state (i.e. when newState is not claimed/running/writing).
func (q *FirestoreTaskQueue) UpdateState(ctx context.Context, taskID string, oldState string, newState string) error {
	var jobID string
	err := q.fs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		doc, err := tx.Get(q.taskDoc(taskID))
		if err != nil {
			return err
		}
		var t Task
		if err := doc.DataTo(&t); err != nil {
			return err
		}
		jobID = t.JobID
		updates := []firestore.Update{{Path: "status", Value: newState}, {Path: "last_updated", Value: time.Now()}}
		if !IsActiveStatus(newState) {
			updates = append(updates, firestore.Update{Path: "owning_worker_id", Value: ""})
		}
		return tx.Update(q.taskDoc(taskID), updates)
	})
	if err != nil {
		return err
	}
	return q.publisher.PublishTaskStateUpdate(ctx, taskID, jobID, oldState, newState)
}

// RecordError marks a task as completed with a non-zero exit code. The process
// ran to completion but reported failure via its exit code.
func (q *FirestoreTaskQueue) RecordError(ctx context.Context, taskID string, exitCode int) error {
	var jobID string
	err := q.fs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		doc, err := tx.Get(q.taskDoc(taskID))
		if err != nil {
			return err
		}
		var t Task
		if err := doc.DataTo(&t); err != nil {
			return err
		}
		jobID = t.JobID
		return tx.Update(q.taskDoc(taskID), []firestore.Update{
			{Path: "status", Value: StatusError},
			{Path: "exit_code", Value: exitCode},
			{Path: "owning_worker_id", Value: ""},
			{Path: "last_updated", Value: time.Now()},
		})
	})
	if err != nil {
		return err
	}
	return q.publisher.PublishTaskStateUpdate(ctx, taskID, jobID, StatusWriting, StatusError)
}

// RecordKilled marks a task as killed. If onlyIfPending is true the update is
// skipped with an error if the task is no longer StatusPending (handles the
// race where a task is claimed between the CLI's query and the kill call).
func (q *FirestoreTaskQueue) RecordKilled(ctx context.Context, taskID string, onlyIfPending bool) error {
	var oldState, jobID string
	err := q.fs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		doc, err := tx.Get(q.taskDoc(taskID))
		if err != nil {
			return err
		}
		var t Task
		if err := doc.DataTo(&t); err != nil {
			return err
		}
		if onlyIfPending && t.Status != StatusPending {
			return fmt.Errorf("task %s is no longer pending (status=%s); skipping kill", taskID, t.Status)
		}
		oldState = t.Status
		jobID = t.JobID
		return tx.Update(q.taskDoc(taskID), []firestore.Update{
			{Path: "status", Value: StatusKilled},
			{Path: "owning_worker_id", Value: ""},
			{Path: "last_updated", Value: time.Now()},
		})
	})
	if err != nil {
		return err
	}
	return q.publisher.PublishTaskStateUpdate(ctx, taskID, jobID, oldState, StatusKilled)
}

// RecordResourceUsage persists a ResourceUsage summary for the given task.
// It is best-effort: callers should log but not fail on error.
func (q *FirestoreTaskQueue) RecordResourceUsage(ctx context.Context, taskID string, ru *ResourceUsage) error {
	_, err := q.taskDoc(taskID).Update(ctx, []firestore.Update{
		{Path: "resource_usage", Value: ru},
	})
	return err
}

// RecordFailed marks a task as failed due to an infrastructure or system error.
// The task did not run to completion. oldState must be the task's current status
// (claimed, running, or writing). failureReason describes what went wrong.
func (q *FirestoreTaskQueue) RecordFailed(ctx context.Context, taskID string, failureReason string, oldState string) error {
	var jobID string
	err := q.fs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		doc, err := tx.Get(q.taskDoc(taskID))
		if err != nil {
			return err
		}
		var t Task
		if err := doc.DataTo(&t); err != nil {
			return err
		}
		jobID = t.JobID
		return tx.Update(q.taskDoc(taskID), []firestore.Update{
			{Path: "status", Value: StatusFailed},
			{Path: "owning_worker_id", Value: ""},
			{Path: "failure_reason", Value: failureReason},
			{Path: "last_updated", Value: time.Now()},
		})
	})
	if err != nil {
		return err
	}
	return q.publisher.PublishTaskStateUpdate(ctx, taskID, jobID, oldState, StatusFailed)
}
