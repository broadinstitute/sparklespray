package v100

import (
	"context"
	"math/rand"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const jobCollection = "Jobs"
const taskCollection = "Tasks"

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
	Name  string  `firestore:"name"`
	Value float64 `firestore:"value"`
}

type Job struct {
	JobID      string          `firestore:"job_id"`
	WorkpoolID string          `firestore:"workpool_id"`
	Resources  []ResourceEntry `firestore:"resources"`
}

type FileToLocalize struct {
	Source       string `firestore:"source"`
	Destination  string `firestore:"destination"`
	IsExecutable bool   `firestore:"is_executable"`
}

type Task struct {
	JobID           string           `firestore:"job_id"`
	TaskID          string           `firestore:"task_id"`
	TaskIndex       int              `firestore:"task_index"`
	WorkpoolID      string           `firestore:"workpool_id"`
	Status          string           `firestore:"status"`
	Command         []string         `firestore:"command"`
	DockerImage     string           `firestore:"docker_image"`
	ResultPath      string           `firestore:"result_path"`
	LogPath         string           `firestore:"log_path"`
	FilesToLocalize []FileToLocalize `firestore:"files_to_localize"`
	OwningWorkerID  string           `firestore:"owning_worker_id"`
	FailureReason   string           `firestore:"failure_reason"`
	ExitCode        int              `firestore:"exit_code"`
}

type TaskQueue interface {
	GetFirstPendingTask(ctx context.Context, workpoolID string) (*Task, error)
	GetJob(ctx context.Context, jobID string) (*Job, error)
	ClaimTask(ctx context.Context, jobID string, workerID string) (*Task, error)
	UpdateState(ctx context.Context, taskID string, oldState string, newState string) error
	RecordError(ctx context.Context, taskID string, exitCode int) error
	RecordFailed(ctx context.Context, taskID string, failureReason string, oldState string) error
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
				claimed = &t
				return tx.Set(snap.Ref, t)
			})
			if err != nil {
				return nil, err
			}
			if claimed != nil {
				if err := q.publisher.PublishTaskStateUpdate(ctx, TaskStateUpdate{
					Type:     "task_state_update",
					TaskID:   claimed.TaskID,
					JobID:    claimed.JobID,
					OldState: StatusPending,
					NewState: StatusClaimed,
				}); err != nil {
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
	updates := []firestore.Update{{Path: "status", Value: newState}}
	if !IsActiveStatus(newState) {
		updates = append(updates, firestore.Update{Path: "owning_worker_id", Value: ""})
	}
	if _, err := q.taskDoc(taskID).Update(ctx, updates); err != nil {
		return err
	}
	return q.publisher.PublishTaskStateUpdate(ctx, TaskStateUpdate{
		Type:     "task_state_update",
		TaskID:   taskID,
		OldState: oldState,
		NewState: newState,
	})
}

// RecordError marks a task as completed with a non-zero exit code. The process
// ran to completion but reported failure via its exit code.
func (q *FirestoreTaskQueue) RecordError(ctx context.Context, taskID string, exitCode int) error {
	if _, err := q.taskDoc(taskID).Update(ctx, []firestore.Update{
		{Path: "status", Value: StatusError},
		{Path: "exit_code", Value: exitCode},
		{Path: "owning_worker_id", Value: ""},
	}); err != nil {
		return err
	}
	return q.publisher.PublishTaskStateUpdate(ctx, TaskStateUpdate{
		Type:     "task_state_update",
		TaskID:   taskID,
		OldState: StatusWriting,
		NewState: StatusError,
	})
}

// RecordFailed marks a task as failed due to an infrastructure or system error.
// The task did not run to completion. oldState must be the task's current status
// (claimed, running, or writing). failureReason describes what went wrong.
func (q *FirestoreTaskQueue) RecordFailed(ctx context.Context, taskID string, failureReason string, oldState string) error {
	if _, err := q.taskDoc(taskID).Update(ctx, []firestore.Update{
		{Path: "status", Value: StatusFailed},
		{Path: "owning_worker_id", Value: ""},
		{Path: "failure_reason", Value: failureReason},
	}); err != nil {
		return err
	}
	return q.publisher.PublishTaskStateUpdate(ctx, TaskStateUpdate{
		Type:     "task_state_update",
		TaskID:   taskID,
		OldState: oldState,
		NewState: StatusFailed,
	})
}


