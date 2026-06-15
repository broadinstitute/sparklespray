package v100

import (
	"context"
	"math/rand"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const jobCollection = "Jobs"
const taskCollection = "Tasks"

type ResourceEntry struct {
	Name  string  `firestore:"name"`
	Value float64 `firestore:"value"`
}

type Job struct {
	JobID     string          `firestore:"job_id"`
	Resources []ResourceEntry `firestore:"resources"`
	Workpool  string          `firestore:"workpool"`
}

type Task struct {
	JobID          string `firestore:"job_id"`
	TaskID         string `firestore:"task_id"`
	TaskIndex      int    `firestore:"task_index"`
	Status         string `firestore:"status"`
	Command        string `firestore:"command"`
	Workpool       string `firestore:"workpool"`
	OwningWorkerID string `firestore:"owning_worker_id"`
	FailureReason  string `firestore:"failure_reason"`
}

type TaskQueue struct {
	fs *firestore.Client
}

func NewTaskQueue(fs *firestore.Client) *TaskQueue {
	return &TaskQueue{fs: fs}
}

func (q *TaskQueue) taskDoc(taskID string) *firestore.DocumentRef {
	return q.fs.Collection(taskCollection).Doc(taskID)
}

// GetFirstPendingTask returns the first pending task in the given workpool, or nil if none exist.
func (q *TaskQueue) GetFirstPendingTask(ctx context.Context, workpool string) (*Task, error) {
	iter := q.fs.Collection(taskCollection).
		Where("workpool", "==", workpool).
		Where("status", "==", "pending").
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
func (q *TaskQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
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
func (q *TaskQueue) ClaimTask(ctx context.Context, jobID string, workerID string) (*Task, error) {
	for {
		docs, err := q.fs.Collection(taskCollection).
			Where("job_id", "==", jobID).
			Where("status", "==", "pending").
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
				if t.Status != "pending" {
					return nil
				}
				t.Status = "claimed"
				t.OwningWorkerID = workerID
				claimed = &t
				return tx.Set(snap.Ref, t)
			})
			if err != nil {
				return nil, err
			}
			if claimed != nil {
				return claimed, nil
			}
		}
		// All 100 candidates were claimed by other workers; try another batch.
	}
}

// RecordComplete marks a task as complete.
func (q *TaskQueue) RecordComplete(ctx context.Context, taskID string) error {
	_, err := q.taskDoc(taskID).Update(ctx, []firestore.Update{
		{Path: "status", Value: "complete"},
		{Path: "owning_worker_id", Value: ""},
	})
	return err
}

// RecordFailed marks a task as failed with a reason.
func (q *TaskQueue) RecordFailed(ctx context.Context, taskID string, failureReason string) error {
	_, err := q.taskDoc(taskID).Update(ctx, []firestore.Update{
		{Path: "status", Value: "failed"},
		{Path: "owning_worker_id", Value: ""},
		{Path: "failure_reason", Value: failureReason},
	})
	return err
}

// RecordKilled marks a task as killed.
func (q *TaskQueue) RecordKilled(ctx context.Context, taskID string) error {
	_, err := q.taskDoc(taskID).Update(ctx, []firestore.Update{
		{Path: "status", Value: "killed"},
		{Path: "owning_worker_id", Value: ""},
	})
	return err
}
