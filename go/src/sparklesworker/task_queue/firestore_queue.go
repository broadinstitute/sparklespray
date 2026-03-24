package task_queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const TaskCollection = "V7Task"
const ClusterCollection = "V7Cluster"
const JobCollection = "V7Job"
const CachedTaskEntryCollection = "V7CachedTaskEntry"

const InitialClaimRetryDelay = 1000

// Cluster represents cluster configuration stored in Firestore
type Cluster struct {
	IncomingTopic string `firestore:"incoming_topic"`
	ResponseTopic string `firestore:"response_topic"`
}

// FirestoreQueue implements TaskQueue using Google Cloud Firestore
type FirestoreQueue struct {
	client            *firestore.Client
	cluster           string
	workerID          string
	InitialClaimRetry time.Duration
	ClaimTimeout      time.Duration
	WatchdogNotifier  func() // Called periodically during long operations
}

// NewFirestoreQueue creates a new FirestoreQueue
func NewFirestoreQueue(client *firestore.Client, cluster string, workerID string, initialClaimRetry time.Duration, claimTimeout time.Duration) *FirestoreQueue {
	return &FirestoreQueue{
		client:            client,
		cluster:           cluster,
		workerID:          workerID,
		InitialClaimRetry: initialClaimRetry,
		ClaimTimeout:      claimTimeout,
		WatchdogNotifier:  func() {}, // No-op by default
	}
}

// GetCluster fetches cluster configuration from Firestore
func GetCluster(ctx context.Context, client *firestore.Client, clusterID string) (*Cluster, error) {
	docSnap, err := client.Collection(ClusterCollection).Doc(clusterID).Get(ctx)
	if err != nil {
		return nil, err
	}
	var cluster Cluster
	if err := docSnap.DataTo(&cluster); err != nil {
		return nil, err
	}
	return &cluster, nil
}

func getTasks(ctx context.Context, client *firestore.Client, cluster string, taskStatus string, maxFetch int) ([]*Task, error) {
	docs, err := client.Collection(TaskCollection).
		Where("cluster", "==", cluster).
		Where("status", "==", taskStatus).
		Limit(maxFetch).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, err
	}

	tasks := make([]*Task, 0, len(docs))
	for _, doc := range docs {
		var task Task
		if err := doc.DataTo(&task); err != nil {
			return nil, err
		}
		task.TaskID = doc.Ref.ID
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func getTimestampMillis() int64 {
	return int64(time.Now().UnixNano()) / int64(time.Millisecond)
}

// ClaimTask attempts to claim a pending task from the queue
func (q *FirestoreQueue) ClaimTask(ctx context.Context) (*Task, error) {
	maxSleepTime := q.InitialClaimRetry
	claimStart := time.Now()

	for {
		if q.WatchdogNotifier != nil {
			q.WatchdogNotifier()
		}

		tasks, err := getTasks(ctx, q.client, q.cluster, StatusPending, 20)
		if err != nil {
			return nil, err
		}
		if len(tasks) == 0 {
			return nil, nil
		}

		// Pick a random task to avoid contention
		task := tasks[rand.Int31n(int32(len(tasks)))]

		finalTask, err := q.claimTaskByID(ctx, task.TaskID)
		if err == nil {
			return finalTask, nil
		}

		// Failed to claim task
		if time.Since(claimStart) > q.ClaimTimeout {
			return nil, errors.New("timed out trying to get task")
		}

		maxSleepTime *= 2
		timeUntilNextTry := time.Duration(rand.Int63n(int64(maxSleepTime)))
		log.Printf("Got error claiming task: %s, will retry after %d milliseconds", err, timeUntilNextTry/time.Millisecond)
		time.Sleep(timeUntilNextTry)
	}
}

func (q *FirestoreQueue) claimTaskByID(ctx context.Context, taskID string) (*Task, error) {
	now := getTimestampMillis()
	event := TaskHistory{
		Timestamp:       float64(now) / 1000.0,
		Status:          StatusClaimed,
		OwnedByWorkerID: q.workerID,
	}

	mutate := func(task *Task) bool {
		if task.Status != StatusPending {
			log.Printf("Expected status to be pending but was '%s'", task.Status)
			return false
		}

		task.History = append(task.History, &event)
		task.Status = StatusClaimed
		task.OwnedByWorkerID = q.workerID
		task.LastUpdated = float64(now) / 1000.0

		return true
	}

	return q.AtomicUpdateTask(ctx, taskID, mutate)
}

// IsJobKilled checks if the job has been killed
func (q *FirestoreQueue) IsJobKilled(ctx context.Context, jobID string) (bool, error) {
	docSnap, err := q.client.Collection(JobCollection).Doc(jobID).Get(ctx)
	if err != nil {
		return false, err
	}
	var job Job
	if err := docSnap.DataTo(&job); err != nil {
		return false, err
	}
	return job.Status == JobStatusKilled, nil
}

// GetTask retrieves a task by ID from Firestore.
func (q *FirestoreQueue) GetTask(ctx context.Context, taskID string) (*Task, error) {
	docSnap, err := q.client.Collection(TaskCollection).Doc(taskID).Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting task %s: %w", taskID, err)
	}
	var task Task
	if err := docSnap.DataTo(&task); err != nil {
		return nil, fmt.Errorf("decoding task %s: %w", taskID, err)
	}
	return &task, nil
}

// AddJob inserts a job and its tasks into Firestore.
// Tasks are inserted in batches of 500 (the WriteBatch limit).
func (q *FirestoreQueue) AddJob(ctx context.Context, job *Job, tasks []*Task) error {
	jobRef := q.client.Collection(JobCollection).Doc(job.Name)
	if _, err := jobRef.Set(ctx, job); err != nil {
		return fmt.Errorf("storing job: %w", err)
	}

	const batchSize = 500
	for i := 0; i < len(tasks); i += batchSize {
		end := i + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		wb := q.client.Batch()
		for _, task := range tasks[i:end] {
			docRef := q.client.Collection(TaskCollection).Doc(task.TaskID)
			wb.Set(docRef, task)
		}
		if _, err := wb.Commit(ctx); err != nil {
			return err
		}
		log.Printf("Inserted tasks %d-%d", i, end-1)
	}
	return nil
}

// AtomicUpdateTask updates a task atomically using the provided callback
func (q *FirestoreQueue) AtomicUpdateTask(ctx context.Context, taskID string, mutateTaskCallback func(task *Task) bool) (*Task, error) {
	var task Task
	client := q.client
	docRef := client.Collection(TaskCollection).Doc(taskID)

	log.Printf("AtomicUpdateTask of task %v", taskID)
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		log.Printf("attempting update of task %s start", taskID)

		docSnap, err := tx.Get(docRef)
		if err != nil {
			return err
		}
		if err := docSnap.DataTo(&task); err != nil {
			return err
		}
		task.TaskID = taskID

		log.Printf("Calling mutate on task %s with version %d", task.TaskID, task.Version)
		successfulUpdate := mutateTaskCallback(&task)
		if !successfulUpdate {
			log.Printf("Update failed on task %s", task.TaskID)
			return errors.New("update failed")
		}

		task.Version = task.Version + 1
		log.Printf("Calling set on task %s with version %d", task.TaskID, task.Version)
		if err := tx.Set(docRef, &task); err != nil {
			return err
		}

		log.Printf("Returning AtomicUpdateTask success")
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &task, nil
}

// FirestoreTaskCache implements TaskCache using Google Cloud Firestore.
type FirestoreTaskCache struct {
	client *firestore.Client
}

func NewFirestoreTaskCache(client *firestore.Client) *FirestoreTaskCache {
	return &FirestoreTaskCache{client: client}
}

func (c *FirestoreTaskCache) GetCachedEntry(ctx context.Context, cacheKey string) (*CachedTaskEntry, error) {
	docSnap, err := c.client.Collection(CachedTaskEntryCollection).Doc(cacheKey).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting cached entry %s: %w", cacheKey, err)
	}
	var entry CachedTaskEntry
	if err := docSnap.DataTo(&entry); err != nil {
		return nil, fmt.Errorf("decoding cached entry %s: %w", cacheKey, err)
	}
	return &entry, nil
}

func (c *FirestoreTaskCache) SetCachedEntry(ctx context.Context, entry *CachedTaskEntry) error {
	_, err := c.client.Collection(CachedTaskEntryCollection).Doc(entry.ID).Set(ctx, entry)
	return err
}
