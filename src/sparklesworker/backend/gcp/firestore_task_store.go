package gcp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FirestoreTaskStore implements backend.TaskStore using Google Cloud Firestore.
// It supports both per-cluster worker operations (ClaimTask, AtomicUpdateTask, etc.)
// and cross-cluster monitor queries (GetClaimedTasks, GetNonCompleteTaskCount, etc.).
type FirestoreTaskStore struct {
	client            *firestore.Client
	ctx               context.Context
	workerID          string
	InitialClaimRetry time.Duration
	ClaimTimeout      time.Duration
	WatchdogNotifier  func() // Called periodically during long operations
}

// NewFirestoreTaskStore creates a FirestoreTaskStore for worker use.
func NewFirestoreTaskStore(client *firestore.Client, workerID string, initialClaimRetry time.Duration, claimTimeout time.Duration) *FirestoreTaskStore {
	return &FirestoreTaskStore{
		client:            client,
		ctx:               context.Background(),
		workerID:          workerID,
		InitialClaimRetry: initialClaimRetry,
		ClaimTimeout:      claimTimeout,
		WatchdogNotifier:  func() {},
	}
}

// newFirestoreTaskStoreGlobal creates a FirestoreTaskStore for autoscaler use
// (no cluster bound; cross-cluster query methods use the provided clusterID parameter).
func newFirestoreTaskStoreGlobal(ctx context.Context, client *firestore.Client) *FirestoreTaskStore {
	return &FirestoreTaskStore{
		client:           client,
		ctx:              ctx,
		WatchdogNotifier: func() {},
	}
}

// ---- Cross-cluster monitor query methods ----

func (q *FirestoreTaskStore) GetClusterIDsFromActiveTasks() ([]string, error) {
	docs, err := q.client.Collection(backend.TaskCollection).
		Where("status", "in", []string{backend.StatusClaimed, backend.StatusPending}).
		Documents(q.ctx).GetAll()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for _, doc := range docs {
		var t backend.Task
		if err := doc.DataTo(&t); err != nil {
			return nil, err
		}
		seen[t.ClusterID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

func (q *FirestoreTaskStore) GetClaimedTasks(clusterID string) ([]*backend.Task, error) {
	docs, err := q.client.Collection(backend.TaskCollection).
		Where("cluster_id", "==", clusterID).
		Where("status", "==", backend.StatusClaimed).
		Documents(q.ctx).GetAll()
	if err != nil {
		return nil, err
	}
	tasks := make([]*backend.Task, 0, len(docs))
	for _, doc := range docs {
		var t backend.Task
		if err := doc.DataTo(&t); err != nil {
			return nil, err
		}
		t.TaskID = doc.Ref.ID
		tasks = append(tasks, &t)
	}
	return tasks, nil
}

func (q *FirestoreTaskStore) MarkTasksPending(tasks []*backend.Task) error {
	const batchSize = 500
	for i := 0; i < len(tasks); i += batchSize {
		end := i + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		wb := q.client.Batch()
		for _, task := range tasks[i:end] {
			docRef := q.client.Collection(backend.TaskCollection).Doc(task.TaskID)
			wb.Update(docRef, []firestore.Update{
				{Path: "status", Value: backend.StatusPending},
				{Path: "owned_by_worker_id", Value: ""},
			})
		}
		if _, err := wb.Commit(q.ctx); err != nil {
			return err
		}
	}
	return nil
}

func (q *FirestoreTaskStore) GetPendingTaskCount(clusterID string) (int, error) {
	docs, err := q.client.Collection(backend.TaskCollection).
		Where("cluster_id", "==", clusterID).
		Where("status", "==", backend.StatusPending).
		Documents(q.ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return len(docs), nil
}

func (q *FirestoreTaskStore) GetNonCompleteTaskCount(clusterID string) (int, error) {
	docs, err := q.client.Collection(backend.TaskCollection).
		Where("cluster_id", "==", clusterID).
		Where("status", "not-in", []string{backend.StatusComplete, backend.StatusFailed, backend.StatusKilled}).
		Documents(q.ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return len(docs), nil
}

func (q *FirestoreTaskStore) GetTasksCompletedBy(batchJobID string) int {
	docs, err := q.client.Collection(backend.TaskCollection).
		Where("owned_by_batch_job_id", "==", batchJobID).
		Where("status", "==", backend.StatusComplete).
		Documents(q.ctx).GetAll()
	if err != nil {
		log.Printf("GetTasksCompletedBy(%s): %v", batchJobID, err)
		return 0
	}
	return len(docs)
}

// ---- Per-cluster worker methods ----

func (q *FirestoreTaskStore) ClaimTask(ctx context.Context, clusterID string) (*backend.Task, error) {
	maxSleepTime := q.InitialClaimRetry
	claimStart := time.Now()

	for {
		if q.WatchdogNotifier != nil {
			q.WatchdogNotifier()
		}

		tasks, err := q.getPendingTasks(ctx, clusterID, 20)
		if err != nil {
			return nil, err
		}
		if len(tasks) == 0 {
			return nil, nil
		}

		task := tasks[rand.Int31n(int32(len(tasks)))]
		finalTask, err := q.claimTaskByID(ctx, task.TaskID)
		if err == nil {
			return finalTask, nil
		}

		if time.Since(claimStart) > q.ClaimTimeout {
			return nil, errors.New("timed out trying to get task")
		}

		maxSleepTime *= 2
		timeUntilNextTry := time.Duration(rand.Int63n(int64(maxSleepTime)))
		log.Printf("Got error claiming task: %s, will retry after %d milliseconds", err, timeUntilNextTry/time.Millisecond)
		time.Sleep(timeUntilNextTry)
	}
}

func (q *FirestoreTaskStore) getPendingTasks(ctx context.Context, clusterID string, maxFetch int) ([]*backend.Task, error) {
	docs, err := q.client.Collection(backend.TaskCollection).
		Where("cluster", "==", clusterID).
		Where("status", "==", backend.StatusPending).
		Limit(maxFetch).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, err
	}
	tasks := make([]*backend.Task, 0, len(docs))
	for _, doc := range docs {
		var task backend.Task
		if err := doc.DataTo(&task); err != nil {
			return nil, err
		}
		task.TaskID = doc.Ref.ID
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func (q *FirestoreTaskStore) claimTaskByID(ctx context.Context, taskID string) (*backend.Task, error) {
	now := backend.GetTimestampMillis()
	event := backend.TaskHistory{
		Timestamp:       float64(now) / 1000.0,
		Status:          backend.StatusClaimed,
		OwnedByWorkerID: q.workerID,
	}

	mutate := func(task *backend.Task) bool {
		if task.Status != backend.StatusPending {
			log.Printf("Expected status to be pending but was '%s'", task.Status)
			return false
		}
		task.History = append(task.History, &event)
		task.Status = backend.StatusClaimed
		task.OwnedByWorkerID = q.workerID
		task.LastUpdated = float64(now) / 1000.0
		return true
	}

	return q.AtomicUpdateTask(ctx, taskID, mutate)
}

func (q *FirestoreTaskStore) IsJobKilled(ctx context.Context, jobID string) (bool, error) {
	docSnap, err := q.client.Collection(backend.JobCollection).Doc(jobID).Get(ctx)
	if err != nil {
		return false, err
	}
	var job backend.Job
	if err := docSnap.DataTo(&job); err != nil {
		return false, err
	}
	return job.Status == backend.JobStatusKilled, nil
}

func (q *FirestoreTaskStore) GetTask(ctx context.Context, taskID string) (*backend.Task, error) {
	docSnap, err := q.client.Collection(backend.TaskCollection).Doc(taskID).Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting task %s: %w", taskID, err)
	}
	var task backend.Task
	if err := docSnap.DataTo(&task); err != nil {
		return nil, fmt.Errorf("decoding task %s: %w", taskID, err)
	}
	return &task, nil
}

func (q *FirestoreTaskStore) AddJob(ctx context.Context, job *backend.Job, tasks []*backend.Task) error {
	jobRef := q.client.Collection(backend.JobCollection).Doc(job.Name)
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
			docRef := q.client.Collection(backend.TaskCollection).Doc(task.TaskID)
			wb.Set(docRef, task)
		}
		if _, err := wb.Commit(ctx); err != nil {
			return err
		}
		log.Printf("Inserted tasks %d-%d", i, end-1)
	}
	return nil
}

func (q *FirestoreTaskStore) AtomicUpdateTask(ctx context.Context, taskID string, mutateTaskCallback func(task *backend.Task) bool) (*backend.Task, error) {
	var task backend.Task
	client := q.client
	docRef := client.Collection(backend.TaskCollection).Doc(taskID)

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

// ---- TaskCache implementation ----

// FirestoreTaskCache implements backend.TaskCache using Google Cloud Firestore.
type FirestoreTaskCache struct {
	client *firestore.Client
}

func NewFirestoreTaskCache(client *firestore.Client) *FirestoreTaskCache {
	return &FirestoreTaskCache{client: client}
}

func (c *FirestoreTaskCache) GetCachedEntry(ctx context.Context, cacheKey string) (*backend.CachedTaskEntry, error) {
	docSnap, err := c.client.Collection(backend.CachedTaskEntryCollection).Doc(cacheKey).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting cached entry %s: %w", cacheKey, err)
	}
	var entry backend.CachedTaskEntry
	if err := docSnap.DataTo(&entry); err != nil {
		return nil, fmt.Errorf("decoding cached entry %s: %w", cacheKey, err)
	}
	return &entry, nil
}

func (c *FirestoreTaskCache) SetCachedEntry(ctx context.Context, entry *backend.CachedTaskEntry) error {
	_, err := c.client.Collection(backend.CachedTaskEntryCollection).Doc(entry.ID).Set(ctx, entry)
	return err
}

// ---- ClusterTopics / GetCluster (used by consume command) ----

// ClusterTopics holds the Pub/Sub topic names for a cluster, as stored in
// the Firestore V7Cluster collection.
type ClusterTopics struct {
	IncomingTopic string `firestore:"incoming_topic"`
	ResponseTopic string `firestore:"response_topic"`
}

// GetCluster fetches cluster Pub/Sub topic configuration from Firestore.
func GetCluster(ctx context.Context, client *firestore.Client, clusterID string) (*ClusterTopics, error) {
	docSnap, err := client.Collection(backend.ClusterCollection).Doc(clusterID).Get(ctx)
	if err != nil {
		return nil, err
	}
	var cluster ClusterTopics
	if err := docSnap.DataTo(&cluster); err != nil {
		return nil, err
	}
	return &cluster, nil
}
