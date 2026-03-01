package task_queue

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	"cloud.google.com/go/datastore"
)

const TaskCollection = "SparklesV6Task"
const ClusterCollection = "SparklesV6Cluster"
const JobCollection = "SparklesV6Job"

const InitialClaimRetryDelay = 1000

// Cluster represents cluster configuration stored in Datastore
type Cluster struct {
	IncomingTopic string `datastore:"incoming_topic"`
	ResponseTopic string `datastore:"response_topic"`
}

// DataStoreQueue implements TaskQueue using Google Cloud Datastore
type DataStoreQueue struct {
	client            *datastore.Client
	cluster           string
	workerID          string
	InitialClaimRetry time.Duration
	ClaimTimeout      time.Duration
	WatchdogNotifier  func() // Called periodically during long operations
}

// NewDataStoreQueue creates a new DataStoreQueue
func NewDataStoreQueue(client *datastore.Client, cluster string, workerID string, initialClaimRetry time.Duration, claimTimeout time.Duration) *DataStoreQueue {
	return &DataStoreQueue{
		client:            client,
		cluster:           cluster,
		workerID:          workerID,
		InitialClaimRetry: initialClaimRetry,
		ClaimTimeout:      claimTimeout,
		WatchdogNotifier:  func() {}, // No-op by default
	}
}

// GetCluster fetches cluster configuration from Datastore
func GetCluster(ctx context.Context, client *datastore.Client, clusterID string) (*Cluster, error) {
	clusterKey := datastore.NameKey(ClusterCollection, clusterID, nil)
	var cluster Cluster
	err := client.Get(ctx, clusterKey, &cluster)
	if err != nil {
		return nil, err
	}
	return &cluster, nil
}

func getTasks(ctx context.Context, client *datastore.Client, cluster string, status string, maxFetch int) ([]*Task, error) {
	q := datastore.NewQuery(TaskCollection).FilterField("cluster", "=", cluster).FilterField("status", "=", status).Limit(maxFetch)
	var tasks []*Task
	keys, err := client.GetAll(ctx, q, &tasks)

	if err != nil {
		return nil, err
	}

	for i, key := range keys {
		tasks[i].TaskID = key.Name
	}

	return tasks, nil
}

func getTimestampMillis() int64 {
	return int64(time.Now().UnixNano()) / int64(time.Millisecond)
}

// ClaimTask attempts to claim a pending task from the queue
func (q *DataStoreQueue) ClaimTask(ctx context.Context) (*Task, error) {
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

func (q *DataStoreQueue) claimTaskByID(ctx context.Context, taskID string) (*Task, error) {
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
func (q *DataStoreQueue) IsJobKilled(ctx context.Context, jobID string) (bool, error) {
	jobKey := datastore.NameKey(JobCollection, jobID, nil)
	var job Job
	err := q.client.Get(ctx, jobKey, &job)
	if err != nil {
		return false, err
	}

	return job.Status == JobStatusKilled, nil
}

// AtomicUpdateTask updates a task atomically using the provided callback
func (q *DataStoreQueue) AtomicUpdateTask(ctx context.Context, taskID string, mutateTaskCallback func(task *Task) bool) (*Task, error) {
	var task Task
	client := q.client

	log.Printf("AtomicUpdateTask of task %v", taskID)
	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		log.Printf("attempting update of task %s start", taskID)

		taskKey := datastore.NameKey(TaskCollection, taskID, nil)
		err := tx.Get(taskKey, &task)
		if err != nil {
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
		log.Printf("Calling put on task %s with version %d", task.TaskID, task.Version)
		_, err = tx.Put(taskKey, &task)
		if err != nil {
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
