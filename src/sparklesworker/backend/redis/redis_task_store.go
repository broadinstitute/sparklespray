package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/redis/go-redis/v9"
)

// RedisTaskStore implements backend.TaskStore using Redis.
// It supports both per-cluster worker operations and cross-cluster monitor queries.
type RedisTaskStore struct {
	client            *redis.Client
	ctx               context.Context
	cluster           string
	workerID          string
	InitialClaimRetry time.Duration
	ClaimTimeout      time.Duration
	WatchdogNotifier  func()
}

// NewRedisTaskStore creates a per-cluster RedisTaskStore for worker use.
func NewRedisTaskStore(client *redis.Client, cluster string, workerID string, initialClaimRetry time.Duration, claimTimeout time.Duration) *RedisTaskStore {
	return &RedisTaskStore{
		client:            client,
		ctx:               context.Background(),
		cluster:           cluster,
		workerID:          workerID,
		InitialClaimRetry: initialClaimRetry,
		ClaimTimeout:      claimTimeout,
		WatchdogNotifier:  func() {},
	}
}

// newRedisTaskStoreGlobal creates a RedisTaskStore for autoscaler use
// (no cluster bound; cross-cluster query methods accept clusterID as a parameter).
func newRedisTaskStoreGlobal(ctx context.Context, client *redis.Client) *RedisTaskStore {
	return &RedisTaskStore{
		client:           client,
		ctx:              ctx,
		WatchdogNotifier: func() {},
	}
}

func getAllTasks(ctx context.Context, client *redis.Client) ([]backend.Task, error) {
	records, err := getAll(ctx, client, "task:*")
	if err != nil {
		return nil, fmt.Errorf("couldn't get all tasks: %w", err)
	}
	tasks := make([]backend.Task, len(records))
	for i, data := range records {
		if err := json.Unmarshal(data, &tasks[i]); err != nil {
			return nil, fmt.Errorf("could not parse task: %w", err)
		}
	}
	return tasks, nil
}

// ---- Redis key helpers ----

func (q *RedisTaskStore) taskKey(taskID string) string {
	return fmt.Sprintf("task:%s", taskID)
}

func (q *RedisTaskStore) jobKey(jobID string) string {
	return fmt.Sprintf("job:%s", jobID)
}

// ---- Cross-cluster monitor query methods ----

func (q *RedisTaskStore) GetClusterIDsFromActiveTasks() ([]string, error) {
	seen := make(map[string]struct{})
	tasks, err := getAllTasks(q.ctx, q.client)
	if err != nil {
		return nil, fmt.Errorf("couldn't get all tasks: %s", err)
	}
	for i := range tasks {
		t := &tasks[i]
		if t.Status == backend.StatusClaimed || t.Status == backend.StatusPending {
			seen[t.ClusterID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

func (q *RedisTaskStore) GetClaimedTasks(clusterID string) ([]*backend.Task, error) {
	return q.scanTasksWithFilter(clusterID, func(t *backend.Task) bool {
		return t.Status == backend.StatusClaimed
	})
}

func (q *RedisTaskStore) MarkTasksPending(tasks []*backend.Task) error {
	for _, task := range tasks {
		_, err := q.AtomicUpdateTask(q.ctx, task.TaskID, func(t *backend.Task) bool {
			t.Status = backend.StatusPending
			t.OwnedByWorkerID = ""
			return true
		})
		if err != nil {
			return fmt.Errorf("marking task %s pending: %w", task.TaskID, err)
		}
	}
	return nil
}

func (q *RedisTaskStore) GetPendingTaskCount(clusterID string) (int, error) {
	tasks, err := q.scanTasksWithFilter(clusterID, func(t *backend.Task) bool {
		return t.Status == backend.StatusPending
	})
	if err != nil {
		return 0, err
	}
	return len(tasks), nil
}

func (q *RedisTaskStore) GetNonCompleteTaskCount(clusterID string) (int, error) {
	tasks, err := q.scanTasksWithFilter(clusterID, func(t *backend.Task) bool {
		s := t.Status
		return s != backend.StatusComplete && s != backend.StatusFailed && s != backend.StatusKilled
	})
	if err != nil {
		return 0, err
	}
	return len(tasks), nil
}

func (q *RedisTaskStore) GetTasksCompletedBy(batchJobID string) int {
	count := 0
	tasks, err := getAllTasks(q.ctx, q.client)
	if err != nil {
		log.Printf("GetTasksCompletedBy(%s): scan error: %v", batchJobID, err)
		return count
	}
	for i := range tasks {
		t := tasks[i]
		if t.OwnedByBatchJobID == batchJobID && t.Status == backend.StatusComplete {
			count++
		}
	}
	return count
}

func (q *RedisTaskStore) scanTasksWithFilter(clusterID string, keep func(*backend.Task) bool) ([]*backend.Task, error) {
	tasks, err := getAllTasks(q.ctx, q.client)
	if err != nil {
		return nil, fmt.Errorf("listing task IDs for cluster %s: %w", clusterID, err)
	}
	var result []*backend.Task
	for i := range tasks {
		t := &tasks[i]
		if keep(t) {
			result = append(result, t)
		}
	}
	return result, nil
}

// ---- Per-cluster worker methods ----

func (q *RedisTaskStore) ClaimTask(ctx context.Context) (*backend.Task, error) {
	maxSleepTime := q.InitialClaimRetry
	claimStart := time.Now()

	for {
		if q.WatchdogNotifier != nil {
			q.WatchdogNotifier()
		}

		taskIDs, err := q.scanPendingTaskIDs(ctx)
		if err != nil {
			return nil, err
		}
		if len(taskIDs) == 0 {
			return nil, nil
		}

		taskID := taskIDs[rand.Intn(len(taskIDs))]
		finalTask, err := q.claimTaskByID(ctx, taskID)
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

func (q *RedisTaskStore) scanPendingTaskIDs(ctx context.Context) ([]string, error) {
	var taskIDs []string
	var cursor uint64
	for {
		keys, next, err := q.client.Scan(ctx, cursor, "task:*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			data, err := q.client.Get(ctx, key).Bytes()
			if err != nil {
				continue
			}
			var task backend.Task
			if err := json.Unmarshal(data, &task); err != nil {
				continue
			}
			if task.Status == backend.StatusPending && task.ClusterID == q.cluster {
				taskIDs = append(taskIDs, task.TaskID)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return taskIDs, nil
}

func (q *RedisTaskStore) claimTaskByID(ctx context.Context, taskID string) (*backend.Task, error) {
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

func (q *RedisTaskStore) IsJobKilled(ctx context.Context, jobID string) (bool, error) {
	s, err := q.client.HGet(ctx, q.jobKey(jobID), "status").Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return s == backend.JobStatusKilled, nil
}

func (q *RedisTaskStore) GetTask(ctx context.Context, taskID string) (*backend.Task, error) {
	taskJSON, err := q.client.Get(ctx, q.taskKey(taskID)).Result()
	if err != nil {
		return nil, fmt.Errorf("getting task %s: %w", taskID, err)
	}
	var task backend.Task
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		return nil, fmt.Errorf("decoding task %s: %w", taskID, err)
	}
	return &task, nil
}

func (q *RedisTaskStore) AddJob(ctx context.Context, job *backend.Job, tasks []*backend.Task) error {
	pipe := q.client.Pipeline()
	pipe.HSet(ctx, q.jobKey(job.Name), "status", job.Status)
	for _, task := range tasks {
		taskJSON, err := json.Marshal(task)
		if err != nil {
			return err
		}
		pipe.Set(ctx, q.taskKey(task.TaskID), taskJSON, 0)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisTaskStore) AtomicUpdateTask(ctx context.Context, taskID string, mutateTaskCallback func(task *backend.Task) bool) (*backend.Task, error) {
	taskKey := q.taskKey(taskID)

	var updatedTask *backend.Task
	err := q.client.Watch(ctx, func(tx *redis.Tx) error {
		taskJSON, err := tx.Get(ctx, taskKey).Result()
		if err != nil {
			return err
		}

		var task backend.Task
		if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
			return err
		}
		task.TaskID = taskID

		log.Printf("Calling mutate on task %s with version %d", task.TaskID, task.Version)
		if !mutateTaskCallback(&task) {
			log.Printf("Update failed on task %s", task.TaskID)
			return errors.New("update failed")
		}

		task.Version = task.Version + 1

		newTaskJSON, err := json.Marshal(&task)
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, taskKey, newTaskJSON, 0)
			return nil
		})
		if err != nil {
			return err
		}

		updatedTask = &task
		return nil
	}, taskKey)

	if err != nil {
		return nil, err
	}

	return updatedTask, nil
}

// ---- RedisTaskCache ----

// RedisTaskCache implements backend.TaskCache using Redis.
type RedisTaskCache struct {
	client *redis.Client
}

func NewRedisTaskCache(client *redis.Client) *RedisTaskCache {
	return &RedisTaskCache{client: client}
}

func (c *RedisTaskCache) cacheKey(key string) string {
	return fmt.Sprintf("cache:%s", key)
}

func (c *RedisTaskCache) GetCachedEntry(ctx context.Context, cacheKey string) (*backend.CachedTaskEntry, error) {
	data, err := c.client.Get(ctx, c.cacheKey(cacheKey)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting cache entry %s: %w", cacheKey, err)
	}
	var entry backend.CachedTaskEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("decoding cache entry %s: %w", cacheKey, err)
	}
	return &entry, nil
}

func (c *RedisTaskCache) SetCachedEntry(ctx context.Context, entry *backend.CachedTaskEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encoding cache entry: %w", err)
	}
	ttl := time.Until(entry.Expiry)
	if ttl <= 0 {
		ttl = 0
	}
	return c.client.Set(ctx, c.cacheKey(entry.ID), data, ttl).Err()
}
