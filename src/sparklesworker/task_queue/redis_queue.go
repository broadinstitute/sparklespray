package task_queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisQueue implements TaskQueue using Redis
type RedisQueue struct {
	client            *redis.Client
	cluster           string
	workerID          string
	InitialClaimRetry time.Duration
	ClaimTimeout      time.Duration
	WatchdogNotifier  func() // Called periodically during long operations
}

// NewRedisQueue creates a new RedisQueue
func NewRedisQueue(client *redis.Client, cluster string, workerID string, initialClaimRetry time.Duration, claimTimeout time.Duration) *RedisQueue {
	return &RedisQueue{
		client:            client,
		cluster:           cluster,
		workerID:          workerID,
		InitialClaimRetry: initialClaimRetry,
		ClaimTimeout:      claimTimeout,
		WatchdogNotifier:  func() {}, // No-op by default
	}
}

// Redis key helpers
func (q *RedisQueue) taskKey(taskID string) string {
	return fmt.Sprintf("task:%s", taskID)
}

func (q *RedisQueue) jobKey(jobID string) string {
	return fmt.Sprintf("job:%s", jobID)
}

// scanPendingTaskIDs scans all task:* keys and returns IDs of tasks that are
// pending and belong to this queue's cluster.
func (q *RedisQueue) scanPendingTaskIDs(ctx context.Context) ([]string, error) {
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
			var task Task
			if err := json.Unmarshal(data, &task); err != nil {
				continue
			}
			if task.Status == StatusPending && task.ClusterID == q.cluster {
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

// ClaimTask attempts to claim a pending task from the queue
func (q *RedisQueue) ClaimTask(ctx context.Context) (*Task, error) {
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

		// Pick a random task to avoid contention
		taskID := taskIDs[rand.Intn(len(taskIDs))]

		finalTask, err := q.claimTaskByID(ctx, taskID)
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

func (q *RedisQueue) claimTaskByID(ctx context.Context, taskID string) (*Task, error) {
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
func (q *RedisQueue) IsJobKilled(ctx context.Context, jobID string) (bool, error) {
	status, err := q.client.HGet(ctx, q.jobKey(jobID), "status").Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return status == JobStatusKilled, nil
}

// GetTask retrieves a task by ID from Redis.
func (q *RedisQueue) GetTask(ctx context.Context, taskID string) (*Task, error) {
	taskJSON, err := q.client.Get(ctx, q.taskKey(taskID)).Result()
	if err != nil {
		return nil, fmt.Errorf("getting task %s: %w", taskID, err)
	}
	var task Task
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		return nil, fmt.Errorf("decoding task %s: %w", taskID, err)
	}
	return &task, nil
}

// AddJob inserts a job and its tasks into Redis.
func (q *RedisQueue) AddJob(ctx context.Context, job *Job, tasks []*Task) error {
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

// RedisTaskCache implements TaskCache using Redis.
type RedisTaskCache struct {
	client *redis.Client
}

func NewRedisTaskCache(client *redis.Client) *RedisTaskCache {
	return &RedisTaskCache{client: client}
}

func (c *RedisTaskCache) cacheKey(key string) string {
	return fmt.Sprintf("cache:%s", key)
}

func (c *RedisTaskCache) GetCachedEntry(ctx context.Context, cacheKey string) (*CachedTaskEntry, error) {
	data, err := c.client.Get(ctx, c.cacheKey(cacheKey)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting cache entry %s: %w", cacheKey, err)
	}
	var entry CachedTaskEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("decoding cache entry %s: %w", cacheKey, err)
	}
	return &entry, nil
}

func (c *RedisTaskCache) SetCachedEntry(ctx context.Context, entry *CachedTaskEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encoding cache entry: %w", err)
	}
	ttl := time.Until(entry.Expiry)
	if ttl <= 0 {
		ttl = 0 // no expiry
	}
	return c.client.Set(ctx, c.cacheKey(entry.ID), data, ttl).Err()
}

// AtomicUpdateTask updates a task atomically using Redis WATCH/MULTI/EXEC
func (q *RedisQueue) AtomicUpdateTask(ctx context.Context, taskID string, mutateTaskCallback func(task *Task) bool) (*Task, error) {
	taskKey := q.taskKey(taskID)

	var updatedTask *Task
	err := q.client.Watch(ctx, func(tx *redis.Tx) error {
		// Get current task
		taskJSON, err := tx.Get(ctx, taskKey).Result()
		if err != nil {
			return err
		}

		var task Task
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
