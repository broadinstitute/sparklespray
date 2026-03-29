package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/redis/go-redis/v9"
)

// RedisSparklesMethodsForPoll is a Redis-backed implementation of SparklesMethodsForPoll
// intended for local testing only.
//
// Key layout:
//
//	"cluster:{clusterID}"        → JSON-encoded Cluster
//	"task:{taskID}"              → JSON-encoded task_queue.Task
//	"tasks:{clusterID}"          → Redis SET of task IDs belonging to that cluster
type RedisSparklesMethodsForPoll struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisSparklesMethodsForPoll(ctx context.Context, client *redis.Client) *RedisSparklesMethodsForPoll {
	return &RedisSparklesMethodsForPoll{client: client, ctx: ctx}
}

func (r *RedisSparklesMethodsForPoll) clusterKey(clusterID string) string {
	return "cluster:" + clusterID
}

func (r *RedisSparklesMethodsForPoll) taskKey(taskID string) string {
	return "task:" + taskID
}

func (r *RedisSparklesMethodsForPoll) clusterTasksKey(clusterID string) string {
	return "tasks:" + clusterID
}

func (r *RedisSparklesMethodsForPoll) GetClusterConfig(clusterID string) (*backend.Cluster, error) {
	data, err := r.client.Get(r.ctx, r.clusterKey(clusterID)).Bytes()
	if err != nil {
		return nil, fmt.Errorf("getting cluster %s: %w", clusterID, err)
	}
	var cluster backend.Cluster
	if err := json.Unmarshal(data, &cluster); err != nil {
		return nil, fmt.Errorf("decoding cluster %s: %w", clusterID, err)
	}
	return &cluster, nil
}

func (r *RedisSparklesMethodsForPoll) SetClusterConfig(clusterID string, cluster backend.Cluster) error {
	data, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("encoding cluster %s: %w", clusterID, err)
	}
	return r.client.Set(r.ctx, r.clusterKey(clusterID), data, 0).Err()
}

func (r *RedisSparklesMethodsForPoll) UpdateClusterMonitorState(clusterID string, state *backend.MonitorState) error {
	cluster, err := r.GetClusterConfig(clusterID)
	if err != nil {
		return err
	}
	wire := backend.MonitorStateJSON{
		BatchJobRequests:        state.BatchJobRequests,
		CompletedJobIds:         state.CompletedJobIds,
		SuspiciouslyFailedToRun: state.SuspiciouslyFailedToRun,
	}
	stateData, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("marshaling monitor state: %w", err)
	}
	cluster.MonitorState = string(stateData)
	clusterData, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("marshaling cluster: %w", err)
	}
	return r.client.Set(r.ctx, r.clusterKey(clusterID), clusterData, 0).Err()
}

func (r *RedisSparklesMethodsForPoll) GetClaimedTasks(clusterID string) ([]*task_queue.Task, error) {
	return r.scanTasksWithFilter(clusterID, func(t *task_queue.Task) bool {
		return t.Status == task_queue.StatusClaimed
	})
}

func (r *RedisSparklesMethodsForPoll) MarkTasksPending(tasks []*task_queue.Task) error {
	for _, task := range tasks {
		q := task_queue.NewRedisQueue(r.client, task.ClusterID, "", 0, 0)
		_, err := q.AtomicUpdateTask(r.ctx, task.TaskID, func(t *task_queue.Task) bool {
			t.Status = task_queue.StatusPending
			t.OwnedByWorkerID = ""
			return true
		})
		if err != nil {
			return fmt.Errorf("marking task %s pending: %w", task.TaskID, err)
		}
	}
	return nil
}

func (r *RedisSparklesMethodsForPoll) GetPendingTaskCount(clusterID string) (int, error) {
	tasks, err := r.scanTasksWithFilter(clusterID, func(t *task_queue.Task) bool {
		return t.Status == task_queue.StatusPending
	})
	if err != nil {
		return 0, err
	}
	return len(tasks), nil
}

func (r *RedisSparklesMethodsForPoll) GetNonCompleteTaskCount(clusterID string) (int, error) {
	tasks, err := r.scanTasksWithFilter(clusterID, func(t *task_queue.Task) bool {
		s := t.Status
		return s != task_queue.StatusComplete && s != task_queue.StatusFailed && s != task_queue.StatusKilled
	})
	if err != nil {
		return 0, err
	}
	return len(tasks), nil
}

func getAll(ctx context.Context, client *redis.Client, pattern string) ([][]byte, error) {
	var cursor uint64
	var result [][]byte
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scanning tasks: %w", err)
		}
		for _, key := range keys {
			data, err := client.Get(ctx, key).Bytes()
			if err != nil {
				continue
			}
			result = append(result, data)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return result, nil
}

func getAllTasks(ctx context.Context, client *redis.Client) ([]task_queue.Task, error) {
	records, err := getAll(ctx, client, "task:*")
	tasks := make([]task_queue.Task, len(records))
	if err != nil {
		return nil, fmt.Errorf("Couldn't get all tasks: %s", err)
	}
	for i, data := range records {
		if err := json.Unmarshal(data, &tasks[i]); err != nil {
			return nil, fmt.Errorf("Could not parse task: %s", data)
		}
	}
	return tasks, nil
}

func (r *RedisSparklesMethodsForPoll) GetActiveClusterIDs() ([]string, error) {
	seen := make(map[string]struct{})

	tasks, err := getAllTasks(r.ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("Couldn't get all tasks: %s", err)
	}
	for i := range tasks {
		t := &tasks[i]
		if t.Status == task_queue.StatusClaimed || t.Status == task_queue.StatusPending {
			seen[t.ClusterID] = struct{}{}
		}
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

func (r *RedisSparklesMethodsForPoll) GetTasksCompletedBy(batchJobID string) int {
	// Scan all task:* keys since we don't have a per-job index.
	count := 0
	tasks, err := getAllTasks(r.ctx, r.client)
	if err != nil {
		log.Printf("getTasksCompletedBy(%s): scan error: %v", batchJobID, err)
		return count
	}
	for i := range tasks {
		t := tasks[i]
		if t.OwnedByBatchJobID == batchJobID && t.Status == task_queue.StatusComplete {
			count++
		}
	}
	return count
}

// scanTasksWithFilter fetches all tasks for clusterID and returns those matching the predicate.
func (r *RedisSparklesMethodsForPoll) scanTasksWithFilter(clusterID string, keep func(*task_queue.Task) bool) ([]*task_queue.Task, error) {
	tasks, err := getAllTasks(r.ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("listing task IDs for cluster %s: %w", clusterID, err)
	}
	var result []*task_queue.Task
	for i := range tasks {
		t := &tasks[i]
		if keep(t) {
			result = append(result, t)
		}
	}
	return result, nil
}
