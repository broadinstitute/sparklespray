package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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

func (r *RedisSparklesMethodsForPoll) getClusterConfig(clusterID string) (Cluster, error) {
	data, err := r.client.Get(r.ctx, r.clusterKey(clusterID)).Bytes()
	if err != nil {
		return Cluster{}, fmt.Errorf("getting cluster %s: %w", clusterID, err)
	}
	var cluster Cluster
	if err := json.Unmarshal(data, &cluster); err != nil {
		return Cluster{}, fmt.Errorf("decoding cluster %s: %w", clusterID, err)
	}
	return cluster, nil
}

func (r *RedisSparklesMethodsForPoll) updateClusterMonitorState(clusterID string, state *MonitorState) error {
	cluster, err := r.getClusterConfig(clusterID)
	if err != nil {
		return err
	}
	wire := monitorStateJSON{
		BatchJobRequests:        state.batchJobRequests,
		CompletedJobIds:         state.completedJobIds,
		SuspiciouslyFailedToRun: state.suspiciouslyFailedToRun,
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

func (r *RedisSparklesMethodsForPoll) getClaimedTasks(clusterID string) ([]*task_queue.Task, error) {
	return r.scanTasksWithFilter(clusterID, func(t *task_queue.Task) bool {
		return t.Status == task_queue.StatusClaimed
	})
}

func (r *RedisSparklesMethodsForPoll) markTasksPending(tasks []*task_queue.Task) error {
	for _, task := range tasks {
		data, err := r.client.Get(r.ctx, r.taskKey(task.TaskID)).Bytes()
		if err != nil {
			return fmt.Errorf("getting task %s: %w", task.TaskID, err)
		}
		var t task_queue.Task
		if err := json.Unmarshal(data, &t); err != nil {
			return fmt.Errorf("decoding task %s: %w", task.TaskID, err)
		}
		t.Status = task_queue.StatusPending
		t.OwnedByWorkerID = ""
		updated, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("encoding task %s: %w", task.TaskID, err)
		}
		if err := r.client.Set(r.ctx, r.taskKey(task.TaskID), updated, 0).Err(); err != nil {
			return fmt.Errorf("storing task %s: %w", task.TaskID, err)
		}
	}
	return nil
}

func (r *RedisSparklesMethodsForPoll) getPendingTaskCount(clusterID string) (int, error) {
	tasks, err := r.scanTasksWithFilter(clusterID, func(t *task_queue.Task) bool {
		return t.Status == task_queue.StatusPending
	})
	if err != nil {
		return 0, err
	}
	return len(tasks), nil
}

func (r *RedisSparklesMethodsForPoll) getNonCompleteTaskCount(clusterID string) (int, error) {
	tasks, err := r.scanTasksWithFilter(clusterID, func(t *task_queue.Task) bool {
		s := t.Status
		return s != task_queue.StatusComplete && s != task_queue.StatusFailed && s != task_queue.StatusKilled
	})
	if err != nil {
		return 0, err
	}
	return len(tasks), nil
}

func (r *RedisSparklesMethodsForPoll) getTasksCompletedBy(batchJobID string) int {
	// Scan all task:* keys since we don't have a per-job index.
	count := 0
	var cursor uint64
	for {
		keys, next, err := r.client.Scan(r.ctx, cursor, "task:*", 100).Result()
		if err != nil {
			log.Printf("getTasksCompletedBy(%s): scan error: %v", batchJobID, err)
			return count
		}
		for _, key := range keys {
			data, err := r.client.Get(r.ctx, key).Bytes()
			if err != nil {
				continue
			}
			var t task_queue.Task
			if err := json.Unmarshal(data, &t); err != nil {
				continue
			}
			if t.OwnedByBatchJobID == batchJobID && t.Status == task_queue.StatusComplete {
				count++
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return count
}

// scanTasksWithFilter fetches all tasks for clusterID and returns those matching the predicate.
func (r *RedisSparklesMethodsForPoll) scanTasksWithFilter(clusterID string, keep func(*task_queue.Task) bool) ([]*task_queue.Task, error) {
	ids, err := r.client.SMembers(r.ctx, r.clusterTasksKey(clusterID)).Result()
	if err != nil {
		return nil, fmt.Errorf("listing task IDs for cluster %s: %w", clusterID, err)
	}
	var result []*task_queue.Task
	for _, id := range ids {
		data, err := r.client.Get(r.ctx, r.taskKey(id)).Bytes()
		if err != nil {
			continue
		}
		var t task_queue.Task
		if err := json.Unmarshal(data, &t); err != nil {
			continue
		}
		if keep(&t) {
			result = append(result, &t)
		}
	}
	return result, nil
}
