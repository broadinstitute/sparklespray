package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func makeRedisQueue(t *testing.T, mr *miniredis.Miniredis, cluster string) *task_queue.RedisQueue {
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return task_queue.NewRedisQueue(client, cluster, "test-worker", 10*time.Millisecond, 5*time.Second)
}

func TestClaimAndComplete3Tasks(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()

	q := makeRedisQueue(t, mr, "test-cluster")

	job := &task_queue.Job{
		Name:      "test-job",
		TaskCount: 3,
		Status:    "pending",
	}
	tasks := make([]*task_queue.Task, 3)
	for i := 0; i < 3; i++ {
		tasks[i] = &task_queue.Task{
			TaskID:    fmt.Sprintf("task-%d", i),
			TaskIndex: int64(i),
			JobID:     job.Name,
			ClusterID: "test-cluster",
			Status:    task_queue.StatusPending,
			TaskSpec:  &task_queue.TaskSpec{Command: []string{fmt.Sprintf("cmd-%d", i)}},
			History: []*task_queue.TaskHistory{
				{Timestamp: float64(time.Now().UnixMilli()) / 1000.0, Status: task_queue.StatusPending},
			},
		}
	}

	require.NoError(t, q.AddJob(ctx, job, tasks))

	processed := 0
	for {
		task, err := q.ClaimTask(ctx, "test-cluster")
		require.NoError(t, err)
		if task == nil {
			break
		}

		_, err = UpdateTaskCompleted(ctx, q, task.TaskID, "0", "", "", "")
		require.NoError(t, err)

		processed++
	}

	require.Equal(t, 3, processed, "expected all 3 tasks to be processed")
}
