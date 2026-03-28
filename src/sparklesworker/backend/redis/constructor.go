package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/redis/go-redis/v9"
)

func CreateMockServices(ctx context.Context, redisAddr string, startMockBatchAPI bool) (*backend.ExternalServices, error) {
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to Redis at %s: %w", redisAddr, err)
	}
	channel := NewRedisChannel(redisClient)
	taskCache := task_queue.NewRedisTaskCache(redisClient)

	gshim := NewRedisMethodsForPoll(ctx, redisClient)
	sshim := NewRedisSparklesMethodsForPoll(ctx, redisClient)

	if startMockBatchAPI {
		StartMockBatchAPI(ctx, redisClient, 500*time.Millisecond)
	}

	return &backend.ExternalServices{
		Channel: channel,
		NewQueue: func(clusterID string) task_queue.TaskQueue {
			return task_queue.NewRedisQueue(redisClient, clusterID, "", 0, 0)
		},
		TaskCache: taskCache,
		Close:     func() { redisClient.Close() },
		Gshim:     gshim,
		Sshim:     sshim,
		SparklesWorkerArgs: []string{
			"--localhost",
			"--redisAddr", redisAddr},
	}, nil
}
