package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/redis/go-redis/v9"
)

func CreateMockServices(ctx context.Context, redisAddr string, clusterID string, pollInterval time.Duration) (*backend.ExternalServices, error) {
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to Redis at %s: %w", redisAddr, err)
	}
	channel := NewRedisChannel(redisClient)
	queue := task_queue.NewRedisQueue(redisClient, clusterID, "", 0, 0)
	taskCache := task_queue.NewRedisTaskCache(redisClient)

	StartMockBatchAPI(ctx, redisClient, pollInterval)

	gshim := NewRedisMethodsForPoll(ctx, redisClient)
	sshim := NewRedisSparklesMethodsForPoll(ctx, redisClient)

	return &backend.ExternalServices{Channel: channel, Queue: queue, TaskCache: taskCache, Close: func() { redisClient.Close() },
		Gshim: gshim, Sshim: sshim}, nil
}
