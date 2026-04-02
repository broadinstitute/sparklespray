package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/redis/go-redis/v9"
)

func CreateMockServices(ctx context.Context, redisAddr string, startMockBatchAPI bool) (*backend.ExternalServices, error) {
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to Redis at %s: %w", redisAddr, err)
	}
	channel := NewRedisChannel(redisClient)
	taskCache := NewRedisTaskCache(redisClient)

	compute := NewLocalWorkerPool(ctx, redisClient)
	cluster := NewRedisClusterStore(ctx, redisClient)
	tasks := newRedisTaskStoreGlobal(ctx, redisClient)

	if startMockBatchAPI {
		StartMockBatchAPI(ctx, redisClient, 500*time.Millisecond)
	}

	return &backend.ExternalServices{
		Channel: channel,
			TaskCache: taskCache,
		Close:     func() { redisClient.Close() },
		Compute:   compute,
		Cluster:   cluster,
		Tasks:     tasks,
		CreateWorkerCommand: func(cluster *backend.Cluster, shouldLinger bool) []string {
			return backend.CreateWorkerCommand(cluster, false, []string{
				"--localhost",
				"--redisAddr", redisAddr})
		}}, nil
}
