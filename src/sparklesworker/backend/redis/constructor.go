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
		NewTaskStore: func(clusterID string) backend.TaskStore {
			return NewRedisTaskStore(redisClient, clusterID, "", 0, 0)
		},
		TaskCache: taskCache,
		Close:     func() { redisClient.Close() },
		Compute:   compute,
		Cluster:   cluster,
		Tasks:     tasks,
		CreateWorkerCommand: func(clusterID string, shouldLinger bool, aetherConfig *backend.AetherConfig) []string {
			return backend.CreateWorkerCommand(clusterID, false, []string{
				"--localhost",
				"--redisAddr", redisAddr}, aetherConfig)
		}}, nil
}
