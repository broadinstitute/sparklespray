// Compatibility shim — constructors and types delegating to backend/redis.
package task_queue

import (
	"time"

	redis_backend "github.com/broadinstitute/sparklesworker/backend/redis"
	"github.com/redis/go-redis/v9"
)

// RedisQueue is an alias for the implementation in backend/redis.
type RedisQueue = redis_backend.RedisTaskStore

// NewRedisQueue creates a per-cluster RedisTaskStore for worker use.
func NewRedisQueue(client *redis.Client, cluster string, workerID string, initialClaimRetry time.Duration, claimTimeout time.Duration) *RedisQueue {
	return redis_backend.NewRedisTaskStore(client, cluster, workerID, initialClaimRetry, claimTimeout)
}

// RedisTaskCache is an alias for the implementation in backend/redis.
type RedisTaskCache = redis_backend.RedisTaskCache

// NewRedisTaskCache creates a new RedisTaskCache.
func NewRedisTaskCache(client *redis.Client) *RedisTaskCache {
	return redis_backend.NewRedisTaskCache(client)
}
