package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/redis/go-redis/v9"
)

// RedisClusterStore is a Redis-backed implementation of ClusterStore
// intended for local testing only.
//
// Key layout:
//
//	"cluster:{clusterID}" → JSON-encoded Cluster
type RedisClusterStore struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisClusterStore(ctx context.Context, client *redis.Client) *RedisClusterStore {
	return &RedisClusterStore{client: client, ctx: ctx}
}

func (r *RedisClusterStore) clusterKey(clusterID string) string {
	return "cluster:" + clusterID
}

func (r *RedisClusterStore) GetClusterConfig(clusterID string) (*backend.Cluster, error) {
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

func (r *RedisClusterStore) SetClusterConfig(clusterID string, cluster backend.Cluster) error {
	data, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("encoding cluster %s: %w", clusterID, err)
	}
	return r.client.Set(r.ctx, r.clusterKey(clusterID), data, 0).Err()
}

func (r *RedisClusterStore) UpdateClusterMonitorState(clusterID string, state *backend.MonitorState) error {
	cluster, err := r.GetClusterConfig(clusterID)
	if err != nil {
		return err
	}
	wire := backend.MonitorStateJSON{
		BatchJobRequests:          state.BatchJobRequests,
		SuspiciouslyFailingJobIds: state.SuspiciouslyFailingJobIds,
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

// getAll scans all Redis keys matching pattern and returns their raw values.
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
