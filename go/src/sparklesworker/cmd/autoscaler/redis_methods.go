package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// redisBatchJob is the JSON-serialized form of a batch job stored in Redis.
type redisBatchJob struct {
	ID                string        `json:"id"`
	State             BatchJobState `json:"state"`
	ClusterID         string        `json:"cluster_id"`
	Region            string        `json:"region"`
	InstanceCount     int           `json:"instance_count"`
	WorkerCommandArgs []string      `json:"worker_command_args"`
	WorkerDockerImage string        `json:"worker_docker_image"`
}

// RedisMethodsForPoll is a Redis-backed implementation of CloudMethodsForPoll
// intended for local testing only.
//
// Key layout:
//
//	"batch_job:{id}"        → JSON-encoded redisBatchJob
//	"batch_jobs:{clusterID}" → Redis SET of job IDs belonging to that cluster
type RedisMethodsForPoll struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisMethodsForPoll(ctx context.Context, client *redis.Client) *RedisMethodsForPoll {
	return &RedisMethodsForPoll{client: client, ctx: ctx}
}

func (r *RedisMethodsForPoll) jobKey(id string) string {
	return "batch_job:" + id
}

func (r *RedisMethodsForPoll) clusterJobsKey(clusterID string) string {
	return "batch_jobs:" + clusterID
}

// listRunningInstances returns an empty list — no real GCE instances exist in
// local testing, so all claimed tasks will appear orphaned.
func (r *RedisMethodsForPoll) listRunningInstances(zones []string, clusterID string) ([]string, error) {
	return nil, nil
}

func (r *RedisMethodsForPoll) submitBatchJobs(cluster Cluster, clusterID string, requests []*BatchJobsToSubmit) error {
	for i, req := range requests {

		job := &redisBatchJob{
			ID:                fmt.Sprintf("%s-%d", clusterID, r.nextID(clusterID)),
			State:             Pending,
			ClusterID:         clusterID,
			Region:            cluster.Region,
			InstanceCount:     req.instanceCount,
			WorkerCommandArgs: cluster.WorkerCommandArgs,
			WorkerDockerImage: cluster.WorkerDockerImage}

		data, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("marshaling job %d: %w", i, err)
		}
		if err := r.client.Set(r.ctx, r.jobKey(job.ID), data, 0).Err(); err != nil {
			return fmt.Errorf("storing job %s: %w", job.ID, err)
		}
		if err := r.client.SAdd(r.ctx, r.clusterJobsKey(clusterID), job.ID).Err(); err != nil {
			return fmt.Errorf("indexing job %s: %w", job.ID, err)
		}
	}
	return nil
}

// nextID returns a monotonically incrementing integer for the cluster by
// incrementing a counter key in Redis.
func (r *RedisMethodsForPoll) nextID(clusterID string) int64 {
	n, _ := r.client.Incr(r.ctx, "batch_job_counter:"+clusterID).Result()
	return n
}

func (r *RedisMethodsForPoll) listBatchJobs(region, clusterID string) ([]*BatchJob, error) {
	ids, err := r.client.SMembers(r.ctx, r.clusterJobsKey(clusterID)).Result()
	if err != nil {
		return nil, fmt.Errorf("listing job IDs for cluster %s: %w", clusterID, err)
	}

	var jobs []*BatchJob
	for _, id := range ids {
		data, err := r.client.Get(r.ctx, r.jobKey(id)).Bytes()
		if err != nil {
			return nil, fmt.Errorf("fetching job %s: %w", id, err)
		}
		var job redisBatchJob
		if err := json.Unmarshal(data, &job); err != nil {
			return nil, fmt.Errorf("decoding job %s: %w", id, err)
		}
		if job.Region != region {
			continue
		}
		jobs = append(jobs, &BatchJob{ID: job.ID, State: job.State, RequestedInstances: job.InstanceCount})
	}
	return jobs, nil
}

func (r *RedisMethodsForPoll) deleteAllBatchJobs(region, clusterID string) error {
	ids, err := r.client.SMembers(r.ctx, r.clusterJobsKey(clusterID)).Result()
	if err != nil {
		return fmt.Errorf("listing job IDs for cluster %s: %w", clusterID, err)
	}

	for _, id := range ids {
		// Only delete jobs in the requested region.
		data, err := r.client.Get(r.ctx, r.jobKey(id)).Bytes()
		if err != nil {
			continue // already gone
		}
		var job redisBatchJob
		if err := json.Unmarshal(data, &job); err != nil {
			continue
		}
		if job.Region != region {
			continue
		}
		r.client.Del(r.ctx, r.jobKey(id))
		r.client.SRem(r.ctx, r.clusterJobsKey(clusterID), id)
	}
	return nil
}
