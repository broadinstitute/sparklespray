package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/redis/go-redis/v9"
)

// redisBatchJob is the JSON-serialized form of a batch job stored in Redis.
type redisBatchJob struct {
	ID                string                `json:"id"`
	State             backend.BatchJobState `json:"state"`
	ClusterID         string                `json:"cluster_id"`
	Region            string                `json:"region"`
	InstanceCount     int                   `json:"instance_count"`
	WorkerDockerImage string                `json:"worker_docker_image"`
	WorkerCommandArgs []string              `json:"worker_command_args"`
}

// LocalWorkerPool is a Redis-backed implementation of WorkerPool
// intended for local testing only.
//
// Key layout:
//
//	"batch_job:{id}" → JSON-encoded redisBatchJob
type LocalWorkerPool struct {
	client *redis.Client
	ctx    context.Context
}

func NewLocalWorkerPool(ctx context.Context, client *redis.Client) *LocalWorkerPool {
	return &LocalWorkerPool{client: client, ctx: ctx}
}

func (r *LocalWorkerPool) jobKey(id string) string {
	return "batch_job:" + id
}

// ListRunningInstances returns an empty list — no real GCE instances exist in
// local testing, so all claimed tasks will appear orphaned. Need to explictly deal with that in autosizer.
func (r *LocalWorkerPool) ListRunningInstances(clusterID string, region string) ([]string, error) {
	return nil, nil
}

func (r *LocalWorkerPool) SubmitWorkerJobs(CreateWorkerCommand backend.CreateWorkerCommandCallback, cluster *backend.Cluster, clusterID string, requests []*backend.BatchJobsToSubmit) ([]string, error) {
	var jobIDs []string
	for i, req := range requests {

		job := &redisBatchJob{
			ID:                fmt.Sprintf("%s-%d", clusterID, r.nextID(clusterID)),
			State:             backend.Pending,
			ClusterID:         clusterID,
			Region:            cluster.Region,
			InstanceCount:     req.InstanceCount,
			WorkerDockerImage: cluster.WorkerDockerImage,
			WorkerCommandArgs: CreateWorkerCommand(cluster, req.ShouldLinger),
		}

		data, err := json.Marshal(job)
		if err != nil {
			return nil, fmt.Errorf("marshaling job %d: %w", i, err)
		}
		if err := r.client.Set(r.ctx, r.jobKey(job.ID), data, 0).Err(); err != nil {
			return nil, fmt.Errorf("storing job %s: %w", job.ID, err)
		}
		jobIDs = append(jobIDs, job.ID)
	}
	return jobIDs, nil
}

// nextID returns a monotonically incrementing integer for the cluster by
// incrementing a counter key in Redis.
func (r *LocalWorkerPool) nextID(clusterID string) int64 {
	n, _ := r.client.Incr(r.ctx, "batch_job_counter:"+clusterID).Result()
	return n
}

func getAllBatchJobs(ctx context.Context, client *redis.Client) ([]redisBatchJob, error) {
	records, err := getAll(ctx, client, "batch_job:*")
	if err != nil {
		return nil, fmt.Errorf("couldn't get all batch jobs: %w", err)
	}
	jobs := make([]redisBatchJob, len(records))
	for i, data := range records {
		if err := json.Unmarshal(data, &jobs[i]); err != nil {
			return nil, fmt.Errorf("could not parse batch job: %w", err)
		}
	}
	return jobs, nil
}

func (r *LocalWorkerPool) ListBatchJobs(region, clusterID string) ([]*backend.BatchJob, error) {
	all, err := getAllBatchJobs(r.ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("listing batch jobs for cluster %s: %w", clusterID, err)
	}

	var jobs []*backend.BatchJob
	for i := range all {
		job := &all[i]
		if job.ClusterID != clusterID || job.Region != region {
			continue
		}
		jobs = append(jobs, &backend.BatchJob{ID: job.ID, State: job.State, RequestedInstances: job.InstanceCount})
	}
	return jobs, nil
}

func (r *LocalWorkerPool) PutSingletonBatchJob(name, region, machineType string, bootVolumeInGB int64, bootVolumeType, dockerImage string, cmd []string) error {
	// Delete existing job if present
	r.client.Del(r.ctx, r.jobKey(name))

	job := &redisBatchJob{
		ID:                name,
		State:             backend.Pending,
		Region:            region,
		InstanceCount:     1,
		WorkerDockerImage: dockerImage,
		WorkerCommandArgs: cmd,
	}
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshaling singleton batch job %s: %w", name, err)
	}
	if err := r.client.Set(r.ctx, r.jobKey(name), data, 0).Err(); err != nil {
		return fmt.Errorf("storing singleton batch job %s: %w", name, err)
	}
	return nil
}

func (r *LocalWorkerPool) GetBatchJobByName(name string) (*backend.BatchJob, error) {
	data, err := r.client.Get(r.ctx, r.jobKey(name)).Bytes()
	if err == redis.Nil {
		return nil, backend.NoSuchBatchJob
	}
	if err != nil {
		return nil, fmt.Errorf("getting batch job %s: %w", name, err)
	}
	var job redisBatchJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("parsing batch job %s: %w", name, err)
	}
	return &backend.BatchJob{ID: job.ID, State: job.State, RequestedInstances: job.InstanceCount}, nil
}

func (r *LocalWorkerPool) DeleteBatchJob(jobID string) error {
	r.client.Del(r.ctx, r.jobKey(jobID))
	return nil
}

func (r *LocalWorkerPool) DeleteAllBatchJobs(region, clusterID string) error {
	all, err := getAllBatchJobs(r.ctx, r.client)
	if err != nil {
		return fmt.Errorf("listing batch jobs for cluster %s: %w", clusterID, err)
	}

	for i := range all {
		job := &all[i]
		if job.ClusterID != clusterID || job.Region != region {
			continue
		}
		r.client.Del(r.ctx, r.jobKey(job.ID))
	}
	return nil
}
