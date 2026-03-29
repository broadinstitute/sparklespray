package gcp

import (
	"context"
	"fmt"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/batch/apiv1/batchpb"
	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/alicebob/miniredis/v2/proto"
	"github.com/broadinstitute/sparklesworker/backend"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GCPMethodsForPoll struct {
	projectID       string
	ctx             context.Context
	instancesClient *compute.InstancesClient
	zoneClient      *compute.ZoneOperationsClient
	batchClient     *batch.Client
}

func (g *GCPMethodsForPoll) getZonesForRegion(region string) ([]string, error) {
	panic("unimp")
}

func (g *GCPMethodsForPoll) ListRunningInstances(clusterID string, region string) ([]string, error) {
	zones, err := g.getZonesForRegion(region)
	if err != nil {
		return nil, fmt.Errorf("listing zones in region %s: %w", region, err)
	}

	var instanceNames []string

	filter := proto.String(fmt.Sprintf(`labels.sparkles-cluster-uuid = "%s"`, clusterID))

	for _, zone := range zones {
		req := &computepb.ListInstancesRequest{
			Project: g.projectID,
			Zone:    zone,
			Filter:  &filter,
		}

		it := g.instancesClient.List(g.ctx, req)
		for {
			instance, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("listing instances in zone %s: %w", zone, err)
			}
			instanceNames = append(instanceNames, instance.GetName())
		}
	}

	return instanceNames, nil
}

func (g *GCPMethodsForPoll) ListBatchJobs(region, clusterID string) ([]*backend.BatchJob, error) {
	req := &batchpb.ListJobsRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", g.projectID, region),
		Filter: fmt.Sprintf(`labels.sparkles-cluster = "%s"`, clusterID),
	}

	var jobs []*backend.BatchJob
	it := g.batchClient.ListJobs(g.ctx, req)
	for {
		job, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing batch jobs: %w", err)
		}
		instanceCount := 0
		for _, tg := range job.GetTaskGroups() {
			instanceCount += int(tg.GetTaskCount())
		}
		jobs = append(jobs, &backend.BatchJob{ID: job.GetName(), State: batchStateToBatchJobState(job), RequestedInstances: instanceCount})
	}
	return jobs, nil
}

func batchStateToBatchJobState(job *batchpb.Job) backend.BatchJobState {
	switch job.GetStatus().GetState() {
	case batchpb.JobStatus_QUEUED, batchpb.JobStatus_SCHEDULED:
		return backend.Pending
	case batchpb.JobStatus_RUNNING:
		return backend.Running
	case batchpb.JobStatus_SUCCEEDED:
		return backend.Complete
	default:
		return backend.Failed
	}
}

func (g *GCPMethodsForPoll) PutSingletonBatchJob(name, region, machineType string, bootVolumeInGB int64, bootVolumeType, dockerImage string, cmd []string) error {
	fullName := fmt.Sprintf("projects/%s/locations/%s/jobs/%s", g.projectID, region, name)

	// Delete existing job if present
	_, err := g.batchClient.GetJob(g.ctx, &batchpb.GetJobRequest{Name: fullName})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("checking for existing batch job %s: %w", name, err)
	}
	if err == nil {
		op, err := g.batchClient.DeleteJob(g.ctx, &batchpb.DeleteJobRequest{Name: fullName})
		if err != nil {
			return fmt.Errorf("deleting existing batch job %s: %w", name, err)
		}
		if err := op.Wait(g.ctx); err != nil {
			return fmt.Errorf("waiting for deletion of batch job %s: %w", name, err)
		}
	}

	jobSpec := &JobSpec{
		Runnables:   []Runnable{{Image: dockerImage, Command: cmd}},
		MachineType: machineType,
		Locations:   zonesAsLocations([]string{region}),
		BootDisk: backend.Disk{
			SizeGB: bootVolumeInGB,
			Type:   bootVolumeType,
		},
	}
	_, err = createBatchJobWithID(g.ctx, g.batchClient, g.projectID, region, name, jobSpec)
	if err != nil {
		return fmt.Errorf("creating batch job %s: %w", name, err)
	}
	return nil
}

func (g *GCPMethodsForPoll) GetBatchJobByName(name string) (*backend.BatchJob, error) {
	job, err := g.batchClient.GetJob(g.ctx, &batchpb.GetJobRequest{Name: name})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, backend.NoSuchBatchJob
		}
		return nil, fmt.Errorf("getting batch job %s: %w", name, err)
	}
	instanceCount := 0
	for _, tg := range job.GetTaskGroups() {
		instanceCount += int(tg.GetTaskCount())
	}
	return &backend.BatchJob{ID: job.GetName(), State: batchStateToBatchJobState(job), RequestedInstances: instanceCount}, nil
}

func (g *GCPMethodsForPoll) DeleteAllBatchJobs(region, clusterID string) error {
	jobs, err := g.ListBatchJobs(region, clusterID)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		op, err := g.batchClient.DeleteJob(g.ctx, &batchpb.DeleteJobRequest{Name: job.ID})
		if err != nil {
			return fmt.Errorf("deleting batch job %s: %w", job.ID, err)
		}
		if err := op.Wait(g.ctx); err != nil {
			return fmt.Errorf("waiting for deletion of %s: %w", job.ID, err)
		}
	}
	return nil
}

func (g *GCPMethodsForPoll) SubmitBatchJobs(CreateWorkerCommand backend.CreateWorkerCommandCallback, cluster *backend.Cluster, clusterID string, requests []*backend.BatchJobsToSubmit) error {
	for _, req := range requests {
		commandArgs := CreateWorkerCommand(clusterID, req.ShouldLinger, cluster.AetherConfig)
		jobSpec := &JobSpec{
			Runnables:       []Runnable{{Image: cluster.WorkerDockerImage, Command: commandArgs}},
			MachineType:     cluster.MachineType,
			Preemptible:     req.IsPreemptable,
			Locations:       zonesAsLocations([]string{cluster.Region}),
			SparklesCluster: clusterID,
		}
		_, err := createBatchJob(g.ctx, g.batchClient, g.projectID, cluster.Region, jobSpec, req.InstanceCount, 0, clusterID)
		if err != nil {
			return fmt.Errorf("submitting batch job: %w", err)
		}
	}
	return nil
}

func zonesAsLocations(zones []string) []string {
	locs := make([]string, len(zones))
	for i, z := range zones {
		locs[i] = "zones/" + z
	}
	return locs
}
