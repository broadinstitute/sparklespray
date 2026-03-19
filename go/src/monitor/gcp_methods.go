package monitor

import (
	"context"
	"fmt"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/batch/apiv1/batchpb"
	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/api/iterator"
)

type CloudMethodsForPoll interface {
	listRunningInstances(zones []string, clusterID string) ([]string, error)
	countRequestedInstances(region string, clusterID string) (int, error)
	listBatchJobs(clusterID string) ([]*BatchJob, error)
	submitBatchJobs(requests []*BatchJobsToSubmit) error
	deleteAllBatchJobs(clusterID string) error
}

type GCPMethodsForPoll struct {
	projectID       string
	ctx             context.Context
	instancesClient *compute.InstancesClient
	batchClient     *batch.Client
}

func (g *GCPMethodsForPoll) listRunningInstances(zones []string, clusterID string) ([]string, error) {
	var instanceNames []string

	filter := proto.String(fmt.Sprintf(`labels.sparkles-cluster-uuid = "%s"`, clusterID))

	for _, zone := range zones {
		req := &computepb.ListInstancesRequest{
			Project: g.projectID,
			Zone:    zone,
			Filter:  filter,
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

func (g *GCPMethodsForPoll) submitBatchJobs(requests []*BatchJobsToSubmit) error {
	panic("unimplemented")
}

func (g *GCPMethodsForPoll) countRequestedInstances(region string, clusterID string) (int, error) {
	req := &batchpb.ListJobsRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", g.projectID, region),
		Filter: fmt.Sprintf(`labels.sparkles-cluster = "%s"`, clusterID),
	}

	total := 0
	it := g.batchClient.ListJobs(g.ctx, req)
	for {
		job, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("listing batch jobs: %w", err)
		}

		state := job.GetStatus().GetState()
		if state == batchpb.JobStatus_QUEUED || state == batchpb.JobStatus_SCHEDULED || state == batchpb.JobStatus_RUNNING {
			for _, tg := range job.GetTaskGroups() {
				total += int(tg.GetTaskCount())
			}
		}
	}

	return total, nil
}

func (g *GCPMethodsForPoll) listBatchJobs(clusterID string) ([]*BatchJob, error) {
	//	req := &batchpb.ListJobsRequest{
	//	    Parent: fmt.Sprintf("projects/%s/locations/%s", projectID, region),
	//	    Filter: `labels.sparkles-cluster = "some-uuid"`,
	//	}
	panic("unimplemented")
}

func (g *GCPMethodsForPoll) deleteAllBatchJobs(clusterID string) error {
	panic("unimplemented")
}
