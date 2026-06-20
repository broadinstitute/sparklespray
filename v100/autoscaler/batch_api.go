package autoscaler

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/api/batch/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
)

const (
	labelBatch    = "sparkles-worker-batch"
	labelWorkpool = "sparkles-worker-workpool"

	pubsubNotificationTopic = "autoscaler-in"
)

// GCPBatchAPIClient implements BatchAPIClient using the GCP Batch API and
// Compute Engine API.
type GCPBatchAPIClient struct {
	project    string
	batchSvc   *batch.Service
	computeSvc *compute.Service
	pools      WorkPoolStore
}

func NewGCPBatchAPIClient(ctx context.Context, project string, pools WorkPoolStore, opts ...option.ClientOption) (*GCPBatchAPIClient, error) {
	batchSvc, err := batch.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating batch service: %w", err)
	}
	computeSvc, err := compute.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating compute service: %w", err)
	}
	return &GCPBatchAPIClient{
		project:    project,
		batchSvc:   batchSvc,
		computeSvc: computeSvc,
		pools:      pools,
	}, nil
}

func (c *GCPBatchAPIClient) CreateJob(ctx context.Context, workpoolID, batchID string, vmCount int, preemptible bool) (string, error) {
	pool, err := c.pools.Get(ctx, workpoolID)
	if err != nil {
		return "", fmt.Errorf("get workpool %s: %w", workpoolID, err)
	}
	if pool.Region == "" {
		return "", fmt.Errorf("workpool %s has no region configured", workpoolID)
	}

	provisioningModel := "STANDARD"
	if preemptible {
		provisioningModel = "SPOT"
	}

	job := &batch.Job{
		Labels: map[string]string{
			labelBatch:    batchID,
			labelWorkpool: workpoolID,
		},
		TaskGroups: []*batch.TaskGroup{
			{
				TaskCount: int64(vmCount),
				TaskSpec:  &batch.TaskSpec{},
			},
		},
		AllocationPolicy: &batch.AllocationPolicy{
			Instances: []*batch.InstancePolicyOrTemplate{
				{
					Policy: &batch.InstancePolicy{
						MachineType:       pool.MachineType,
						ProvisioningModel: provisioningModel,
					},
				},
			},
		},
		Notifications: []*batch.JobNotification{
			{
				PubsubTopic: fmt.Sprintf("projects/%s/topics/%s", c.project, pubsubNotificationTopic),
			},
		},
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", c.project, pool.Region)
	created, err := c.batchSvc.Projects.Locations.Jobs.Create(parent, job).Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("batch create job: %w", err)
	}
	return created.Name, nil
}

func (c *GCPBatchAPIClient) GetJobStatus(ctx context.Context, jobID string) (BatchJobStatus, error) {
	job, err := c.batchSvc.Projects.Locations.Jobs.Get(jobID).Context(ctx).Do()
	if err != nil {
		return BatchJobStatusFailed, fmt.Errorf("get batch job %s: %w", jobID, err)
	}
	if job.Status == nil {
		return BatchJobStatusQueued, nil
	}
	switch job.Status.State {
	case "QUEUED":
		return BatchJobStatusQueued, nil
	case "SCHEDULED":
		return BatchJobStatusScheduled, nil
	case "RUNNING":
		return BatchJobStatusRunning, nil
	case "SUCCEEDED":
		return BatchJobStatusSucceeded, nil
	case "FAILED", "DELETION_IN_PROGRESS":
		return BatchJobStatusFailed, nil
	default:
		return BatchJobStatusQueued, nil
	}
}

func (c *GCPBatchAPIClient) ListRunningVMs(ctx context.Context, filterLabelName, filterLabelValue string) (map[string]VMInfo, error) {
	filter := fmt.Sprintf("labels.%s=%s AND status=RUNNING", filterLabelName, filterLabelValue)

	result := make(map[string]VMInfo)
	pageToken := ""
	for {
		call := c.computeSvc.Instances.AggregatedList(c.project).Filter(filter).Context(ctx)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		resp, err := call.Do()
		if err != nil {
			return nil, fmt.Errorf("list running VMs (label %s=%s): %w", filterLabelName, filterLabelValue, err)
		}
		for _, scopedList := range resp.Items {
			for _, inst := range scopedList.Instances {
				if inst.Status != "RUNNING" {
					continue
				}
				result[inst.Name] = VMInfo{
					InstanceName: inst.Name,
					Zone:         zoneFromURL(inst.Zone),
				}
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return result, nil
}

func (c *GCPBatchAPIClient) TerminateVM(ctx context.Context, zone, instanceName string) error {
	_, err := c.computeSvc.Instances.Delete(c.project, zone, instanceName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete instance %s in %s: %w", instanceName, zone, err)
	}
	return nil
}

func (c *GCPBatchAPIClient) TerminateJob(ctx context.Context, jobID string) error {
	_, err := c.batchSvc.Projects.Locations.Jobs.Cancel(jobID, &batch.CancelJobRequest{}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("cancel batch job %s: %w", jobID, err)
	}
	return nil
}

// zoneFromURL extracts the zone name from a GCP self-link URL such as
// "https://www.googleapis.com/compute/v1/projects/my-proj/zones/us-central1-a".
func zoneFromURL(zoneURL string) string {
	parts := strings.Split(zoneURL, "/")
	if len(parts) == 0 {
		return zoneURL
	}
	return parts[len(parts)-1]
}
