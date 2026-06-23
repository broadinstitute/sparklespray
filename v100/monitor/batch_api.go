package monitor

import (
	"context"
	"fmt"

	"google.golang.org/api/batch/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
)

const (
	labelBatch    = "sparkles-worker-batch"
	labelWorkpool = "sparkles-worker-workpool"

	pubsubNotificationTopic = "batch-api-notifications"

	cloudSDKImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"
)

// GCPBatchAPIClient implements BatchAPIClient using the GCP Batch API and
// Compute Engine API.
type GCPBatchAPIClient struct {
	project    string
	batchSvc   *batch.Service
	computeSvc *compute.Service
}

func NewGCPBatchAPIClient(ctx context.Context, project string, opts ...option.ClientOption) (*GCPBatchAPIClient, error) {
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
	}, nil
}

func (c *GCPBatchAPIClient) CreateJob(ctx context.Context, spec *WorkerJobSpec) (string, error) {

	if spec.Region == "" {
		return "", fmt.Errorf("workpool %s has no region configured", spec.WorkpoolID)
	}

	provisioningModel := "STANDARD"
	if spec.Preemptible {
		provisioningModel = "SPOT"
	}

	disks := make([]*batch.AttachedDisk, 0, len(spec.EmptyVolumes))
	volumes := make([]*batch.Volume, 0, len(spec.EmptyVolumes))
	for i, ev := range spec.EmptyVolumes {
		deviceName := fmt.Sprintf("empty-vol-%d", i)
		disks = append(disks, &batch.AttachedDisk{
			DeviceName: deviceName,
			NewDisk: &batch.Disk{
				Type:   ev.Type,
				SizeGb: int64(ev.SizeInGB),
			},
		})
		volumes = append(volumes, &batch.Volume{
			DeviceName: deviceName,
			MountPath:  ev.MountPoint,
		})
	}

	job := &batch.Job{
		Labels: map[string]string{
			labelBatch:    spec.BatchID,
			labelWorkpool: spec.WorkpoolID,
		},
		TaskGroups: []*batch.TaskGroup{
			{
				TaskCount: int64(spec.VMCount),
				TaskSpec: &batch.TaskSpec{
					Volumes: volumes,
					Runnables: []*batch.Runnable{
						{
							// Download the worker binary from GCS.
							Container: &batch.Container{
								ImageUri: cloudSDKImage,
								Commands: []string{
									"gcloud", "storage", "cp",
									spec.SparklesWorkerGCSPath,
									spec.RootDir + "/sparkles",
								},
							},
						},
						{
							// Make the binary executable and run it.
							Script: &batch.Script{
								Text: fmt.Sprintf(
									"chmod +x %s/sparkles && exec %s/sparkles --root-dir %s",
									spec.RootDir, spec.RootDir, spec.RootDir,
								),
							},
						},
					},
				},
			},
		},
		AllocationPolicy: &batch.AllocationPolicy{
			Instances: []*batch.InstancePolicyOrTemplate{
				{
					Policy: &batch.InstancePolicy{
						MachineType:       spec.MachineType,
						ProvisioningModel: provisioningModel,
						Disks:             disks,
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

	parent := fmt.Sprintf("projects/%s/locations/%s", c.project, spec.Region)
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

func (c *GCPBatchAPIClient) ListRunningVMs(ctx context.Context, filterLabelName, filterLabelValue string, zones []string) (map[string]VMInfo, error) {
	filter := fmt.Sprintf("labels.%s=%s AND status=RUNNING", filterLabelName, filterLabelValue)

	result := make(map[string]VMInfo)
	for _, zone := range zones {
		pageToken := ""
		for {
			call := c.computeSvc.Instances.List(c.project, zone).Filter(filter).Context(ctx)
			if pageToken != "" {
				call = call.PageToken(pageToken)
			}
			resp, err := call.Do()
			if err != nil {
				return nil, fmt.Errorf("list running VMs in zone %s (label %s=%s): %w", zone, filterLabelName, filterLabelValue, err)
			}
			for _, inst := range resp.Items {
				if inst.Status != "RUNNING" {
					continue
				}
				result[inst.Name] = VMInfo{
					InstanceName: inst.Name,
					Zone:         zone,
				}
			}
			if resp.NextPageToken == "" {
				break
			}
			pageToken = resp.NextPageToken
		}
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
