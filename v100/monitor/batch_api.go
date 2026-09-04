package monitor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/api/batch/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	loggingv2 "google.golang.org/api/logging/v2"
	"google.golang.org/api/option"
)

const (
	labelWorkpool = "sparkles-workpool"

	pubsubNotificationTopic = "batch-api-notifications"

	cloudSDKImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"
)

// GCPBatchAPIClient implements BatchAPIClient using the GCP Batch API and
// Compute Engine API.
//
// project is the control-plane project the monitor was started with. It is
// used both as the default for workload operations (see resolveProject) and,
// unconditionally, for the two things that must stay in the control plane
// regardless of where a workpool's VMs run: the Batch job's Pub/Sub
// notification topic and the --project the worker binary is launched with.
type GCPBatchAPIClient struct {
	project    string
	batchSvc   *batch.Service
	computeSvc *compute.Service
	loggingSvc *loggingv2.Service
}

// resolveProject returns the project a workload operation should target:
// projectID when a workpool overrides it, otherwise the client's own project.
func (c *GCPBatchAPIClient) resolveProject(projectID string) string {
	if projectID != "" {
		return projectID
	}
	return c.project
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
	loggingSvc, err := loggingv2.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating logging service: %w", err)
	}
	return &GCPBatchAPIClient{
		project:    project,
		batchSvc:   batchSvc,
		computeSvc: computeSvc,
		loggingSvc: loggingSvc,
	}, nil
}

// validGoogleLabel reports whether s is a valid GCP label value:
// lowercase letters, digits, and hyphens only, starting with a letter.
func validGoogleLabel(s string) bool {
	if len(s) == 0 || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

func formatResources(resources []ResourceEntry) string {
	parts := make([]string, len(resources))
	for i, r := range resources {
		parts[i] = fmt.Sprintf("%s=%g", r.Name, r.Value)
	}
	return strings.Join(parts, ",")
}

// formatBindMountArgs derives a --bind-mount flag for each empty volume's mount point,
// so task containers can see the extra disks that GCP Batch mounts onto the VM host.
func formatBindMountArgs(rootDir string, volumes []EmptyVolume) string {
	var b strings.Builder
	fmt.Fprintf(&b, " --bind-mount '%s:%s'", rootDir, rootDir)
	for _, v := range volumes {
		fmt.Fprintf(&b, " --bind-mount '%s:%s'", v.MountPoint, v.MountPoint)
	}
	return b.String()
}

// gcpJobLabels builds the GCE label map for a Batch job's VMs: the
// workpool's user-defined labels (spec.Labels, propagated from
// WorkPool.Labels for filtering/billing attribution), plus the reserved
// labelWorkpool label the monitor relies on to find a batch's VMs — which
// always wins if a user label happens to reuse that key. GCP label keys and
// values must match its own restricted charset (lowercase letters, digits,
// '-', '_'); invalid user labels surface as a CreateJob error, handled the
// same as any other batch_failed outcome.
func gcpJobLabels(spec *WorkerJobSpec) map[string]string {
	labels := make(map[string]string, len(spec.Labels)+1)
	for _, l := range spec.Labels {
		labels[l.Name] = l.Value
	}
	labels[labelWorkpool] = spec.WorkpoolID
	return labels
}

func (c *GCPBatchAPIClient) CreateJob(ctx context.Context, spec *WorkerJobSpec) (string, error) {
	if len(spec.Resources) == 0 {
		return "", fmt.Errorf("workpool %s has no resources configured", spec.WorkpoolID)
	}

	if !validGoogleLabel(spec.WorkpoolID) {
		return "", fmt.Errorf("workpool %s must be all lowercase, only be letters or numbers or '-' and start with a letter.", spec.WorkpoolID)
	}

	if spec.ServiceAccount == "" {
		return "", fmt.Errorf("workpool %s has no service account configured", spec.WorkpoolID)
	}

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

	// --project is deliberately c.project, not spec.ProjectID: it's the project
	// the worker uses for Firestore and Pub/Sub to claim tasks and report
	// state, which stays in the control plane even when the VM itself runs
	// in another project.
	workerArgs := fmt.Sprintf("--stream --batch %s --project %s --db %s --workpool %s --work-dir %s --resources %s --linger %d", spec.BatchID, c.project, spec.DBName, spec.WorkpoolID, spec.RootDir, formatResources(spec.Resources), spec.LingerTime/time.Second) +
		formatBindMountArgs(spec.RootDir, spec.EmptyVolumes)
	log.Printf("worker args: %s", workerArgs)

	job := &batch.Job{
		TaskGroups: []*batch.TaskGroup{
			{
				TaskCount:        int64(spec.VMCount),
				TaskCountPerNode: 1,
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
								Volumes: []string{spec.RootDir + ":" + spec.RootDir},
							},
						},
						{
							// Make the binary executable and run it.
							Script: &batch.Script{
								Text: fmt.Sprintf(
									"chmod +x %s/sparkles && exec %s/sparkles worker %s",
									spec.RootDir, spec.RootDir, workerArgs,
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
			ServiceAccount: &batch.ServiceAccount{
				Email: spec.ServiceAccount,
			},
			Labels: gcpJobLabels(spec),
		},
		LogsPolicy: &batch.LogsPolicy{
			Destination: "CLOUD_LOGGING",
		},
		// These topics are deliberately in c.project, not spec.ProjectID: this
		// is the topic the monitor subscribes to (see NewGCPPubSubReceiver), so
		// pointing it at a workload project would mean never hearing about
		// state changes. Cross-project delivery requires granting the workload
		// project's Batch service agent publish rights on this topic.
		Notifications: []*batch.JobNotification{
			{
				PubsubTopic: fmt.Sprintf("projects/%s/topics/%s", c.project, pubsubNotificationTopic),
				Message: &batch.Message{
					Type: "JOB_STATE_CHANGED",
				},
			},
			{
				PubsubTopic: fmt.Sprintf("projects/%s/topics/%s", c.project, pubsubNotificationTopic),
				Message: &batch.Message{
					Type: "TASK_STATE_CHANGED",
				},
			},
		},
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", c.resolveProject(spec.ProjectID), spec.Region)
	created, err := c.batchSvc.Projects.Locations.Jobs.Create(parent, job).JobId(spec.BatchID).Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("batch create job: %w", err)
	}

	return created.Name, nil
}

func (c *GCPBatchAPIClient) GetJobStatus(ctx context.Context, jobID string) (BatchJobStatus, error) {
	job, err := c.batchSvc.Projects.Locations.Jobs.Get(jobID).Context(ctx).Do()
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			return BatchJobStatusDeleted, nil
		}
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

func (c *GCPBatchAPIClient) ListRunningVMs(ctx context.Context, projectID, filterLabelName, filterLabelValue string, zones []string) (map[string]VMInfo, error) {
	filter := fmt.Sprintf("labels.%s=%s AND status=RUNNING", filterLabelName, filterLabelValue)
	project := c.resolveProject(projectID)

	result := make(map[string]VMInfo)
	for _, zone := range zones {
		pageToken := ""
		for {
			call := c.computeSvc.Instances.List(project, zone).Filter(filter).Context(ctx)
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

func (c *GCPBatchAPIClient) TerminateVM(ctx context.Context, projectID, zone, instanceName string) error {
	_, err := c.computeSvc.Instances.Delete(c.resolveProject(projectID), zone, instanceName).Context(ctx).Do()
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

func (c *GCPBatchAPIClient) PrintBatchDebuggingInfo(ctx context.Context, projectID, jobID string) error {
	job, err := c.batchSvc.Projects.Locations.Jobs.Get(jobID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("getting batch job %s: %w", jobID, err)
	}

	// Derive time bounds from status events; fall back to CreateTime for the start.
	var startTime, endTime time.Time
	if job.CreateTime != "" {
		if t, err := time.Parse(time.RFC3339, job.CreateTime); err == nil {
			startTime = t
		}
	}
	if job.Status != nil {
		for _, ev := range job.Status.StatusEvents {
			if ev.EventTime == "" {
				continue
			}
			t, err := time.Parse(time.RFC3339, ev.EventTime)
			if err != nil {
				continue
			}
			if startTime.IsZero() || t.Before(startTime) {
				startTime = t
			}
			if t.After(endTime) {
				endTime = t
			}
		}
	}
	if startTime.IsZero() {
		startTime = time.Now().Add(-1 * time.Hour)
	}
	if endTime.IsZero() {
		endTime = time.Now()
	}

	queryStart := startTime.Add(-5 * time.Minute).UTC().Format(time.RFC3339)
	queryEnd := endTime.Add(5 * time.Minute).UTC().Format(time.RFC3339)

	// Batch writes its logs into the project the job ran in.
	project := c.resolveProject(projectID)

	filter := fmt.Sprintf(
		`(logName="projects/%s/logs/batch_task_logs" OR logName="projects/%s/logs/batch_agent_logs") `+
			`labels.job_uid="%s" `+
			`timestamp>="%s" `+
			`timestamp<="%s" `+
			`severity>=DEFAULT`,
		project, project, job.Uid, queryStart, queryEnd,
	)

	resp, err := c.loggingSvc.Entries.List(&loggingv2.ListLogEntriesRequest{
		ResourceNames: []string{fmt.Sprintf("projects/%s", project)},
		Filter:        filter,
		OrderBy:       "timestamp asc",
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("listing log entries for batch job %s: %w", jobID, err)
	}

	if err := os.MkdirAll("batch-api-errors", 0o755); err != nil {
		return fmt.Errorf("creating batch-api-errors dir: %w", err)
	}
	base := fmt.Sprintf("batch-api-errors/%s", time.Now().UTC().Format("20060102-150405"))
	var f *os.File
	filename := base
	for i := 1; ; i++ {
		f, err = os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return fmt.Errorf("creating debug file: %w", err)
		}
		filename = fmt.Sprintf("%s-%d", base, i)
	}
	defer f.Close()

	fmt.Fprintf(f, "jobID: %s\n", jobID)
	fmt.Fprintf(f, "uid: %s\n\n", job.Uid)
	if job.Status != nil {
		for _, ev := range job.Status.StatusEvents {
			fmt.Fprintf(f, "  status event [%s]: %s\n", ev.EventTime, ev.Description)
		}
	}
	fmt.Fprintf(f, "\n%d log entries found\n\n", len(resp.Entries))
	for _, entry := range resp.Entries {
		fmt.Fprintf(f, "[%s] [%s] %s: %s\n", entry.Timestamp, entry.Severity, entry.LogName, entry.TextPayload)
	}

	log.Printf("batch job %s: full debug info written to %s", jobID, filename)
	return nil
}
