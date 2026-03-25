package gcp

import (
	"context"
	"math/rand"
	"regexp"
	"strings"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/batch/apiv1/batchpb"
)

type Runnable struct {
	Image   string   `json:"image"`
	Command []string `json:"command"`
}

type Disk struct {
	Name      string `json:"name"`
	SizeGB    int64  `json:"size_gb"`
	Type      string `json:"type"`
	MountPath string `json:"mount_path"`
}

type GCSBucketMount struct {
	Path         string   `json:"path"`
	RemotePath   string   `json:"remote_path"`
	MountOptions []string `json:"mount_options"`
}

type JobSpec struct {
	TaskCount           string           `json:"task_count"`
	Runnables           []Runnable       `json:"runnables"`
	MachineType         string           `json:"machine_type"`
	Preemptible         bool             `json:"preemptible"`
	Locations           []string         `json:"locations"`
	NetworkTags         []string         `json:"network_tags"`
	SparklesJob         string           `json:"sparkles_job"`
	SparklesCluster     string           `json:"sparkles_cluster"`
	SparklesTimestamp   string           `json:"sparkles_timestamp"`
	BootDisk            Disk             `json:"boot_disk"`
	Disks               []Disk           `json:"disks"`
	ServiceAccountEmail string           `json:"service_account_email"`
	Scopes              []string         `json:"scopes"`
	GCSBucketMounts     []GCSBucketMount `json:"gcs_bucket_mounts"`
}

var nonAlphanumRe = regexp.MustCompile(`[^a-z0-9]+`)

func normalizeLabel(label string) string {
	label = strings.ToLower(label)
	label = nonAlphanumRe.ReplaceAllString(label, "-")
	if len(label) > 0 && (label[0] < 'a' || label[0] > 'z') {
		label = "x-" + label
	}
	return label
}

const labelAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func makeUniqueLabel(label string) string {
	const suffixLen = 5
	const maxLen = 63
	truncated := label
	if len(truncated) > maxLen-suffixLen-1 {
		truncated = truncated[:maxLen-suffixLen-1]
	}
	suffix := make([]byte, suffixLen)
	for i := range suffix {
		suffix[i] = labelAlphabet[rand.Intn(len(labelAlphabet))]
	}
	return truncated + "-" + string(suffix)
}

func createBatchJob(ctx context.Context, client *batch.Client, project, location string, jobSpec *JobSpec, workerCount, maxRetryCount int, pubsubTopicPrefix string) (*batchpb.Job, error) {
	// Build volumes
	var volumes []*batchpb.Volume
	for _, disk := range jobSpec.Disks {
		volumes = append(volumes, &batchpb.Volume{
			MountPath: disk.MountPath,
			Source: &batchpb.Volume_DeviceName{
				DeviceName: disk.Name,
			},
		})
	}
	for _, gcsMount := range jobSpec.GCSBucketMounts {
		mountOptions := append([]string{"-o", "ro"}, gcsMount.MountOptions...)
		volumes = append(volumes, &batchpb.Volume{
			MountPath:    gcsMount.Path,
			MountOptions: mountOptions,
			Source: &batchpb.Volume_Gcs{
				Gcs: &batchpb.GCS{
					RemotePath: gcsMount.RemotePath,
				},
			},
		})
	}

	// Build runnables
	var runnables []*batchpb.Runnable
	for _, r := range jobSpec.Runnables {
		var containerVolumes []string
		for _, disk := range jobSpec.Disks {
			containerVolumes = append(containerVolumes, disk.MountPath+":"+disk.MountPath)
		}
		runnables = append(runnables, &batchpb.Runnable{
			Executable: &batchpb.Runnable_Container_{
				Container: &batchpb.Runnable_Container{
					ImageUri: r.Image,
					Commands: r.Command,
					Volumes:  containerVolumes,
					Options:  "-u 0",
				},
			},
		})
	}

	// Build attached disks
	var attachedDisks []*batchpb.AllocationPolicy_AttachedDisk
	for _, disk := range jobSpec.Disks {
		attachedDisks = append(attachedDisks, &batchpb.AllocationPolicy_AttachedDisk{
			DeviceName: disk.Name,
			Attached: &batchpb.AllocationPolicy_AttachedDisk_NewDisk{
				NewDisk: &batchpb.AllocationPolicy_Disk{
					Type:   disk.Type,
					SizeGb: disk.SizeGB,
				},
			},
		})
	}

	// Provisioning model
	provisioningModel := batchpb.AllocationPolicy_STANDARD
	if jobSpec.Preemptible {
		provisioningModel = batchpb.AllocationPolicy_SPOT
	}

	pubsubTopic := "projects/" + project + "/topics/" + pubsubTopicPrefix + "-batchapi-out"

	job := &batchpb.Job{
		TaskGroups: []*batchpb.TaskGroup{
			{
				TaskCount:        int64(workerCount),
				TaskCountPerNode: 1,
				TaskSpec: &batchpb.TaskSpec{
					Runnables:     runnables,
					Volumes:       volumes,
					MaxRetryCount: int32(maxRetryCount),
				},
			},
		},
		AllocationPolicy: &batchpb.AllocationPolicy{
			Location: &batchpb.AllocationPolicy_LocationPolicy{
				AllowedLocations: jobSpec.Locations,
			},
			ServiceAccount: &batchpb.ServiceAccount{
				Email:  jobSpec.ServiceAccountEmail,
				Scopes: jobSpec.Scopes,
			},
			Instances: []*batchpb.AllocationPolicy_InstancePolicyOrTemplate{
				{
					PolicyTemplate: &batchpb.AllocationPolicy_InstancePolicyOrTemplate_Policy{
						Policy: &batchpb.AllocationPolicy_InstancePolicy{
							MachineType:       jobSpec.MachineType,
							ProvisioningModel: provisioningModel,
							BootDisk: &batchpb.AllocationPolicy_Disk{
								Type:       jobSpec.BootDisk.Type,
								SizeGb:     jobSpec.BootDisk.SizeGB,
								DataSource: &batchpb.AllocationPolicy_Disk_Image{Image: "batch-cos"},
							},
							Disks: attachedDisks,
						},
					},
				},
			},
			Tags: jobSpec.NetworkTags,
		},
		LogsPolicy: &batchpb.LogsPolicy{
			Destination: batchpb.LogsPolicy_CLOUD_LOGGING,
		},
		Labels: map[string]string{
			"sparkles-job":     normalizeLabel(jobSpec.SparklesJob),
			"sparkles-cluster": jobSpec.SparklesCluster,
		},
		Notifications: []*batchpb.JobNotification{
			{
				PubsubTopic: pubsubTopic,
				Message: &batchpb.JobNotification_Message{
					Type: batchpb.JobNotification_JOB_STATE_CHANGED,
				},
			},
			{
				PubsubTopic: pubsubTopic,
				Message: &batchpb.JobNotification_Message{
					Type: batchpb.JobNotification_TASK_STATE_CHANGED,
				},
			},
		},
	}

	jobID := makeUniqueLabel("sparkles-" + normalizeLabel(jobSpec.SparklesJob))

	req := &batchpb.CreateJobRequest{
		Parent: "projects/" + project + "/locations/" + location,
		JobId:  jobID,
		Job:    job,
	}

	return client.CreateJob(ctx, req)
}
