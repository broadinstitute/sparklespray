package gcp

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"regexp"
	"strings"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/broadinstitute/sparklesworker/backend"
)

type Runnable struct {
	Image   string   `json:"image"`
	Command []string `json:"command"`
}

type JobSpec struct {
	TaskCount           string                   `json:"task_count"`
	Runnables           []Runnable               `json:"runnables"`
	MachineType         string                   `json:"machine_type"`
	Preemptible         bool                     `json:"preemptible"`
	Locations           []string                 `json:"locations"`
	NetworkTags         []string                 `json:"network_tags"`
	SparklesJob         string                   `json:"sparkles_job"`
	SparklesCluster     string                   `json:"sparkles_cluster"`
	SparklesTimestamp   string                   `json:"sparkles_timestamp"`
	BootDisk            backend.Disk             `json:"boot_disk"`
	Disks               []backend.Disk           `json:"disks"`
	ServiceAccountEmail string                   `json:"service_account_email"`
	Scopes              []string                 `json:"scopes"`
	GCSBucketMounts     []backend.GCSBucketMount `json:"gcs_bucket_mounts"`
}

func createRunnables(runnableSpecs []Runnable, disks []backend.Disk) []*batchpb.Runnable {
	var runnables []*batchpb.Runnable
	for _, r := range runnableSpecs {
		if r.Image == "" {
			// No image: run directly on the host as a script
			runnables = append(runnables, &batchpb.Runnable{
				Executable: &batchpb.Runnable_Script_{
					Script: &batchpb.Runnable_Script{
						Command: &batchpb.Runnable_Script_Text{
							Text: strings.Join(r.Command, " "),
						},
					},
				},
			})
		} else {
			var containerVolumes []string
			for _, disk := range disks {
				containerVolumes = append(containerVolumes, disk.MountPath+":"+disk.MountPath)
			}
			// allow this docker container to talk to the host docker daemon
			containerVolumes = append(containerVolumes, "/var/run/docker.sock:/var/run/docker.sock")
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
	}
	return runnables
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

func createBatchJob(ctx context.Context, client *batch.Client, project, location string, jobSpec *JobSpec, workerCount, maxRetryCount int, pubsubTopic string) (*batchpb.Job, error) {
	fullPubsubTopic := fmt.Sprintf("projects/%s/topics/%s", project, pubsubTopic)

	// Build volumes
	var volumes []*batchpb.Volume
	for _, disk := range jobSpec.Disks {
		volumes = append(volumes, &batchpb.Volume{
			MountPath: disk.MountPath,
			Source: &batchpb.Volume_DeviceName{
				DeviceName: disk.Name,
			},
		})
		log.Printf("createBatchJob mounting %s at %s", disk.Name, disk.MountPath)
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
	runnables := createRunnables(jobSpec.Runnables, jobSpec.Disks)

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

	if !strings.HasPrefix(pubsubTopic, "projects/") {

	}

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
				PubsubTopic: fullPubsubTopic,
				Message: &batchpb.JobNotification_Message{
					Type: batchpb.JobNotification_JOB_STATE_CHANGED,
				},
			},
			{
				PubsubTopic: fullPubsubTopic,
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

// createBatchJobWithID is like createBatchJob but uses an explicit job ID
// instead of generating a random one.
func createBatchJobWithID(ctx context.Context, client *batch.Client, project, location, jobID string, jobSpec *JobSpec) (*batchpb.Job, error) {
	runnables := createRunnables(jobSpec.Runnables, jobSpec.Disks)

	job := &batchpb.Job{
		TaskGroups: []*batchpb.TaskGroup{
			{
				TaskCount:        1,
				TaskCountPerNode: 1,
				TaskSpec: &batchpb.TaskSpec{
					Runnables: runnables,
				},
			},
		},
		AllocationPolicy: &batchpb.AllocationPolicy{
			Location: &batchpb.AllocationPolicy_LocationPolicy{
				AllowedLocations: jobSpec.Locations,
			},
			Instances: []*batchpb.AllocationPolicy_InstancePolicyOrTemplate{
				{
					PolicyTemplate: &batchpb.AllocationPolicy_InstancePolicyOrTemplate_Policy{
						Policy: &batchpb.AllocationPolicy_InstancePolicy{
							MachineType: jobSpec.MachineType,
							BootDisk: &batchpb.AllocationPolicy_Disk{
								Type:       jobSpec.BootDisk.Type,
								SizeGb:     jobSpec.BootDisk.SizeGB,
								DataSource: &batchpb.AllocationPolicy_Disk_Image{Image: "batch-cos"},
							},
						},
					},
				},
			},
		},
		LogsPolicy: &batchpb.LogsPolicy{
			Destination: batchpb.LogsPolicy_CLOUD_LOGGING,
		},
	}

	return client.CreateJob(ctx, &batchpb.CreateJobRequest{
		Parent: "projects/" + project + "/locations/" + location,
		JobId:  jobID,
		Job:    job,
	})
}
