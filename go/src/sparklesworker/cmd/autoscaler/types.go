package autoscaler

import (
	"encoding/json"
	"fmt"
)

// Cluster holds the configuration for a single managed cluster. Immutable
// fields describe the cluster's identity and never change after creation.
// Mutable fields are updated by the monitor or manually by the user.
type Cluster struct {
	// UUID uniquely identifies this cluster and is used as a label value when
	// querying GCP resources (e.g. Compute instances, Batch jobs) that belong
	// to this cluster.
	UUID string `firestore:"uuid"`

	// MachineType is the GCE machine type used when launching worker nodes.
	MachineType string `firestore:"machine_type"`

	// WorkerDockerImage is the container image run on each worker node.
	WorkerDockerImage string `firestore:"worker_docker_image"`

	// WorkerCommandArgs are the arguments passed to the worker container on startup.
	WorkerCommandArgs []string `firestore:"worker_command_args"`

	// PubSubInTopic is the Pub/Sub topic the monitor publishes control messages to.
	PubSubInTopic string `firestore:"pub_sub_in_topic"`

	// PubSubOutTopic is the Pub/Sub topic workers publish status messages to.
	PubSubOutTopic string `firestore:"pub_sub_out_topic"`

	// Region is the GCP region where Batch jobs are submitted (e.g. "us-central1").
	// Used to construct the Batch API parent path.
	Region string `firestore:"region"`

	// Zones is the list of GCE zones within the region to query for running
	// instances. Passed to listRunningInstances on each poll.
	Zones []string `firestore:"zones"`

	// MaxPreemptableAttempts is the total number of preemptable node-attempts
	// allowed per job run. Resets when the queue drains to zero.
	MaxPreemptableAttempts int `firestore:"max_preemptable_attempts"`

	// MaxInstanceCount caps the number of nodes the monitor will request,
	// regardless of queue depth.
	MaxInstanceCount int `firestore:"max_instance_count"`

	// UsedPreemptableAttempts tracks how many preemptable nodes have been
	// requested in the current job run, counted against MaxPreemptableAttempts.
	UsedPreemptableAttempts int `firestore:"used_preemptable_attempts"`

	// MaxSuspiciousFailures is the threshold for how many batch jobs may complete
	// without doing any work before the monitor halts node creation and alerts.
	MaxSuspiciousFailures int `firestore:"max_suspicious_failures"`

	// MonitorState is a JSON-encoded blob persisted between polls. Decoded into
	// MonitorState at the start of each poll and re-encoded at the end.
	MonitorState string `firestore:"monitor_state"`
}

// MonitorState is the per-cluster state the monitor persists between polls,
// stored as JSON in Cluster.MonitorState.
type MonitorState struct {
	// batchJobRequests is the cumulative count of node-launch requests made
	// for this cluster. Used as the expectedJobCount when querying Batch jobs
	// to detect API propagation delays.
	batchJobRequests int

	// completedJobIds is the set of Batch job IDs that have already been
	// inspected after completion. Used to identify newly completed jobs on each
	// poll so each job is only evaluated once.
	completedJobIds []string

	// suspiciouslyFailedToRun is the running count of Batch jobs that completed
	// without executing any Sparkles tasks. Compared against
	// Cluster.MaxSuspiciousFailures to decide whether to halt the cluster.
	suspiciouslyFailedToRun int
}

// monitorStateJSON is the exported-field mirror of MonitorState used for JSON
// serialization, since encoding/json cannot marshal unexported fields.
type monitorStateJSON struct {
	BatchJobRequests        int      `json:"batchJobRequests"`
	CompletedJobIds         []string `json:"completedJobIds"`
	SuspiciouslyFailedToRun int      `json:"suspiciouslyFailedToRun"`
}

func (c *Cluster) getMonitorState() (*MonitorState, error) {
	if c.MonitorState == "" {
		return &MonitorState{}, nil
	}
	var wire monitorStateJSON
	if err := json.Unmarshal([]byte(c.MonitorState), &wire); err != nil {
		return nil, fmt.Errorf("unmarshaling monitor state: %w", err)
	}
	return &MonitorState{
		batchJobRequests:        wire.BatchJobRequests,
		completedJobIds:         wire.CompletedJobIds,
		suspiciouslyFailedToRun: wire.SuspiciouslyFailedToRun,
	}, nil
}

// BatchJobState represents the lifecycle state of a GCP Batch job.
type BatchJobState int

const (
	Pending  BatchJobState = 1
	Running  BatchJobState = 2
	Failed   BatchJobState = 3
	Complete BatchJobState = 4
)

// BatchJob is a summary of a GCP Batch job associated with a cluster.
type BatchJob struct {
	ID                 string
	State              BatchJobState
	RequestedInstances int
}

// BatchJobsToSubmit describes a batch of nodes to launch with uniform properties.
// A single value with instanceCount > 1 maps to one GCP Batch job submission.
type BatchJobsToSubmit struct {
	// instanceCount is the number of nodes to launch in this request.
	instanceCount int

	// isPreemptable indicates whether to request preemptable (Spot) VMs,
	// which are cheaper but may be reclaimed by GCP at any time.
	isPreemptable bool

	// shouldLinger indicates this is the cluster's lingering node — the first
	// node created on a cold start, which uses a longer idle timeout to avoid
	// a full cold-start cycle if new tasks arrive shortly after the queue drains.
	shouldLinger bool
}
