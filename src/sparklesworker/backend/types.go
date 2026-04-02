package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Cluster holds the configuration for a single managed cluster. Immutable
// fields describe the cluster's identity and never change after creation.
// Mutable fields are updated by the monitor or manually by the user.
type Cluster struct {
	// UUID uniquely identifies this cluster and is used as a label value when
	// querying GCP resources (e.g. Compute instances, Batch jobs) that belong
	// to this cluster.
	UUID string `firestore:"uuid" json:"uuid"`

	// MachineType is the GCE machine type used when launching worker nodes.
	MachineType string `firestore:"machine_type" json:"machine_type"`

	// WorkerDockerImage is the container image run on each worker node.
	WorkerDockerImage string `firestore:"worker_docker_image" json:"worker_docker_image"`

	// PubSubInTopic is the Pub/Sub topic the monitor publishes control messages to.
	PubSubInTopic string `firestore:"pub_sub_in_topic" json:"pub_sub_in_topic"`

	// PubSubOutTopic is the Pub/Sub topic workers publish status messages to.
	PubSubOutTopic string `firestore:"pub_sub_out_topic" json:"pub_sub_out_topic"`

	// Region is the GCP region where Batch jobs are submitted (e.g. "us-central1").
	// Used to construct the Batch API parent path.
	Region string `firestore:"region" json:"region"`

	// MaxPreemptableAttempts is the total number of preemptable node-attempts
	// allowed per job run. Resets when the queue drains to zero.
	MaxPreemptableAttempts int `firestore:"max_preemptable_attempts" json:"max_preemptable_attempts"`

	// MaxInstanceCount caps the number of nodes the monitor will request,
	// regardless of queue depth.
	MaxInstanceCount int `firestore:"max_instance_count" json:"max_instance_count"`

	MaxLingerSeconds int `firestore:"max_linger_seconds" json:"max_linger_seconds"`

	// UsedPreemptableAttempts tracks how many preemptable nodes have been
	// requested in the current job run, counted against MaxPreemptableAttempts.
	UsedPreemptableAttempts int `firestore:"used_preemptable_attempts" json:"used_preemptable_attempts"`

	// MaxSuspiciousFailures is the threshold for how many batch jobs may complete
	// without doing any work before the monitor halts node creation and alerts.
	MaxSuspiciousFailures int `firestore:"max_suspicious_failures" json:"max_suspicious_failures"`

	// MonitorState is a JSON-encoded blob persisted between polls. Decoded into
	// MonitorState at the start of each poll and re-encoded at the end.
	MonitorState string `firestore:"monitor_state" json:"monitor_state"`

	BootDiskType    string
	BootDiskSizeGB  int
	Disks           []Disk           `firestore:"disks" json:"disks"`
	GCSBucketMounts []GCSBucketMount `firestore:"gcs_bucket_mounts" json:"gcs_bucket_mounts"`

	AetherConfig *AetherConfig `firestore:"aether_config" json:"aether_config"`
}

// MonitorState is the per-cluster state the monitor persists between polls,
// stored as JSON in Cluster.MonitorState.
type MonitorState struct {
	// batchJobRequests is the cumulative count of node-launch requests made
	// for this cluster. Used as the expectedJobCount when querying Batch jobs
	// to detect API propagation delays.
	BatchJobRequests int

	// completedJobIds is the set of Batch job IDs that have already been
	// inspected after completion. Used to identify newly completed jobs on each
	// poll so each job is only evaluated once.
	SuspiciouslyFailingJobIds []string
}

// monitorStateJSON is the exported-field mirror of MonitorState used for JSON
// serialization, since encoding/json cannot marshal unexported fields.
type MonitorStateJSON struct {
	BatchJobRequests          int      `json:"batchJobRequests"`
	SuspiciouslyFailingJobIds []string `json:"suspiciouslyFailingJobIds"`
}

func (c *Cluster) GetMonitorState() (*MonitorState, error) {
	if c.MonitorState == "" {
		return &MonitorState{}, nil
	}
	var wire MonitorStateJSON
	if err := json.Unmarshal([]byte(c.MonitorState), &wire); err != nil {
		return nil, fmt.Errorf("unmarshaling monitor state: %w", err)
	}
	return &MonitorState{
		BatchJobRequests:          wire.BatchJobRequests,
		SuspiciouslyFailingJobIds: wire.SuspiciouslyFailingJobIds,
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
	RunDuration        time.Duration
}

// BatchJobsToSubmit describes a batch of nodes to launch with uniform properties.
// A single value with instanceCount > 1 maps to one GCP Batch job submission.
type BatchJobsToSubmit struct {
	// instanceCount is the number of nodes to launch in this request.
	InstanceCount int

	// isPreemptable indicates whether to request preemptable (Spot) VMs,
	// which are cheaper but may be reclaimed by GCP at any time.
	IsPreemptable bool

	// shouldLinger indicates this is the cluster's lingering node — the first
	// node created on a cold start, which uses a longer idle timeout to avoid
	// a full cold-start cycle if new tasks arrive shortly after the queue drains.
	ShouldLinger bool
}

// Message represents an incoming control message
type Message struct {
	Type      string
	RequestID string
	Payload   []byte
}

// Response represents an outgoing response to a control message
type Response struct {
	Type      string
	RequestID string
	Payload   interface{}
	Error     string
}

// MessageHandler processes incoming control messages and returns a response
// Returns (response, shouldRespond) - if shouldRespond is false, no response is sent
type MessageHandler func(ctx context.Context, msg *Message) (*Response, bool)

// Channel defines the interface for bidirectional communication
// between workers and the control plane
type Channel interface {
	// Listen starts receiving messages and calls the handler for each one.
	// This method blocks until the context is cancelled or an error occurs.
	Listen(ctx context.Context, handler MessageHandler) error

	// Notify publishes an event/message to the control plane.
	Notify(ctx context.Context, event interface{}) error

	// Close cleans up any resources (subscriptions, connections, etc.)
	Close() error
}

// IncomingMessage wraps incoming requests with a type field to route them
type IncomingMessage struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id"`
	Payload   json.RawMessage `json:"payload"`
}

// OutgoingResponse wraps outgoing responses
type OutgoingResponse struct {
	Type      string      `json:"type"`
	RequestID string      `json:"request_id"`
	Payload   interface{} `json:"payload,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// Worker status event types
const (
	WorkerEventStarted       = "worker_started"
	WorkerEventStopping      = "worker_stopping"
	WorkerEventTaskStarted   = "task_started"
	WorkerEventTaskCompleted = "task_completed"
)

// WorkerStatusEvent is published when worker status changes
type WorkerStatusEvent struct {
	Type      string `json:"type"`
	WorkerID  string `json:"worker_id"`
	Timestamp int64  `json:"timestamp"`
	TaskID    string `json:"task_id,omitempty"`
	ExitCode  string `json:"exit_code,omitempty"`
	Error     string `json:"error,omitempty"`
}

// GetTimestampMillis returns current time in milliseconds
func GetTimestampMillis() int64 {
	return int64(time.Now().UnixNano()) / int64(time.Millisecond)
}

// AetherConfig holds configuration for the aether content-addressed store.
type AetherConfig struct {
	Root            string // aether store root (gs://bucket/prefix or local path)
	MaxSizeToBundle int64  // max file size eligible for bundling (0 = disable bundling)
	MaxBundleSize   int64  // target max size per bundle
	Workers         int    // parallel upload workers (0 = use aether default of 1)
}
