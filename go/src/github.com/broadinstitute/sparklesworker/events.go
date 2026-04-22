package sparklesworker

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/datastore"
	pubsub "cloud.google.com/go/pubsub/v2"
	"github.com/google/uuid"
)

const EventCollection = "SparklesV5Event"
const ClusterCollection = "SparklesV5Cluster"
const TopicLifecycle = "sparkles-events"
const EventExpiry = 7 * 24 * time.Hour

type Cluster struct {
	ClusterID   string    `datastore:"cluster_id"`
	MachineType string    `datastore:"machine_type"`
	CreatedAt   time.Time `datastore:"created_at"`
}

// EventWriter writes lifecycle events to Datastore and notifies via Pub/Sub.
// psClient may be nil when Pub/Sub is disabled (--pubsubDisable flag).
type EventWriter struct {
	dsClient  *datastore.Client
	psClient  *pubsub.Client
	clusterID string
}

func NewEventWriter(dsClient *datastore.Client, psClient *pubsub.Client, clusterID string) *EventWriter {
	return &EventWriter{dsClient: dsClient, psClient: psClient, clusterID: clusterID}
}

func (ew *EventWriter) WriteCluster(ctx context.Context, machineType string) error {
	c := &Cluster{
		ClusterID:   ew.clusterID,
		MachineType: machineType,
		CreatedAt:   time.Now().UTC(),
	}
	key := datastore.NameKey(ClusterCollection, ew.clusterID, nil)
	_, err := ew.dsClient.Put(ctx, key, c)
	return err
}

func (ew *EventWriter) writeEvent(ctx context.Context, props datastore.PropertyList) error {
	eventID := uuid.New().String()
	now := time.Now().UTC()
	props = append(props,
		datastore.Property{Name: "event_id", Value: eventID, NoIndex: false},
		datastore.Property{Name: "timestamp", Value: now, NoIndex: false},
		datastore.Property{Name: "expiry", Value: now.Add(EventExpiry), NoIndex: false},
	)

	key := datastore.NameKey(EventCollection, eventID, nil)
	if _, err := ew.dsClient.Put(ctx, key, &props); err != nil {
		return fmt.Errorf("writeEvent failed in DS put: %s", err)
	}

	var eventType string
	for _, p := range props {
		if p.Name == "type" {
			eventType, _ = p.Value.(string)
		}
	}

	if eventType != "" && ew.psClient != nil {
		publisher := ew.psClient.Publisher(TopicLifecycle)
		defer publisher.Stop()
		result := publisher.Publish(ctx, &pubsub.Message{
			Attributes: map[string]string{"type": eventType},
		})
		if _, err := result.Get(ctx); err != nil {
			return fmt.Errorf("writeEvent failed in publish.Get(): %s", err)
		}
	}

	return nil
}

func (ew *EventWriter) WriteWorkerStarted(ctx context.Context, workerID string) error {
	log.Printf("WriteWorkerStarted: workerID=%s", workerID)
	return ew.writeEvent(ctx, datastore.PropertyList{
		{Name: "type", Value: "worker_started", NoIndex: false},
		{Name: "cluster_id", Value: ew.clusterID, NoIndex: false},
		{Name: "worker_id", Value: workerID, NoIndex: false},
	})
}

func (ew *EventWriter) WriteWorkerStopped(ctx context.Context, workerID string) error {
	log.Printf("WriteWorkerStopped: workerID=%s", workerID)
	return ew.writeEvent(ctx, datastore.PropertyList{
		{Name: "type", Value: "worker_stopped", NoIndex: false},
		{Name: "cluster_id", Value: ew.clusterID, NoIndex: false},
		{Name: "worker_id", Value: workerID, NoIndex: false},
	})
}

func (ew *EventWriter) WriteTaskClaimed(ctx context.Context, taskID, jobID string) error {
	return ew.writeEvent(ctx, datastore.PropertyList{
		{Name: "type", Value: "task_claimed", NoIndex: false},
		{Name: "cluster_id", Value: ew.clusterID, NoIndex: false},
		{Name: "job_id", Value: jobID, NoIndex: false},
		{Name: "task_id", Value: taskID, NoIndex: false},
	})
}

func (ew *EventWriter) WriteTaskExecStarted(ctx context.Context, taskID, jobID string) error {
	return ew.writeEvent(ctx, datastore.PropertyList{
		{Name: "type", Value: "task_exec_started", NoIndex: false},
		{Name: "cluster_id", Value: ew.clusterID, NoIndex: false},
		{Name: "job_id", Value: jobID, NoIndex: false},
		{Name: "task_id", Value: taskID, NoIndex: false},
	})
}

func (ew *EventWriter) WriteTaskExecComplete(ctx context.Context, taskID, jobID string) error {
	return ew.writeEvent(ctx, datastore.PropertyList{
		{Name: "type", Value: "task_exec_complete", NoIndex: false},
		{Name: "cluster_id", Value: ew.clusterID, NoIndex: false},
		{Name: "job_id", Value: jobID, NoIndex: false},
		{Name: "task_id", Value: taskID, NoIndex: false},
	})
}

func (ew *EventWriter) WriteTaskComplete(ctx context.Context, taskID, jobID string, exitCode int64, usage *ResourceUsage, downloadBytes, uploadBytes int64) error {
	props := datastore.PropertyList{
		{Name: "type", Value: "task_complete", NoIndex: false},
		{Name: "cluster_id", Value: ew.clusterID, NoIndex: false},
		{Name: "job_id", Value: jobID, NoIndex: false},
		{Name: "task_id", Value: taskID, NoIndex: false},
		{Name: "exit_code", Value: exitCode, NoIndex: true},
		{Name: "download_bytes", Value: downloadBytes, NoIndex: true},
		{Name: "upload_bytes", Value: uploadBytes, NoIndex: true},
	}
	if usage != nil {
		maxMemBytes := usage.MaxMemorySize
		maxMemGB := float64(maxMemBytes) / (1024 * 1024 * 1024)
		userCPUSec := float64(usage.UserCPUTime.Sec) + float64(usage.UserCPUTime.Usec)/1e6
		sysCPUSec := float64(usage.SystemCPUTime.Sec) + float64(usage.SystemCPUTime.Usec)/1e6
		props = append(props,
			datastore.Property{Name: "max_mem_in_gb", Value: maxMemGB, NoIndex: true},
			datastore.Property{Name: "max_memory_bytes", Value: maxMemBytes, NoIndex: true},
			datastore.Property{Name: "shared_memory_bytes", Value: usage.SharedMemorySize, NoIndex: true},
			datastore.Property{Name: "unshared_memory_bytes", Value: usage.UnsharedMemorySize, NoIndex: true},
			datastore.Property{Name: "user_cpu_sec", Value: userCPUSec, NoIndex: true},
			datastore.Property{Name: "system_cpu_sec", Value: sysCPUSec, NoIndex: true},
			datastore.Property{Name: "block_input_ops", Value: usage.BlockInputOps, NoIndex: true},
			datastore.Property{Name: "block_output_ops", Value: usage.BlockOutputOps, NoIndex: true},
		)
	}
	return ew.writeEvent(ctx, props)
}

func (ew *EventWriter) WriteTaskFailed(ctx context.Context, taskID, jobID, reason string) error {
	return ew.writeEvent(ctx, datastore.PropertyList{
		{Name: "type", Value: "task_failed", NoIndex: false},
		{Name: "cluster_id", Value: ew.clusterID, NoIndex: false},
		{Name: "job_id", Value: jobID, NoIndex: false},
		{Name: "task_id", Value: taskID, NoIndex: false},
		{Name: "failure_reason", Value: reason, NoIndex: true},
	})
}
