package v100

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"github.com/google/uuid"
)

const EventCollection = "Events"
const eventCollection = EventCollection
const eventTTL = 7 * 24 * time.Hour

// EventRecord is written to the Events Firestore collection for every event
// published to the sparkles-events topic. It contains the same fields as the
// corresponding Pub/Sub message plus Timestamp and Expiry.
type EventRecord struct {
	EventID   string    `firestore:"event_id"`
	Type      string    `firestore:"type"`
	Timestamp time.Time `firestore:"timestamp"`
	Expiry    time.Time `firestore:"expiry"`
	// Worker event fields (populated for worker_started and worker_stopped)
	WorkerID   string `firestore:"worker_id"`
	WorkpoolID string `firestore:"workpool_id"`
	// Task state update fields (populated for task_state_update)
	TaskID   string `firestore:"task_id"`
	JobID    string `firestore:"job_id"`
	OldState string `firestore:"old_state"`
	NewState string `firestore:"new_state"`
}

// WorkerEvent is published to sparkles-events and recorded in Events on worker
// lifecycle changes.
type WorkerEvent struct {
	Type       string `json:"type"`
	WorkerID   string `json:"worker_id"`
	WorkpoolID string `json:"workpool_id"`
}

// TaskStateUpdate is published to sparkles-events and recorded in Events on
// every task state transition.
type TaskStateUpdate struct {
	Type     string `json:"type"`
	TaskID   string `json:"task_id"`
	JobID    string `json:"job_id,omitempty"`
	OldState string `json:"old_state"`
	NewState string `json:"new_state"`
}

// JobCreatedEvent is published to sparkles-events and recorded in Events when a new job is submitted.
type JobCreatedEvent struct {
	Type       string `json:"type"`
	JobID      string `json:"job_id"`
	WorkpoolID string `json:"workpool_id"`
}

// JobTerminatedEvent is published to sparkles-events and recorded in Events when
// a job reaches a terminal state (success, error, failed, or killed).
type JobTerminatedEvent struct {
	Type       string `json:"type"`
	JobID      string `json:"job_id"`
	WorkpoolID string `json:"workpool_id"`
}

// EventPublisher writes events to the sparkles-events Pub/Sub topic and
// records a corresponding document in the Events Firestore collection.
// Firestore is written first (durable record), then Pub/Sub (real-time
// delivery). If the Pub/Sub publish fails the event is still preserved in
// Firestore.
type EventPublisher struct {
	publisher *pubsub.Publisher
	fs        *firestore.Client
}

func NewEventPublisher(publisher *pubsub.Publisher, fs *firestore.Client) *EventPublisher {
	return &EventPublisher{publisher: publisher, fs: fs}
}

// Stop flushes pending publishes and stops the underlying Pub/Sub publisher.
func (ep *EventPublisher) Stop() {
	if ep.publisher != nil {
		ep.publisher.Stop()
	}
}

func (ep *EventPublisher) recordAndPublish(ctx context.Context, record EventRecord, payload any) error {
	if ep.fs == nil || ep.publisher == nil {
		return nil
	}
	// Write to Firestore first so the event is durably recorded even if the
	// Pub/Sub publish subsequently fails.
	if _, err := ep.fs.Collection(eventCollection).Doc(record.EventID).Set(ctx, record); err != nil {
		return fmt.Errorf("recording event in firestore: %w", err)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling event: %w", err)
	}
	if _, err := ep.publisher.Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: map[string]string{"event_id": record.EventID},
	}).Get(ctx); err != nil {
		return fmt.Errorf("publishing event to topic: %w", err)
	}
	return nil
}

// PublishWorkerEvent records and publishes a worker lifecycle event.
func (ep *EventPublisher) PublishWorkerEvent(ctx context.Context, event WorkerEvent) error {
	now := time.Now()
	record := EventRecord{
		EventID:    uuid.New().String(),
		Type:       event.Type,
		Timestamp:  now,
		Expiry:     now.Add(eventTTL),
		WorkerID:   event.WorkerID,
		WorkpoolID: event.WorkpoolID,
	}
	return ep.recordAndPublish(ctx, record, event)
}

// PublishJobTerminated records and publishes a job termination event.
// Satisfies the monitor.JobTerminatedPublisher interface.
func (ep *EventPublisher) PublishJobTerminated(ctx context.Context, jobID, workpoolID string) error {
	event := JobTerminatedEvent{Type: "job_terminated", JobID: jobID, WorkpoolID: workpoolID}
	now := time.Now()
	record := EventRecord{
		EventID:    uuid.New().String(),
		Type:       "job_terminated",
		Timestamp:  now,
		Expiry:     now.Add(eventTTL),
		JobID:      jobID,
		WorkpoolID: workpoolID,
	}
	return ep.recordAndPublish(ctx, record, event)
}

// PublishJobCreated records and publishes a job creation event.
func (ep *EventPublisher) PublishJobCreated(ctx context.Context, event JobCreatedEvent) error {
	event.Type = "job_created"
	now := time.Now()
	record := EventRecord{
		EventID:    uuid.New().String(),
		Type:       "job_created",
		Timestamp:  now,
		Expiry:     now.Add(eventTTL),
		JobID:      event.JobID,
		WorkpoolID: event.WorkpoolID,
	}
	return ep.recordAndPublish(ctx, record, event)
}

// PublishTaskStateUpdate records and publishes a task state transition event.
func (ep *EventPublisher) PublishTaskStateUpdate(ctx context.Context, update TaskStateUpdate) error {
	now := time.Now()
	record := EventRecord{
		EventID:   uuid.New().String(),
		Type:      update.Type,
		Timestamp: now,
		Expiry:    now.Add(eventTTL),
		TaskID:    update.TaskID,
		JobID:     update.JobID,
		OldState:  update.OldState,
		NewState:  update.NewState,
	}
	return ep.recordAndPublish(ctx, record, update)
}
