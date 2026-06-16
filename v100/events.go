package v100

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

const eventCollection = "Events"
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

// EventPublisher writes events to the sparkles-events Pub/Sub topic and
// records a corresponding document in the Events Firestore collection.
// Firestore is written first (durable record), then Pub/Sub (real-time
// delivery). If the Pub/Sub publish fails the event is still preserved in
// Firestore.
type EventPublisher struct {
	topic *pubsub.Topic
	fs    *firestore.Client
}

func NewEventPublisher(topic *pubsub.Topic, fs *firestore.Client) *EventPublisher {
	return &EventPublisher{topic: topic, fs: fs}
}

// Stop flushes pending publishes and stops the underlying Pub/Sub topic client.
func (ep *EventPublisher) Stop() {
	ep.topic.Stop()
}

func (ep *EventPublisher) recordAndPublish(ctx context.Context, record EventRecord, payload any) error {
	// Write to Firestore first so the event is durably recorded even if the
	// Pub/Sub publish subsequently fails.
	if _, err := ep.fs.Collection(eventCollection).Doc(record.EventID).Set(ctx, record); err != nil {
		return fmt.Errorf("recording event in firestore: %w", err)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling event: %w", err)
	}
	if _, err := ep.topic.Publish(ctx, &pubsub.Message{Data: data}).Get(ctx); err != nil {
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
