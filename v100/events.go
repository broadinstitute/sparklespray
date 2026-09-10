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
	// CleanlyTerminated is populated for worker_stopped: true when the worker
	// self-reported a normal shutdown, false when it was marked a zombie
	// (heartbeat expired without a clean shutdown).
	CleanlyTerminated bool `firestore:"cleanly_terminated"`
	// Task state update fields (populated for task_state_update)
	TaskID   string `firestore:"task_id"`
	JobID    string `firestore:"job_id"`
	OldState string `firestore:"old_state"`
	NewState string `firestore:"new_state"`
	// Workpool state change fields (populated for workpool_state_change)
	StateMessage string `firestore:"state_message"`
	// IncidentType is populated for workpool_incident: the kind of watchdog
	// anomaly detected (see monitor.IncidentType* constants, e.g. "zombie").
	IncidentType string `firestore:"incident_type"`
}

// WorkerEvent is published to sparkles-events and recorded in Events on worker
// lifecycle changes.
type WorkerEvent struct {
	Type       string `json:"type"`
	WorkerID   string `json:"worker_id"`
	WorkpoolID string `json:"workpool_id"`
	// CleanlyTerminated is populated for worker_stopped: true when the worker
	// self-reported a normal shutdown, false when it was marked a zombie
	// (heartbeat expired without a clean shutdown).
	CleanlyTerminated bool `json:"cleanly_terminated,omitempty"`
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

// WorkpoolStateChangeEvent is published to sparkles-events and recorded in Events
// whenever workpool state is saved.
type WorkpoolStateChangeEvent struct {
	Type         string `json:"type"`
	WorkpoolID   string `json:"workpool_id"`
	State        string `json:"state"`
	StateMessage string `json:"state_message"`
}

// BatchFailedEvent is published to sparkles-events and recorded in Events
// whenever a batch attempt (or a CreateJob call that never became a batch)
// fails.
type BatchFailedEvent struct {
	Type       string `json:"type"`
	WorkpoolID string `json:"workpool_id"`
	Reason     string `json:"reason"`
}

// BatchSucceededEvent is published to sparkles-events and recorded in Events
// once per batch, the first time it is confirmed healthy (its first worker
// registers).
type BatchSucceededEvent struct {
	Type       string `json:"type"`
	WorkpoolID string `json:"workpool_id"`
}

// WorkpoolIncidentEvent is published to sparkles-events and recorded in
// Events whenever the watchdog detects a batch/worker anomaly for a
// workpool. WorkPoolSummary's state_message/last_incident_at/incident_count
// fields are derived by querying recent workpool_incident events rather than
// from persisted mutable state.
type WorkpoolIncidentEvent struct {
	Type         string `json:"type"`
	WorkpoolID   string `json:"workpool_id"`
	Reason       string `json:"reason"`
	IncidentType string `json:"incident_type"`
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
		EventID:           uuid.New().String(),
		Type:              event.Type,
		Timestamp:         now,
		Expiry:            now.Add(eventTTL),
		WorkerID:          event.WorkerID,
		WorkpoolID:        event.WorkpoolID,
		CleanlyTerminated: event.CleanlyTerminated,
	}
	return ep.recordAndPublish(ctx, record, event)
}

// PublishWorkerStopped records and publishes a worker_stopped event.
// Satisfies the monitor.WorkerEventPublisher interface. cleanlyTerminated is
// false when the worker was marked a zombie (heartbeat expired without a
// clean shutdown), true for a normal, self-reported shutdown.
func (ep *EventPublisher) PublishWorkerStopped(ctx context.Context, workerID, workpoolID string, cleanlyTerminated bool) error {
	return ep.PublishWorkerEvent(ctx, WorkerEvent{
		Type:              "worker_stopped",
		WorkerID:          workerID,
		WorkpoolID:        workpoolID,
		CleanlyTerminated: cleanlyTerminated,
	})
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

// PublishWorkpoolStateChange records and publishes a workpool state change event.
// Satisfies the monitor.WorkpoolStatePublisher interface.
func (ep *EventPublisher) PublishWorkpoolStateChange(ctx context.Context, workpoolID, state, stateMessage string) error {
	event := WorkpoolStateChangeEvent{
		Type:         "workpool_state_change",
		WorkpoolID:   workpoolID,
		State:        state,
		StateMessage: stateMessage,
	}
	now := time.Now()
	record := EventRecord{
		EventID:      uuid.New().String(),
		Type:         "workpool_state_change",
		Timestamp:    now,
		Expiry:       now.Add(eventTTL),
		WorkpoolID:   workpoolID,
		NewState:     state,
		StateMessage: stateMessage,
	}
	return ep.recordAndPublish(ctx, record, event)
}

// PublishBatchFailed records and publishes a batch_failed event.
// Satisfies the monitor.BatchOutcomePublisher interface.
func (ep *EventPublisher) PublishBatchFailed(ctx context.Context, workpoolID, reason string) error {
	event := BatchFailedEvent{Type: "batch_failed", WorkpoolID: workpoolID, Reason: reason}
	now := time.Now()
	record := EventRecord{
		EventID:      uuid.New().String(),
		Type:         "batch_failed",
		Timestamp:    now,
		Expiry:       now.Add(eventTTL),
		WorkpoolID:   workpoolID,
		StateMessage: reason,
	}
	return ep.recordAndPublish(ctx, record, event)
}

// PublishBatchSucceeded records and publishes a batch_succeeded event.
// Satisfies the monitor.BatchOutcomePublisher interface.
func (ep *EventPublisher) PublishBatchSucceeded(ctx context.Context, workpoolID string) error {
	event := BatchSucceededEvent{Type: "batch_succeeded", WorkpoolID: workpoolID}
	now := time.Now()
	record := EventRecord{
		EventID:    uuid.New().String(),
		Type:       "batch_succeeded",
		Timestamp:  now,
		Expiry:     now.Add(eventTTL),
		WorkpoolID: workpoolID,
	}
	return ep.recordAndPublish(ctx, record, event)
}

// PublishWorkpoolIncident records and publishes a workpool_incident event.
// Satisfies the monitor.WorkpoolIncidentPublisher interface.
func (ep *EventPublisher) PublishWorkpoolIncident(ctx context.Context, workpoolID, incidentType, reason string) error {
	event := WorkpoolIncidentEvent{Type: "workpool_incident", WorkpoolID: workpoolID, Reason: reason, IncidentType: incidentType}
	now := time.Now()
	record := EventRecord{
		EventID:      uuid.New().String(),
		Type:         "workpool_incident",
		Timestamp:    now,
		Expiry:       now.Add(eventTTL),
		WorkpoolID:   workpoolID,
		StateMessage: reason,
		IncidentType: incidentType,
	}
	return ep.recordAndPublish(ctx, record, event)
}

// PublishTaskStateUpdate records and publishes a task state transition event.
// Implements monitor.TaskStatePublisher.
func (ep *EventPublisher) PublishTaskStateUpdate(ctx context.Context, taskID, jobID, oldState, newState string) error {
	update := TaskStateUpdate{
		Type:     "task_state_update",
		TaskID:   taskID,
		JobID:    jobID,
		OldState: oldState,
		NewState: newState,
	}
	now := time.Now()
	record := EventRecord{
		EventID:   uuid.New().String(),
		Type:      "task_state_update",
		Timestamp: now,
		Expiry:    now.Add(eventTTL),
		TaskID:    taskID,
		JobID:     jobID,
		OldState:  oldState,
		NewState:  newState,
	}
	return ep.recordAndPublish(ctx, record, update)
}
