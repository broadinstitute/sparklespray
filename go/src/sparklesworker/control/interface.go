package control

import (
	"context"
	"encoding/json"
	"time"
)

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
