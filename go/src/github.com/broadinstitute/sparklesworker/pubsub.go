package sparklesworker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

// PubSubMessage wraps incoming requests with a type field to route them
type PubSubMessage struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id"`
	Payload   json.RawMessage `json:"payload"`
}

// PubSubResponse wraps outgoing responses
type PubSubResponse struct {
	Type      string      `json:"type"`
	RequestID string      `json:"request_id"`
	Payload   interface{} `json:"payload,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// ReadOutputRequest mirrors the protobuf ReadOutputRequest
type PubSubReadOutputRequest struct {
	TaskID   string `json:"task_id"`
	Size     int32  `json:"size"`
	Offset   int64  `json:"offset"`
	WorkerID string `json:"worker_id"`
}

// GetProcessStatusRequest contains the worker_id for filtering
type PubSubGetProcessStatusRequest struct {
	WorkerID string `json:"worker_id"`
}

// ReadOutputResponse mirrors the protobuf ReadOutputReply
type PubSubReadOutputResponse struct {
	Data      []byte `json:"data"`
	EndOfFile bool   `json:"end_of_file"`
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

// GetProcessStatusResponse mirrors the protobuf GetProcessStatusReply
type PubSubGetProcessStatusResponse struct {
	ProcessCount         int32 `json:"process_count"`
	TotalMemory          int64 `json:"total_memory"`
	TotalData            int64 `json:"total_data"`
	TotalShared          int64 `json:"total_shared"`
	TotalResident        int64 `json:"total_resident"`
	CpuUser              int64 `json:"cpu_user"`
	CpuSystem            int64 `json:"cpu_system"`
	CpuIdle              int64 `json:"cpu_idle"`
	CpuIowait            int64 `json:"cpu_iowait"`
	MemTotal             int64 `json:"mem_total"`
	MemAvailable         int64 `json:"mem_available"`
	MemFree              int64 `json:"mem_free"`
	MemPressureSomeAvg10 int32 `json:"mem_pressure_some_avg10"`
	MemPressureFullAvg10 int32 `json:"mem_pressure_full_avg10"`
}

// PubSubHandler handles pub/sub messages and sends responses
type PubSubHandler struct {
	monitor       *Monitor
	responseTopic *pubsub.Topic
	workerID      string
}

// NewPubSubHandler creates a new pub/sub handler
func NewPubSubHandler(monitor *Monitor, responseTopic *pubsub.Topic, workerID string) *PubSubHandler {
	return &PubSubHandler{
		monitor:       monitor,
		responseTopic: responseTopic,
		workerID:      workerID,
	}
}

// HandleMessage processes an incoming pub/sub message and sends a response
func (h *PubSubHandler) HandleMessage(ctx context.Context, msg *pubsub.Message) {
	var request PubSubMessage
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		log.Printf("Failed to unmarshal pub/sub message: %v", err)
		msg.Ack()
		return
	}

	log.Printf("Received pub/sub message type=%s request_id=%s", request.Type, request.RequestID)

	var response PubSubResponse
	response.Type = request.Type
	response.RequestID = request.RequestID

	// shouldRespond indicates whether this worker should send a response
	// (false when worker_id doesn't match - another worker should handle it)
	var shouldRespond bool

	switch request.Type {
	case "read_output":
		shouldRespond = h.handleReadOutput(ctx, &request, &response)
	case "get_process_status":
		shouldRespond = h.handleGetProcessStatus(ctx, &request, &response)
	default:
		response.Error = fmt.Sprintf("unknown message type: %s", request.Type)
		shouldRespond = true
	}

	// Only send response if this worker should handle the request
	if shouldRespond {
		if err := h.sendResponse(ctx, &response); err != nil {
			log.Printf("Failed to send pub/sub response: %v", err)
		}
	}

	msg.Ack()
}

func (h *PubSubHandler) handleReadOutput(ctx context.Context, request *PubSubMessage, response *PubSubResponse) bool {
	var req PubSubReadOutputRequest
	if err := json.Unmarshal(request.Payload, &req); err != nil {
		response.Error = fmt.Sprintf("failed to unmarshal read_output payload: %v", err)
		return true
	}

	// Check if this request is for this worker
	if req.WorkerID != "" && req.WorkerID != h.workerID {
		log.Printf("Ignoring read_output request for worker_id %s (this worker is %s)", req.WorkerID, h.workerID)
		return false
	}

	// Use the monitor's ReadOutput logic (reusing the same code path)
	knownTaskIds := make([]string, 0, 100)
	h.monitor.mutex.Lock()
	stdoutPath, ok := h.monitor.logPerTaskId[req.TaskID]
	for _, taskId := range h.monitor.logPerTaskId {
		knownTaskIds = append(knownTaskIds, taskId)
	}
	h.monitor.mutex.Unlock()

	if !ok {
		response.Error = fmt.Sprintf("unknown task: %s", req.TaskID)
		return true
	}

	// Read the file
	data, eof, err := readOutputFile(stdoutPath, req.Size, req.Offset)
	if err != nil {
		response.Error = fmt.Sprintf("failed to read output: %v", err)
		return true
	}

	response.Payload = PubSubReadOutputResponse{
		Data:      data,
		EndOfFile: eof,
	}
	return true
}

func (h *PubSubHandler) handleGetProcessStatus(ctx context.Context, request *PubSubMessage, response *PubSubResponse) bool {
	var req PubSubGetProcessStatusRequest
	if err := json.Unmarshal(request.Payload, &req); err != nil {
		// Payload might be empty for backwards compatibility, continue
		log.Printf("Could not unmarshal get_process_status payload (may be empty): %v", err)
	}

	// Check if this request is for this worker
	if req.WorkerID != "" && req.WorkerID != h.workerID {
		log.Printf("Ignoring get_process_status request for worker_id %s (this worker is %s)", req.WorkerID, h.workerID)
		return false
	}

	mem, err := getMemoryUsage()
	if err != nil {
		response.Error = fmt.Sprintf("failed to get memory usage: %v", err)
		return true
	}

	resp := PubSubGetProcessStatusResponse{
		TotalMemory:   mem.totalSize * PAGE_SIZE,
		TotalData:     mem.totalData * PAGE_SIZE,
		TotalShared:   mem.totalShared * PAGE_SIZE,
		TotalResident: mem.totalResident * PAGE_SIZE,
		ProcessCount:  int32(mem.procCount),
	}

	// Add CPU stats (best effort)
	if cpu, err := getCPUStats(); err == nil {
		resp.CpuUser = cpu.User
		resp.CpuSystem = cpu.System
		resp.CpuIdle = cpu.Idle
		resp.CpuIowait = cpu.Iowait
	}

	// Add system memory info (best effort)
	if sysMem, err := getSystemMemory(); err == nil {
		resp.MemTotal = sysMem.Total
		resp.MemAvailable = sysMem.Available
		resp.MemFree = sysMem.Free
	}

	// Add memory pressure
	pressure := getMemoryPressure()
	resp.MemPressureSomeAvg10 = pressure.SomeAvg10
	resp.MemPressureFullAvg10 = pressure.FullAvg10

	response.Payload = resp
	return true
}

func (h *PubSubHandler) sendResponse(ctx context.Context, response *PubSubResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	result := h.responseTopic.Publish(ctx, &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"worker_id": h.workerID,
		},
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish response: %w", err)
	}

	return nil
}

// StartPubSubSubscriber starts listening for messages on the incoming topic
// Returns a WorkerNotifier that can be used to publish status events
func StartPubSubSubscriber(ctx context.Context, projectID string, incomingTopic string, responseTopic string, monitor *Monitor, workerID string) (*WorkerNotifier, error) {
	if incomingTopic == "" || responseTopic == "" {
		return nil, fmt.Errorf("pub/sub topics not configured: incomingTopic=%q, responseTopic=%q", incomingTopic, responseTopic)
	}

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create pub/sub client: %w", err)
	}

	// Get or create subscription for this worker
	// Use workerID as part of subscription name to make it unique per worker
	subName := fmt.Sprintf("%s-%s", incomingTopic, workerID)
	sub := client.Subscription(subName)

	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check subscription existence: %w", err)
	}

	if !exists {
		topic := client.Topic(incomingTopic)
		sub, err = client.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
			Topic: topic,
			// Auto-delete subscription after 1 hour of inactivity
			ExpirationPolicy: time.Hour,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create subscription: %w", err)
		}
		log.Printf("Created pub/sub subscription: %s", subName)
	}

	respTopic := client.Topic(responseTopic)
	handler := NewPubSubHandler(monitor, respTopic, workerID)

	// Start receiving messages in a goroutine
	go func() {
		log.Printf("Starting pub/sub subscriber on topic %s", incomingTopic)
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			handler.HandleMessage(ctx, msg)
		})
		if err != nil {
			log.Printf("Pub/sub subscriber error: %v", err)
		}
	}()

	// Create and return notifier for publishing status events
	notifier := NewWorkerNotifier(ctx, respTopic, workerID)
	return notifier, nil
}

// readOutputFile reads from a file at the given offset
func readOutputFile(path string, size int32, offset int64) ([]byte, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	buffer := make([]byte, size)
	n, err := f.ReadAt(buffer, offset)
	buffer = buffer[:n]

	eof := err != nil && err == io.EOF
	if err != nil && err != io.EOF {
		return nil, false, err
	}

	return buffer, eof, nil
}

// WorkerNotifier publishes worker status events to pub/sub
type WorkerNotifier struct {
	topic    *pubsub.Topic
	workerID string
	ctx      context.Context
}

// NewWorkerNotifier creates a new worker notifier
func NewWorkerNotifier(ctx context.Context, topic *pubsub.Topic, workerID string) *WorkerNotifier {
	return &WorkerNotifier{
		topic:    topic,
		workerID: workerID,
		ctx:      ctx,
	}
}

// NotifyWorkerStarted publishes a worker started event
func (n *WorkerNotifier) NotifyWorkerStarted() {
	n.publish(WorkerStatusEvent{
		Type:      WorkerEventStarted,
		WorkerID:  n.workerID,
		Timestamp: getTimestampMillis(),
	})
}

// NotifyWorkerStopping publishes a worker stopping event
func (n *WorkerNotifier) NotifyWorkerStopping() {
	n.publish(WorkerStatusEvent{
		Type:      WorkerEventStopping,
		WorkerID:  n.workerID,
		Timestamp: getTimestampMillis(),
	})
}

// NotifyTaskStarted publishes a task started event
func (n *WorkerNotifier) NotifyTaskStarted(taskID string) {
	n.publish(WorkerStatusEvent{
		Type:      WorkerEventTaskStarted,
		WorkerID:  n.workerID,
		Timestamp: getTimestampMillis(),
		TaskID:    taskID,
	})
}

// NotifyTaskCompleted publishes a task completed event
func (n *WorkerNotifier) NotifyTaskCompleted(taskID string, exitCode string, errorMsg string) {
	n.publish(WorkerStatusEvent{
		Type:      WorkerEventTaskCompleted,
		WorkerID:  n.workerID,
		Timestamp: getTimestampMillis(),
		TaskID:    taskID,
		ExitCode:  exitCode,
		Error:     errorMsg,
	})
}

func (n *WorkerNotifier) publish(event WorkerStatusEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal worker status event: %v", err)
		return
	}

	result := n.topic.Publish(n.ctx, &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"worker_id":  n.workerID,
			"event_type": event.Type,
		},
	})

	// Don't block on publish, just log errors
	go func() {
		_, err := result.Get(n.ctx)
		if err != nil {
			log.Printf("Failed to publish worker status event: %v", err)
		}
	}()
}
