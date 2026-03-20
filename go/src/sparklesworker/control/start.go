package control

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// Notifier publishes worker status events via a Channel
type Notifier struct {
	channel  Channel
	workerID string
	ctx      context.Context
}

// NewNotifier creates a new worker notifier using a Channel
func NewNotifier(ctx context.Context, channel Channel, workerID string) *Notifier {
	return &Notifier{
		channel:  channel,
		workerID: workerID,
		ctx:      ctx,
	}
}

// NotifyWorkerStarted publishes a worker started event
func (n *Notifier) NotifyWorkerStarted() {
	n.publish(WorkerStatusEvent{
		Type:      WorkerEventStarted,
		WorkerID:  n.workerID,
		Timestamp: GetTimestampMillis(),
	})
}

// NotifyWorkerStopping publishes a worker stopping event
func (n *Notifier) NotifyWorkerStopping() {
	n.publish(WorkerStatusEvent{
		Type:      WorkerEventStopping,
		WorkerID:  n.workerID,
		Timestamp: GetTimestampMillis(),
	})
}

// NotifyTaskStarted publishes a task started event
func (n *Notifier) NotifyTaskStarted(taskID string) {
	n.publish(WorkerStatusEvent{
		Type:      WorkerEventTaskStarted,
		WorkerID:  n.workerID,
		Timestamp: GetTimestampMillis(),
		TaskID:    taskID,
	})
}

// NotifyTaskCompleted publishes a task completed event
func (n *Notifier) NotifyTaskCompleted(taskID string, exitCode string, errorMsg string) {
	n.publish(WorkerStatusEvent{
		Type:      WorkerEventTaskCompleted,
		WorkerID:  n.workerID,
		Timestamp: GetTimestampMillis(),
		TaskID:    taskID,
		ExitCode:  exitCode,
		Error:     errorMsg,
	})
}

func (n *Notifier) publish(event WorkerStatusEvent) {
	go func() {
		if err := n.channel.Notify(n.ctx, event); err != nil {
			log.Printf("Failed to publish worker status event: %v", err)
		}
	}()
}

// StartPubSubChannel starts a Pub/Sub control channel.
// Returns a Notifier and cleanup function.
func StartPubSubChannel(ctx context.Context, projectID, incomingTopic, responseTopic, workerID string, handler MessageHandler) (*Notifier, func(), error) {
	channel, err := NewPubSubChannel(ctx, PubSubConfig{
		ProjectID:     projectID,
		IncomingTopic: incomingTopic,
		ResponseTopic: responseTopic,
		WorkerID:      workerID,
	})
	if err != nil {
		return nil, nil, err
	}

	// Start listening in a goroutine
	go func() {
		err := channel.Listen(ctx, handler)
		if err != nil {
			log.Printf("Control channel listener error: %v", err)
		}
	}()

	cleanup := func() {
		channel.Close()
	}

	notifier := NewNotifier(ctx, channel, workerID)
	return notifier, cleanup, nil
}

// StartRedisChannel starts a Redis control channel.
// Returns a Notifier and cleanup function.
func StartRedisChannel(ctx context.Context, redisClient *redis.Client, cluster, workerID string, handler MessageHandler) (*Notifier, func(), error) {
	log.Printf("Creating Redis control channel for cluster %s", cluster)

	channel, err := NewRedisChannel(redisClient, RedisConfig{
		IncomingChannel: fmt.Sprintf("worker-commands:%s", cluster),
		ResponseChannel: fmt.Sprintf("worker-responses:%s", cluster),
		WorkerID:        workerID,
	})
	if err != nil {
		return nil, nil, err
	}

	// Start listening in a goroutine
	go func() {
		err := channel.Listen(ctx, handler)
		if err != nil && err != context.Canceled {
			log.Printf("Redis control channel listener error: %v", err)
		}
	}()

	cleanup := func() {
		channel.Close()
	}

	notifier := NewNotifier(ctx, channel, workerID)
	log.Printf("Redis control channel started for cluster %s", cluster)
	return notifier, cleanup, nil
}
