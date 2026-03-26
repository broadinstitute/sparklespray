package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/redis/go-redis/v9"
)

// RedisChannel implements Channel using Redis Pub/Sub
type RedisChannel struct {
	client          *redis.Client
	incomingChannel string
	responseChannel string
	workerID        string
}

// RedisConfig holds configuration for creating a RedisChannel
type RedisConfig struct {
	IncomingChannel string
	ResponseChannel string
	WorkerID        string
}

// NewRedisChannel creates a new RedisChannel
func NewRedisChannel(client *redis.Client, config RedisConfig) (*RedisChannel, error) {
	if config.IncomingChannel == "" || config.ResponseChannel == "" {
		return nil, fmt.Errorf("redis channels not configured: incomingChannel=%q, responseChannel=%q", config.IncomingChannel, config.ResponseChannel)
	}

	return &RedisChannel{
		client:          client,
		incomingChannel: config.IncomingChannel,
		responseChannel: config.ResponseChannel,
		workerID:        config.WorkerID,
	}, nil
}

// Listen starts receiving messages and calls the handler for each one
func (c *RedisChannel) Listen(ctx context.Context, handler backend.MessageHandler) error {
	log.Printf("Starting Redis control channel listener on channel %s", c.incomingChannel)

	pubsub := c.client.Subscribe(ctx, c.incomingChannel)
	defer pubsub.Close()

	// Wait for confirmation that subscription is created
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return fmt.Errorf("failed to subscribe to channel: %w", err)
	}

	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			c.handleMessage(ctx, msg, handler)
		}
	}
}

func (c *RedisChannel) handleMessage(ctx context.Context, msg *redis.Message, handler backend.MessageHandler) {
	// Parse the incoming message
	var incomingMsg backend.IncomingMessage
	if err := json.Unmarshal([]byte(msg.Payload), &incomingMsg); err != nil {
		log.Printf("Failed to unmarshal Redis message: %v", err)
		return
	}

	log.Printf("Received control message type=%s request_id=%s", incomingMsg.Type, incomingMsg.RequestID)

	// Convert to Message
	controlMsg := &backend.Message{
		Type:      incomingMsg.Type,
		RequestID: incomingMsg.RequestID,
		Payload:   incomingMsg.Payload,
	}

	// Call the handler
	response, shouldRespond := handler(ctx, controlMsg)

	// Send response if needed
	if shouldRespond && response != nil {
		outResp := backend.OutgoingResponse{
			Type:      response.Type,
			RequestID: response.RequestID,
			Payload:   response.Payload,
			Error:     response.Error,
		}
		if err := c.sendResponse(ctx, &outResp); err != nil {
			log.Printf("Failed to send control response: %v", err)
		}
	}
}

// Notify publishes an event to the response channel
func (c *RedisChannel) Notify(ctx context.Context, event interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Wrap in an envelope with worker_id for consistency
	envelope := map[string]interface{}{
		"worker_id": c.workerID,
		"data":      json.RawMessage(data),
	}

	// Extract event type if available
	if e, ok := event.(backend.WorkerStatusEvent); ok {
		envelope["event_type"] = e.Type
	}

	envelopeData, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope: %w", err)
	}

	err = c.client.Publish(ctx, c.responseChannel, envelopeData).Err()
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}

// Close cleans up resources (does not close the Redis client - caller owns it)
func (c *RedisChannel) Close() error {
	log.Printf("Closing Redis control channel for worker %s", c.workerID)
	return nil
}

func (c *RedisChannel) sendResponse(ctx context.Context, response *backend.OutgoingResponse) error {
	// Wrap response with worker_id
	envelope := map[string]interface{}{
		"worker_id": c.workerID,
		"response":  response,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	err = c.client.Publish(ctx, c.responseChannel, data).Err()
	if err != nil {
		return fmt.Errorf("failed to publish response: %w", err)
	}

	return nil
}
