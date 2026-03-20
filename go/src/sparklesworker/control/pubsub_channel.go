package control

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

// PubSubChannel implements Channel using Google Cloud Pub/Sub
type PubSubChannel struct {
	client        *pubsub.Client
	subscription  *pubsub.Subscription
	responseTopic *pubsub.Topic
	workerID      string
	subName       string
}

// PubSubConfig holds configuration for creating a PubSubChannel
type PubSubConfig struct {
	ProjectID     string
	IncomingTopic string
	ResponseTopic string
	WorkerID      string
}

// NewPubSubChannel creates a new PubSubChannel
func NewPubSubChannel(ctx context.Context, config PubSubConfig) (*PubSubChannel, error) {
	if config.IncomingTopic == "" || config.ResponseTopic == "" {
		return nil, fmt.Errorf("pub/sub topics not configured: incomingTopic=%q, responseTopic=%q", config.IncomingTopic, config.ResponseTopic)
	}

	client, err := pubsub.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create pub/sub client: %w", err)
	}

	// Get or create subscription for this worker
	subName := fmt.Sprintf("%s-%s", config.IncomingTopic, config.WorkerID)
	sub := client.Subscription(subName)

	exists, err := sub.Exists(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to check subscription existence: %w", err)
	}

	if !exists {
		topic := client.Topic(config.IncomingTopic)
		sub, err = client.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
			Topic:            topic,
			ExpirationPolicy: 24 * time.Hour,
		})
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create subscription: %w", err)
		}
		log.Printf("Created pub/sub subscription: %s", subName)
	}

	respTopic := client.Topic(config.ResponseTopic)

	return &PubSubChannel{
		client:        client,
		subscription:  sub,
		responseTopic: respTopic,
		workerID:      config.WorkerID,
		subName:       subName,
	}, nil
}

// Listen starts receiving messages and calls the handler for each one
func (c *PubSubChannel) Listen(ctx context.Context, handler MessageHandler) error {
	log.Printf("Starting pub/sub control channel listener on subscription %s", c.subName)

	return c.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Parse the incoming message
		var incomingMsg IncomingMessage
		if err := json.Unmarshal(msg.Data, &incomingMsg); err != nil {
			log.Printf("Failed to unmarshal pub/sub message: %v", err)
			msg.Ack()
			return
		}

		log.Printf("Received control message type=%s request_id=%s", incomingMsg.Type, incomingMsg.RequestID)

		// Convert to Message
		controlMsg := &Message{
			Type:      incomingMsg.Type,
			RequestID: incomingMsg.RequestID,
			Payload:   incomingMsg.Payload,
		}

		// Call the handler
		response, shouldRespond := handler(ctx, controlMsg)

		// Send response if needed
		if shouldRespond && response != nil {
			outResp := OutgoingResponse{
				Type:      response.Type,
				RequestID: response.RequestID,
				Payload:   response.Payload,
				Error:     response.Error,
			}
			if err := c.sendResponse(ctx, &outResp); err != nil {
				log.Printf("Failed to send control response: %v", err)
			}
		}

		msg.Ack()
	})
}

// Notify publishes an event to the response topic
func (c *PubSubChannel) Notify(ctx context.Context, event interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Extract event type if available
	eventType := ""
	if e, ok := event.(WorkerStatusEvent); ok {
		eventType = e.Type
	}

	result := c.responseTopic.Publish(ctx, &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"worker_id":  c.workerID,
			"event_type": eventType,
		},
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}

// Close cleans up the subscription and closes the client
func (c *PubSubChannel) Close() error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.subscription.Delete(cleanupCtx); err != nil {
		log.Printf("Failed to delete pub/sub subscription %s: %v", c.subName, err)
	} else {
		log.Printf("Deleted pub/sub subscription: %s", c.subName)
	}

	return c.client.Close()
}

func (c *PubSubChannel) sendResponse(ctx context.Context, response *OutgoingResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	result := c.responseTopic.Publish(ctx, &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"worker_id": c.workerID,
		},
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish response: %w", err)
	}

	return nil
}
