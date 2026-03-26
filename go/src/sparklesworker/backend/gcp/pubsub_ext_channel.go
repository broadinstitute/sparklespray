package gcp

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

// PubSubChannel implements ExtChannel using Google Cloud Pub/Sub.
type PubSubChannel struct {
	projectID string
}

// NewPubSubChannel creates a PubSubChannel for the given GCP project.
func NewPubSubChannel(projectID string) *PubSubChannel {
	return &PubSubChannel{projectID: projectID}
}

// Subscribe creates a per-process subscription on topicName and calls
// callback for each message until ctx is cancelled.
func (c *PubSubChannel) Subscribe(ctx context.Context, topicName string, callback func([]byte)) error {
	client, err := pubsub.NewClient(ctx, c.projectID)
	if err != nil {
		return fmt.Errorf("pubsub: creating client: %w", err)
	}
	defer client.Close()

	subName := fmt.Sprintf("%s-worker", topicName)
	sub := client.Subscription(subName)

	exists, err := sub.Exists(ctx)
	if err != nil {
		return fmt.Errorf("pubsub: checking subscription %s: %w", subName, err)
	}
	if !exists {
		topic := client.Topic(topicName)
		sub, err = client.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
			Topic:            topic,
			ExpirationPolicy: 24 * time.Hour,
		})
		if err != nil {
			return fmt.Errorf("pubsub: creating subscription %s: %w", subName, err)
		}
		log.Printf("ext_channel/pubsub: created subscription %s", subName)
	}

	log.Printf("ext_channel/pubsub: subscribing on %s", subName)
	return sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		callback(msg.Data)
		msg.Ack()
	})
}

// Publish sends message to the named Pub/Sub topic.
func (c *PubSubChannel) Publish(ctx context.Context, topicName string, message []byte) error {
	client, err := pubsub.NewClient(ctx, c.projectID)
	if err != nil {
		return fmt.Errorf("pubsub: creating client: %w", err)
	}
	defer client.Close()

	topic := client.Topic(topicName)
	result := topic.Publish(ctx, &pubsub.Message{Data: message})
	if _, err := result.Get(ctx); err != nil {
		return fmt.Errorf("pubsub: publishing to %s: %w", topicName, err)
	}
	return nil
}
