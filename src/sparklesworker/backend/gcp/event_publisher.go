package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/backend"
)

// EventCollection is the Firestore collection where event records are stored.
const EventCollection = "Event"

// FirestoreEventPublisher writes events to Firestore and then publishes them
// to a Pub/Sub topic. It implements backend.EventPublisher.
type FirestoreEventPublisher struct {
	client    *firestore.Client
	channel   *PubSubChannel
	topicName string
	eventTTL  time.Duration
}

// NewFirestoreEventPublisher creates a FirestoreEventPublisher. Events are
// stored in the "Event" Firestore collection and published to topicName.
func NewFirestoreEventPublisher(client *firestore.Client, channel *PubSubChannel, topicName string) *FirestoreEventPublisher {
	return &FirestoreEventPublisher{
		client:    client,
		channel:   channel,
		topicName: topicName,
		eventTTL:  30 * 24 * time.Hour,
	}
}

// PublishEvent stamps the event's Expiry, writes it to Firestore, then
// publishes a JSON-encoded copy to the configured Pub/Sub topic.
func (p *FirestoreEventPublisher) PublishEvent(ctx context.Context, event backend.Event) error {
	event.Expiry = time.Now().Add(p.eventTTL).Format(time.RFC3339)

	if _, _, err := p.client.Collection(EventCollection).Add(ctx, event); err != nil {
		return fmt.Errorf("writing event to firestore: %w", err)
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	if err := p.channel.Publish(ctx, p.topicName, data); err != nil {
		return fmt.Errorf("publishing event to pubsub: %w", err)
	}

	return nil
}
