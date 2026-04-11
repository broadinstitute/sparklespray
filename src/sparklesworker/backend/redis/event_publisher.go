package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/redis/go-redis/v9"
)

// RedisEventPublisher writes events to Redis (key "event:<nanosecond>") and
// publishes them to a Redis pub/sub channel. It implements backend.EventPublisher
// and mirrors the behaviour of FirestoreEventPublisher for local testing.
type RedisEventPublisher struct {
	client    *redis.Client
	channel   *RedisChannel
	topicName string
	eventTTL  time.Duration
}

// NewRedisEventPublisher creates a RedisEventPublisher. Events are stored under
// "event:<nanosecond-timestamp>" keys and published to topicName.
func NewRedisEventPublisher(client *redis.Client, channel *RedisChannel, topicName string) *RedisEventPublisher {
	return &RedisEventPublisher{
		client:    client,
		channel:   channel,
		topicName: topicName,
		eventTTL:  30 * 24 * time.Hour,
	}
}

// PublishEvent stamps the event's Expiry, stores it in Redis, then publishes
// a JSON-encoded copy to the configured Redis pub/sub channel.
func (p *RedisEventPublisher) PublishEvent(ctx context.Context, event backend.Event) error {
	event.Expiry = time.Now().Add(p.eventTTL).Format(time.RFC3339)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	key := fmt.Sprintf("event:%d", time.Now().UnixNano())
	if err := p.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("writing event to redis: %w", err)
	}

	if err := p.channel.Publish(ctx, p.topicName, data); err != nil {
		return fmt.Errorf("publishing event to redis channel: %w", err)
	}

	return nil
}
