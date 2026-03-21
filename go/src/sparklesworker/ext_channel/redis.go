package ext_channel

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// RedisChannel implements ExtChannel using Redis pub/sub.
type RedisChannel struct {
	client *redis.Client
}

// NewRedisChannel creates a RedisChannel backed by the given Redis client.
func NewRedisChannel(client *redis.Client) *RedisChannel {
	return &RedisChannel{client: client}
}

// Subscribe subscribes to the Redis channel named topicName and calls
// callback for each message until ctx is cancelled.
func (c *RedisChannel) Subscribe(ctx context.Context, topicName string, callback func([]byte)) error {
	ps := c.client.Subscribe(ctx, topicName)
	defer ps.Close()

	// Wait for subscription confirmation.
	if _, err := ps.Receive(ctx); err != nil {
		return fmt.Errorf("redis: subscribing to %s: %w", topicName, err)
	}

	log.Printf("ext_channel/redis: subscribed to %s", topicName)
	ch := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			callback([]byte(msg.Payload))
		}
	}
}

// Publish sends message to the Redis channel named topicName.
func (c *RedisChannel) Publish(ctx context.Context, topicName string, message []byte) error {
	if err := c.client.Publish(ctx, topicName, message).Err(); err != nil {
		return fmt.Errorf("redis: publishing to %s: %w", topicName, err)
	}
	return nil
}
