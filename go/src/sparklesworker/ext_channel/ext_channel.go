package ext_channel

import "context"

// ExtChannel is an abstract publish/subscribe mechanism. Implementations
// include PubSubChannel (Google Cloud Pub/Sub) and RedisChannel (Redis
// pub/sub), as well as NullChannel for testing/no-op use.
type ExtChannel interface {
	// Subscribe listens on topicName and calls callback for each received
	// message. It blocks until the context is cancelled or an error occurs.
	Subscribe(ctx context.Context, topicName string, callback func(message []byte)) error

	// Publish sends message to topicName.
	Publish(ctx context.Context, topicName string, message []byte) error
}
