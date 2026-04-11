package backend

import "context"

// NullChannel is a no-op ExtChannel that discards all published messages
// and never delivers any to subscribers.
type NullChannel struct{}

// NewNullChannel creates a NullChannel.
func NewNullChannel() *NullChannel {
	return &NullChannel{}
}

// Subscribe blocks until ctx is cancelled, delivering no messages.
func (c *NullChannel) Subscribe(ctx context.Context, _ string, _ func([]byte)) error {
	<-ctx.Done()
	return ctx.Err()
}

// Publish discards the message and returns nil.
func (c *NullChannel) Publish(_ context.Context, _ string, _ []byte) error {
	return nil
}

// NullEventPublisher is a no-op EventPublisher that discards all events.
type NullEventPublisher struct{}

// PublishEvent discards the event and returns nil.
func (n *NullEventPublisher) PublishEvent(_ context.Context, _ Event) error {
	return nil
}
