package backend

import (
	"context"
	"log"
)

// StartLogStream subscribes to topicName on channel, printing each received
// message. It returns a cancel function that stops the stream.
func StartLogStream(ctx context.Context, channel MessageBus, topicName string) context.CancelFunc {
	streamCtx, cancel := context.WithCancel(ctx)
	if channel != nil {
		go channel.Subscribe(streamCtx, topicName, func(message []byte) {
			log.Printf("Message: %s", string(message))
		})
	}
	return cancel
}
