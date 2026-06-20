package autoscaler

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/pubsub/v2"
)

const autoscalerSubscription = "autoscaler-in"

// batchNotificationMessage is the JSON payload published by GCP Batch API
// state-change notifications.
type batchNotificationMessage struct {
	JobName string `json:"jobName"`
}

// GCPPubSubReceiver implements PubSubReceiver by pulling from a GCP Pub/Sub
// subscription. It reverse-looks up the internal batch_id from the GCP job
// name using the BatchRequestStore.
type GCPPubSubReceiver struct {
	ch chan string
}

// NewGCPPubSubReceiver creates a receiver and starts a background pull loop.
// The loop runs until ctx is cancelled.
func NewGCPPubSubReceiver(ctx context.Context, project string, batches BatchRequestStore) (*GCPPubSubReceiver, error) {
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	r := &GCPPubSubReceiver{
		ch: make(chan string, 64),
	}

	sub := client.Subscriber(autoscalerSubscription)

	go func() {
		defer client.Close()
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()

			var n batchNotificationMessage
			if err := json.Unmarshal(msg.Data, &n); err != nil {
				log.Printf("pubsub: failed to parse notification: %v", err)
				return
			}
			if n.JobName == "" {
				return
			}

			batch, err := batches.GetByJobID(ctx, n.JobName)
			if err != nil {
				log.Printf("pubsub: lookup batch for job %s: %v", n.JobName, err)
				return
			}
			if batch == nil {
				return
			}

			select {
			case r.ch <- batch.BatchID:
			default:
				// Channel full — drop; the autoscaler has a periodic fallback.
			}
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("pubsub: receive loop exited: %v", err)
		}
	}()

	return r, nil
}

func (r *GCPPubSubReceiver) Notifications() <-chan string {
	return r.ch
}
