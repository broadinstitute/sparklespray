package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const autoscalerSubscription = "autoscaler-in"
const sparklesEventsTopic = "sparkles-events"
const autoscalerEventsSubscription = "autoscaler-events-in"

// batchNotificationMessage is the JSON payload published by GCP Batch API
// state-change notifications.
type batchNotificationMessage struct {
	JobName string `json:"jobName"`
}

// GCPPubSubReceiver implements PubSubReceiver by pulling from a GCP Pub/Sub
// subscription. It reverse-looks up the internal batch_id from the GCP job
// name using the BatchRequestStore.
type GCPPubSubReceiver struct {
	ch chan Notification
}

// ensurePubSubResources creates the named topic and subscription if they do not exist.
func ensurePubSubResources(ctx context.Context, client *pubsub.Client, project, topicID, subID string) error {
	topicName := fmt.Sprintf("projects/%s/topics/%s", project, topicID)
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", project, subID)

	_, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: topicName})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return fmt.Errorf("checking topic: %w", err)
		}
		if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName}); err != nil {
			return fmt.Errorf("creating topic %s: %w", topicName, err)
		}
		log.Printf("pubsub: created topic %s", topicName)
	}

	_, err = client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subName})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return fmt.Errorf("checking subscription: %w", err)
		}
		if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:  subName,
			Topic: topicName,
		}); err != nil {
			return fmt.Errorf("creating subscription %s: %w", subName, err)
		}
		log.Printf("pubsub: created subscription %s", subName)
	}

	return nil
}

// NewGCPPubSubReceiver creates a receiver and starts a background pull loop.
// The loop runs until ctx is cancelled. If the loop exits due to a fatal error
// (rather than ctx cancellation), a Notification with Err set is sent so the
// caller can fail fast.
func NewGCPPubSubReceiver(ctx context.Context, project string, batches BatchRequestStore) (*GCPPubSubReceiver, error) {
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	if err := ensurePubSubResources(ctx, client, project, autoscalerSubscription, autoscalerSubscription); err != nil {
		client.Close()
		return nil, fmt.Errorf("ensuring pubsub subscription: %w", err)
	}

	r := &GCPPubSubReceiver{
		ch: make(chan Notification, 64),
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
			case r.ch <- Notification{BatchID: batch.BatchID}:
			default:
				// Channel full — drop; the autoscaler has a periodic fallback.
			}
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("pubsub: receive loop exited: %v", err)
			r.ch <- Notification{Err: err}
		}
	}()

	return r, nil
}

func (r *GCPPubSubReceiver) Notifications() <-chan Notification {
	return r.ch
}

// jobCreatedMessage is the JSON payload for a job_created event on sparkles-events.
type jobCreatedMessage struct {
	Type       string `json:"type"`
	JobID      string `json:"job_id"`
	WorkpoolID string `json:"workpool_id"`
}

// GCPJobEventReceiver implements JobEventReceiver by pulling from the sparkles-events
// Pub/Sub topic and forwarding job_created events.
type GCPJobEventReceiver struct {
	ch chan JobNotification
}

// NewGCPJobEventReceiver creates a receiver and starts a background pull loop.
// If the loop exits due to a fatal error, a JobNotification with Err set is sent.
func NewGCPJobEventReceiver(ctx context.Context, project string) (*GCPJobEventReceiver, error) {
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	if err := ensurePubSubResources(ctx, client, project, sparklesEventsTopic, autoscalerEventsSubscription); err != nil {
		client.Close()
		return nil, fmt.Errorf("ensuring job events subscription: %w", err)
	}

	r := &GCPJobEventReceiver{ch: make(chan JobNotification, 64)}
	sub := client.Subscriber(autoscalerEventsSubscription)

	go func() {
		defer client.Close()
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
			var e jobCreatedMessage
			if err := json.Unmarshal(msg.Data, &e); err != nil || e.Type != "job_created" {
				return
			}
			log.Printf("pubsub: received job_created event for job %s workpool %s", e.JobID, e.WorkpoolID)
			select {
			case r.ch <- JobNotification{JobID: e.JobID, WorkpoolID: e.WorkpoolID}:
			default:
				// Channel full — drop; the autoscaler has a periodic fallback.
			}
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("pubsub: job event receive loop exited: %v", err)
			r.ch <- JobNotification{Err: err}
		}
	}()

	return r, nil
}

func (r *GCPJobEventReceiver) JobEvents() <-chan JobNotification {
	return r.ch
}
