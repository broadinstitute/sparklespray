package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"log"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const batchAPINotificationsSubscription = "batch-api-notifications"
const sparklesEventsTopic = "sparkles-events"
const monitorEventsSubscription = "monitor-events-in"

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

	if err := ensurePubSubResources(ctx, client, project, batchAPINotificationsSubscription, batchAPINotificationsSubscription); err != nil {
		client.Close()
		return nil, fmt.Errorf("ensuring pubsub subscription: %w", err)
	}

	r := &GCPPubSubReceiver{
		ch: make(chan Notification, 64),
	}

	sub := client.Subscriber(batchAPINotificationsSubscription)

	go func() {
		defer client.Close()
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()

			attrs := make([]string, 0, len(msg.Attributes))
			for k, v := range msg.Attributes {
				attrs = append(attrs, k+"="+v)
			}
			log.Printf("pubsub: message arrived with attributes: %v", attrs)

			jobName := msg.Attributes["JobName"]
			if jobName == "" {
				if taskName := msg.Attributes["TaskName"]; taskName != "" {
					if prefix, _, ok := strings.Cut(taskName, "/taskGroups/"); ok {
						jobName = prefix
					}
				}
			}
			if jobName == "" {
				attrs := make([]string, 0, len(msg.Attributes))
				for k, v := range msg.Attributes {
					attrs = append(attrs, k+"="+v)
				}
				log.Printf("pubsub: missing JobName/TaskName attribute, data was: \"%s\", attributes: %v", msg.Data, attrs)
				return
			}

			batch, err := batches.GetByJobID(ctx, jobName)
			if err != nil {
				log.Printf("pubsub: lookup batch for job %s: %v", jobName, err)
				return
			}
			if batch == nil {
				return
			}

			select {
			case r.ch <- Notification{BatchID: batch.BatchID}:
			default:
				// Channel full — drop; the monitor has a periodic fallback.
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

// sparklesEventMessage is the minimal JSON shape shared by all sparkles-events payloads.
type sparklesEventMessage struct {
	Type       string `json:"type"`
	JobID      string `json:"job_id"`
	WorkpoolID string `json:"workpool_id"`
}

// GCPJobEventReceiver implements JobEventReceiver by pulling from the sparkles-events
// Pub/Sub topic and forwarding job_created and task_state_update events.
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

	if err := ensurePubSubResources(ctx, client, project, sparklesEventsTopic, monitorEventsSubscription); err != nil {
		client.Close()
		return nil, fmt.Errorf("ensuring job events subscription: %w", err)
	}

	r := &GCPJobEventReceiver{ch: make(chan JobNotification, 64)}
	sub := client.Subscriber(monitorEventsSubscription)

	go func() {
		defer client.Close()
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
			var e sparklesEventMessage
			if err := json.Unmarshal(msg.Data, &e); err != nil {
				return
			}
			switch e.Type {
			case "job_created", "task_state_update":
			default:
				return
			}
			log.Printf("pubsub: received %s event for job %s", e.Type, e.JobID)
			select {
			case r.ch <- JobNotification{EventType: e.Type, JobID: e.JobID, WorkpoolID: e.WorkpoolID}:
			default:
				// Channel full — drop; the monitor has a periodic fallback.
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
