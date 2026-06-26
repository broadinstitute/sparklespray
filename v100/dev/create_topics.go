package dev

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/urfave/cli"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// topicSpec describes a Pub/Sub topic and its associated subscriptions.
type topicSpec struct {
	topic         string
	subscriptions []string
}

// sparklesTopics is the canonical list of Pub/Sub topics and subscriptions
// required by sparklespray, as described in datamodel.md.
var sparklesTopics = []topicSpec{
	{
		topic:         "sparkles-events",
		subscriptions: []string{"monitor-events-in"},
	},
	{
		topic:         "batch-api-notifications",
		subscriptions: []string{"batch-api-notifications"},
	},
	{
		// Per-worker subscriptions are created at worker startup;
		// only the topic needs to exist in advance.
		topic: "sparkles-worker-in",
	},
}

func runDevCreateTopics(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}

	ctx := context.Background()
	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer psClient.Close()

	for _, spec := range sparklesTopics {
		topicName := fmt.Sprintf("projects/%s/topics/%s", project, spec.topic)
		if _, err := psClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName}); err != nil {
			if grpcstatus.Code(err) == codes.AlreadyExists {
				fmt.Printf("topic already exists:  %s\n", spec.topic)
			} else {
				return fmt.Errorf("creating topic %s: %w", spec.topic, err)
			}
		} else {
			fmt.Printf("topic created:         %s\n", spec.topic)
		}

		for _, sub := range spec.subscriptions {
			subName := fmt.Sprintf("projects/%s/subscriptions/%s", project, sub)
			if _, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
				Name:  subName,
				Topic: topicName,
			}); err != nil {
				if grpcstatus.Code(err) == codes.AlreadyExists {
					fmt.Printf("subscription already exists: %s\n", sub)
				} else {
					return fmt.Errorf("creating subscription %s: %w", sub, err)
				}
			} else {
				fmt.Printf("subscription created:  %s\n", sub)
			}
		}
	}

	return nil
}
