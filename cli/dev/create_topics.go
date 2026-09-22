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

// sprinklesTopics is the canonical list of Pub/Sub topics and subscriptions
// required by sprinkles, as described in docs/design/datamodel.md.
var sprinklesTopics = []topicSpec{
	{
		topic:         "sprinkles-events",
		subscriptions: []string{"monitor-events-in"},
	},
	{
		topic:         "batch-api-notifications",
		subscriptions: []string{"batch-api-notifications"},
	},
	{
		// Per-worker subscriptions are created at worker startup;
		// only the topic needs to exist in advance.
		topic: "sprinkles-worker-in",
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

	return createTopics(ctx, psClient, project)
}

// createTopics creates every Pub/Sub topic/subscription in sprinklesTopics,
// tolerating ones that already exist. Exported for reuse by both "dev
// create-topics" and "dev bootstrap-project".
func createTopics(ctx context.Context, psClient *pubsub.Client, project string) error {
	for _, spec := range sprinklesTopics {
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
