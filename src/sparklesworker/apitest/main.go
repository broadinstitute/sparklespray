package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/pubsub"
)

const topicName = "apitest-out"
const outDir = "apitest-out"

func main() {
	projectID := flag.String("project", os.Getenv("GOOGLE_CLOUD_PROJECT"), "GCP project ID")
	location := flag.String("location", "us-central1", "GCP region for the batch job")
	flag.Parse()

	if *projectID == "" {
		log.Fatal("project ID required: set --project or GOOGLE_CLOUD_PROJECT env var")
	}

	ctx := context.Background()

	// Step 1: Ensure pubsub topic exists.
	log.Printf("Connecting to pubsub (project=%s)...", *projectID)
	psClient, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	defer psClient.Close()

	topic := psClient.Topic(topicName)
	exists, err := topic.Exists(ctx)
	if err != nil {
		log.Fatalf("checking topic %q: %v", topicName, err)
	}
	if !exists {
		log.Printf("Creating pubsub topic %q...", topicName)
		topic, err = psClient.CreateTopic(ctx, topicName)
		if err != nil {
			log.Fatalf("creating topic %q: %v", topicName, err)
		}
		log.Printf("Topic %q created.", topicName)
	} else {
		log.Printf("Topic %q already exists.", topicName)
	}

	// Step 2: Create subscription and start receiving messages.
	subName := topicName + "-apitest-sub"
	sub := psClient.Subscription(subName)
	exists, err = sub.Exists(ctx)
	if err != nil {
		log.Fatalf("checking subscription %q: %v", subName, err)
	}
	if !exists {
		log.Printf("Creating subscription %q...", subName)
		sub, err = psClient.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
			Topic:            topic,
			ExpirationPolicy: 24 * time.Hour,
		})
		if err != nil {
			log.Fatalf("creating subscription %q: %v", subName, err)
		}
		log.Printf("Subscription %q created.", subName)
	} else {
		log.Printf("Subscription %q already exists.", subName)
	}

	// Ensure output directory exists.
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("creating output dir %q: %v", outDir, err)
	}
	log.Printf("Event files will be written to %q/", outDir)

	// Start receiving pubsub messages in the background.
	receiveCtx, cancelReceive := context.WithCancel(ctx)
	receiveErr := make(chan error, 1)
	go func() {
		log.Print("Pubsub receiver started.")
		err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			ts := time.Now().UTC().Format("20060102-150405.000000000")

			dataFile := filepath.Join(outDir, ts+".data")
			if werr := os.WriteFile(dataFile, msg.Data, 0644); werr != nil {
				log.Printf("Failed to write data file %s: %v", dataFile, werr)
			} else {
				log.Printf("Event received -> %s (%d bytes)", dataFile, len(msg.Data))
			}

			var attrLines []byte
			for k, v := range msg.Attributes {
				attrLines = append(attrLines, []byte(k+"="+v+"\n")...)
			}
			attrsFile := filepath.Join(outDir, ts+".attrs")
			if werr := os.WriteFile(attrsFile, attrLines, 0644); werr != nil {
				log.Printf("Failed to write attrs file %s: %v", attrsFile, werr)
			}

			msg.Ack()
		})
		receiveErr <- err
	}()

	// Step 3: Create the batch job with three tasks running "echo hello".
	log.Print("Creating batch client...")
	batchClient, err := batch.NewClient(ctx)
	if err != nil {
		log.Fatalf("batch.NewClient: %v", err)
	}
	defer batchClient.Close()

	fullTopic := fmt.Sprintf("projects/%s/topics/%s", *projectID, topicName)
	jobID := fmt.Sprintf("apitest-%d", time.Now().Unix())
	parent := fmt.Sprintf("projects/%s/locations/%s", *projectID, *location)

	log.Printf("Creating batch job %q (parent=%s)...", jobID, parent)
	createdJob, err := batchClient.CreateJob(ctx, &batchpb.CreateJobRequest{
		Parent: parent,
		JobId:  jobID,
		Job: &batchpb.Job{
			TaskGroups: []*batchpb.TaskGroup{
				{
					TaskCount: 3,
					TaskSpec: &batchpb.TaskSpec{
						Runnables: []*batchpb.Runnable{
							{
								Executable: &batchpb.Runnable_Script_{
									Script: &batchpb.Runnable_Script{
										Command: &batchpb.Runnable_Script_Text{
											Text: "sleep 30",
										},
									},
								},
							},
							{
								Executable: &batchpb.Runnable_Script_{
									Script: &batchpb.Runnable_Script{
										Command: &batchpb.Runnable_Script_Text{
											Text: "echo hello2",
										},
									},
								},
							},
						},
					},
				},
			},
			AllocationPolicy: &batchpb.AllocationPolicy{
				Location: &batchpb.AllocationPolicy_LocationPolicy{
					AllowedLocations: []string{"regions/" + *location},
				},
			},
			LogsPolicy: &batchpb.LogsPolicy{
				Destination: batchpb.LogsPolicy_CLOUD_LOGGING,
			},
			Notifications: []*batchpb.JobNotification{
				{
					PubsubTopic: fullTopic,
					Message: &batchpb.JobNotification_Message{
						Type: batchpb.JobNotification_JOB_STATE_CHANGED,
					},
				},
				{
					PubsubTopic: fullTopic,
					Message: &batchpb.JobNotification_Message{
						Type: batchpb.JobNotification_TASK_STATE_CHANGED,
					},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("creating batch job: %v", err)
	}
	log.Printf("Batch job created: %s", createdJob.Name)

	// Step 4: Poll job status every 10 seconds until a terminal state is reached.
	log.Print("Polling job status every 10 seconds...")
	for {
		time.Sleep(10 * time.Second)

		j, err := batchClient.GetJob(ctx, &batchpb.GetJobRequest{Name: createdJob.Name})
		if err != nil {
			log.Printf("GetJob error: %v", err)
			continue
		}

		state := j.Status.State
		log.Printf("Job state: %s", state)

		if state == batchpb.JobStatus_SUCCEEDED ||
			state == batchpb.JobStatus_FAILED ||
			state == batchpb.JobStatus_DELETION_IN_PROGRESS {
			log.Printf("Job reached terminal state %s, shutting down.", state)
			break
		}
	}

	// Allow a few extra seconds for any in-flight pubsub messages to arrive.
	log.Print("Waiting 5 seconds for trailing events...")
	time.Sleep(5 * time.Second)
	cancelReceive()

	// Drain the receiver goroutine.
	if err := <-receiveErr; err != nil && receiveCtx.Err() == nil {
		log.Printf("Pubsub receiver error: %v", err)
	}
	log.Print("Pubsub receiver stopped.")

	// Step 5: Delete the subscription.
	log.Printf("Deleting subscription %q...", subName)
	if err := sub.Delete(ctx); err != nil {
		log.Printf("Warning: failed to delete subscription: %v", err)
	} else {
		log.Printf("Subscription %q deleted.", subName)
	}

	log.Print("Done.")
}
