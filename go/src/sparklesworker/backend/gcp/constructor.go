package gcp

import (
	"context"
	"fmt"
	"log"

	batch "cloud.google.com/go/batch/apiv1"
	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/ext_channel"
	"github.com/broadinstitute/sparklesworker/task_queue"
)

func CreateGCPServices(ctx context.Context, projectID string, database string, clusterID string) (*backend.ExternalServices, error) {
	log.Printf("Using Firestore backend (project=%s)", projectID)
	firestoreClient, err := firestore.NewClientWithDatabase(ctx, projectID, database)
	if err != nil {
		return nil, fmt.Errorf("creating firestore client: %w", err)
	}
	channel := ext_channel.NewPubSubChannel(projectID)
	queue := task_queue.NewFirestoreQueue(firestoreClient, clusterID, "", 0, 0)
	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating compute client: %w", err)
	}
	defer instancesClient.Close()

	batchClient, err := batch.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating batch client: %w", err)
	}
	defer batchClient.Close()

	gshim := &GCPMethodsForPoll{
		projectID:       projectID,
		ctx:             ctx,
		instancesClient: instancesClient,
		batchClient:     batchClient,
	}

	sshim := &FirestoreSparklesMethodsForPoll{client: firestoreClient, ctx: ctx}

	return &backend.ExternalServices{Channel: channel, Queue: queue, Close: func() {
		firestoreClient.Close()
	},
		Gshim: gshim, Sshim: sshim}, nil
}
