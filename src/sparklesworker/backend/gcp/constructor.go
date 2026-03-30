package gcp

import (
	"context"
	"fmt"
	"log"

	batch "cloud.google.com/go/batch/apiv1"
	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/backend"
)

func CreateGCPServices(ctx context.Context, projectID string, database string) (*backend.ExternalServices, error) {
	log.Printf("Using Firestore backend (project=%s)", projectID)
	firestoreClient, err := firestore.NewClientWithDatabase(ctx, projectID, database)
	if err != nil {
		return nil, fmt.Errorf("creating firestore client: %w", err)
	}
	channel := NewPubSubChannel(projectID)
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

	compute := &GCPWorkerPool{
		projectID:       projectID,
		ctx:             ctx,
		instancesClient: instancesClient,
		batchClient:     batchClient,
	}

	cluster := &FirestoreClusterStore{client: firestoreClient, ctx: ctx}
	tasks := newFirestoreTaskStoreGlobal(ctx, firestoreClient)
	taskCache := NewFirestoreTaskCache(firestoreClient)

	return &backend.ExternalServices{
		Channel: channel,
		TaskCache: taskCache,
		Close:     func() { firestoreClient.Close() },
		Compute:   compute,
		Cluster:   cluster,
		Tasks:     tasks,
		CreateWorkerCommand: func(clusterID string, shouldLinger bool, aetherConfig *backend.AetherConfig) []string {
			return backend.CreateWorkerCommand(clusterID, shouldLinger, []string{
				"--projectId", projectID,
				"--database", database}, aetherConfig)
		}}, nil
}
