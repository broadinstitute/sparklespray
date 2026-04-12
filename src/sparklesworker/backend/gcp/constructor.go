package gcp

import (
	"context"
	"fmt"
	"log"
	"strings"

	batch "cloud.google.com/go/batch/apiv1"
	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/backend"
	"google.golang.org/api/iterator"
)

func CreateGCPServices(ctx context.Context, projectID string, region string, database string) (*backend.ExternalServices, error) {
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

	batchClient, err := batch.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating batch client: %w", err)
	}

	zoneClient, err := compute.NewZonesRESTClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("compute.NewZonesRESTClient: %w", err)
	}
	defer zoneClient.Close()

	zones, err := getZonesForRegion(ctx, zoneClient, projectID, region)
	if err != nil {
		return nil, fmt.Errorf("listing zones in region %s: %w", region, err)
	}

	compute := &GCPWorkerPool{
		region:          region,
		projectID:       projectID,
		ctx:             ctx,
		instancesClient: instancesClient,
		batchClient:     batchClient,
		zones:           zones,
	}

	log.Printf("Created GCPWorkerPool for projectID=%s, region=%s, zones=[%s]", projectID, region, strings.Join(zones, ", "))

	cluster := &FirestoreClusterStore{client: firestoreClient, ctx: ctx}
	tasks := newFirestoreTaskStoreGlobal(ctx, firestoreClient)
	taskCache := NewFirestoreTaskCache(firestoreClient)

	return &backend.ExternalServices{
		Channel:   channel,
		TaskCache: taskCache,
		Close: func() {
			firestoreClient.Close()
			instancesClient.Close()
			batchClient.Close()
		},
		Compute: compute,
		Cluster: cluster,
		Tasks:   tasks,
		CreateWorkerCommand: func(cluster *backend.Cluster, shouldLinger bool) []string {
			return backend.CreateWorkerCommand(cluster, shouldLinger, []string{
				"--projectId", projectID,
				"--database", database})
		},
		CreateEventPublisher: func(topic string) backend.EventPublisher {
			return NewFirestoreEventPublisher(firestoreClient, channel, topic)
		},
	}, nil
}

func getZonesForRegion(ctx context.Context, zoneClient *compute.ZonesClient, projectID string, region string) ([]string, error) {
	filter := fmt.Sprintf("region eq .*/regions/%s", region)
	req := &computepb.ListZonesRequest{
		Project: projectID,
		Filter:  &filter,
	}

	var zones []string
	it := zoneClient.List(ctx, req)
	for {
		zone, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iterating zones: %w", err)
		}
		zones = append(zones, zone.GetName())
	}

	return zones, nil
}
