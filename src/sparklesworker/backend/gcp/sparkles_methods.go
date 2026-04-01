package gcp

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/backend"
)

// FirestoreClusterStore implements backend.ClusterStore using Google Cloud Firestore.
type FirestoreClusterStore struct {
	client *firestore.Client
	ctx    context.Context
}

func NewFirestoreClusterStore(ctx context.Context, client *firestore.Client) *FirestoreClusterStore {
	return &FirestoreClusterStore{client: client, ctx: ctx}
}

func (s *FirestoreClusterStore) GetClusterConfig(clusterID string) (*backend.Cluster, error) {
	docSnap, err := s.client.Collection(backend.ClusterCollection).Doc(clusterID).Get(s.ctx)
	if err != nil {
		return nil, fmt.Errorf("getting cluster %s: %w", clusterID, err)
	}
	var cluster backend.Cluster
	if err := docSnap.DataTo(&cluster); err != nil {
		return nil, fmt.Errorf("decoding cluster %s: %w", clusterID, err)
	}
	return &cluster, nil
}

func (s *FirestoreClusterStore) SetClusterConfig(clusterID string, cluster backend.Cluster) error {
	_, err := s.client.Collection(backend.ClusterCollection).Doc(clusterID).Set(s.ctx, cluster)
	return err
}

func (s *FirestoreClusterStore) UpdateClusterMonitorState(clusterID string, state *backend.MonitorState) error {
	wire := backend.MonitorStateJSON{
		BatchJobRequests:        state.BatchJobRequests,
		CompletedJobIds:         state.CompletedJobIds,
		SuspiciouslyFailedToRun: state.SuspiciouslyFailedToRun,
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("marshaling monitor state: %w", err)
	}
	_, err = s.client.Collection(backend.ClusterCollection).Doc(clusterID).Update(s.ctx, []firestore.Update{
		{Path: "monitor_state", Value: string(data)},
	})
	return err
}
