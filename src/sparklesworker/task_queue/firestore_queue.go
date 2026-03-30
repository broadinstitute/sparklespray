// Compatibility shim — constructors and types delegating to backend/gcp.
package task_queue

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
	gcp_backend "github.com/broadinstitute/sparklesworker/backend/gcp"
)

// FirestoreQueue is an alias for the implementation in backend/gcp.
type FirestoreQueue = gcp_backend.FirestoreTaskStore

// NewFirestoreQueue creates a per-cluster FirestoreTaskStore for worker use.
func NewFirestoreQueue(client *firestore.Client, cluster string, workerID string, initialClaimRetry time.Duration, claimTimeout time.Duration) *FirestoreQueue {
	return gcp_backend.NewFirestoreTaskStore(client, workerID, initialClaimRetry, claimTimeout)
}

// Cluster holds the Pub/Sub topic names for a cluster (compatibility alias).
type Cluster = gcp_backend.ClusterTopics

// GetCluster fetches cluster Pub/Sub topic configuration from Firestore.
func GetCluster(ctx context.Context, client *firestore.Client, clusterID string) (*Cluster, error) {
	return gcp_backend.GetCluster(ctx, client, clusterID)
}

// FirestoreTaskCache is an alias for the implementation in backend/gcp.
type FirestoreTaskCache = gcp_backend.FirestoreTaskCache

// NewFirestoreTaskCache creates a new FirestoreTaskCache.
func NewFirestoreTaskCache(client *firestore.Client) *FirestoreTaskCache {
	return gcp_backend.NewFirestoreTaskCache(client)
}
