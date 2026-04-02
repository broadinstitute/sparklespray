package gcp

import (
	"context"
	"testing"

	batch "cloud.google.com/go/batch/apiv1"
)

func TestFetchDebugInfo(t *testing.T) {
	ctx := context.Background()

	batchClient, err := batch.NewClient(ctx)
	if err != nil {
		t.Fatalf("creating batch client: %v", err)
	}
	defer batchClient.Close()

	infos, err := FetchDebugInfo(ctx, batchClient, "test-sparkles-2", "us-central1", "c-231719e6ca246bd8")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	PrintDebugInfos(infos)
}
