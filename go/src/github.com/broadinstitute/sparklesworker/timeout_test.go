package sparklesworker

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2/google"

	compute "google.golang.org/api/compute/v1"
)

func TestInstances(t *testing.T) {
	ctx := context.Background()
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/compute.readonly")
	assert.Nil(t, err)
	service, err := compute.New(client)
	assert.Nil(t, err)

	cluster := "x"
	zones := []string{"a", "b"}
	projectID := "broad-achilles"
	owner := "owner"
	Timeout := time.Minute
	ReservationSize := 1
	ReservationTimeout := time.Minute

	timeout := NewClusterTimeout(service, cluster, zones, projectID, owner, Timeout,
		ReservationSize, ReservationTimeout)

	timeout.Reset(time.Now())
}
