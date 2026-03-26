package autoscaler

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	gcp_backend "github.com/broadinstitute/sparklesworker/backend/gcp"
	redis_backend "github.com/broadinstitute/sparklesworker/backend/redis"
	"github.com/urfave/cli"
)

var AutoscalerCmd = cli.Command{
	Name:  "autoscaler",
	Usage: "Create instances of worker based on cluster parameters",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "project", Usage: "Google Cloud project ID"},
		cli.StringFlag{Name: "cluster", Usage: "Cluster ID to monitor"},
		cli.DurationFlag{Name: "poll-interval", Usage: "Delay between poll cycles", Value: 5 * time.Second},
		cli.StringFlag{Name: "redis", Usage: "Redis address for local testing (e.g. localhost:6379); uses GCP Batch API when not set"},
		cli.StringFlag{Name: "database", Usage: "Firestore database ID (defaults to the project's default database)", Value: "(default)"},
	},
	Action: run,
}

func run(c *cli.Context) error {
	projectID := c.String("project")
	clusterID := c.String("cluster")
	pollInterval := c.Duration("poll-interval")
	redisAddr := c.String("redis")
	database := c.String("database")

	fmt.Printf("sparkles-monitor\n")
	fmt.Printf("  project: %s\n", projectID)
	fmt.Printf("  cluster: %s\n", clusterID)
	fmt.Printf("  poll-interval: %s\n", pollInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var extServices *backend.ExternalServices
	var err error
	if redisAddr != "" {
		fmt.Printf("  mode: redis (%s)\n", redisAddr)
		extServices, err = redis_backend.CreateMockServices(ctx, redisAddr, clusterID, pollInterval)

		if err != nil {
			return fmt.Errorf("Creating redis backed services failed: %s", err)
		}
	} else {
		fmt.Printf("  mode: GCP\n")
		extServices, err = gcp_backend.CreateGCPServices(ctx, projectID, database, clusterID)
		if err != nil {
			return fmt.Errorf("Creating gcp backed services failed: %s", err)
		}
	}

	defer extServices.Close()

	for {
		if err := Poll(clusterID, extServices.Gshim, extServices.Sshim); err != nil {
			fmt.Fprintf(os.Stderr, "poll error: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(pollInterval):
		}
	}
}
