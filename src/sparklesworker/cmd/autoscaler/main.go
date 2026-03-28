package autoscaler

import (
	"context"
	"fmt"
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
		cli.DurationFlag{Name: "poll-interval", Usage: "Delay between poll cycles", Value: 5 * time.Second},
		cli.StringFlag{Name: "redis", Usage: "Redis address for local testing (e.g. localhost:6379); uses GCP Batch API when not set"},
		cli.StringFlag{Name: "database", Usage: "Firestore database ID (defaults to the project's default database)", Value: "(default)"},
		cli.DurationFlag{Name: "max-idle", Usage: "Stop polling after no active clusters for this long (defaults to 0s)", Value: 0},
	},
	Action: run,
}

func run(c *cli.Context) error {
	projectID := c.String("project")
	pollInterval := c.Duration("poll-interval")
	redisAddr := c.String("redis")
	database := c.String("database")
	maxIdle := c.Duration("max-idle")

	fmt.Printf("autoscaler\n")
	fmt.Printf("  project: %s\n", projectID)
	fmt.Printf("  poll-interval: %s\n", pollInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var extServices *backend.ExternalServices
	var err error
	if redisAddr != "" {
		fmt.Printf("  mode: redis (%s)\n", redisAddr)
		extServices, err = redis_backend.CreateMockServices(ctx, redisAddr, true)

		if err != nil {
			return fmt.Errorf("Creating redis backed services failed: %s", err)
		}

	} else {
		fmt.Printf("  mode: GCP\n")
		extServices, err = gcp_backend.CreateGCPServices(ctx, projectID, database)
		if err != nil {
			return fmt.Errorf("Creating gcp backed services failed: %s", err)
		}
	}

	defer extServices.Close()

	idleSince := time.Now()
	for {
		clusterIDs, err := extServices.Sshim.GetActiveClusterIDs()
		if err != nil {
			return fmt.Errorf("Couldn't find active clusters: %s", err)
		}

		if len(clusterIDs) == 0 {
			if time.Since(idleSince) > maxIdle {
				break
			}
		} else {
			idleSince = time.Time{}
		}

		for _, clusterID := range clusterIDs {
			if err := Poll(clusterID, extServices.Gshim, extServices.Sshim, extServices.SparklesWorkerArgs); err != nil {
				return fmt.Errorf("poll (clusterID=%s) error: %v\n", clusterID, err)
			}
		}

		// go to sleep after we've polled all the clusters
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(pollInterval):
		}
	}

	return nil
}
