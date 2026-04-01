package autoscaler

import (
	"context"
	"fmt"
	"log"
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
		cli.StringFlag{Name: "region", Usage: "Google Cloud region"},
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
	region := c.String("region")
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
		extServices, err = gcp_backend.CreateGCPServices(ctx, projectID, region, database)
		if err != nil {
			return fmt.Errorf("Creating gcp backed services failed: %s", err)
		}
	}

	defer extServices.Close()

	firstTimeSeenCluster := make(map[string]bool)

	idleSince := time.Now()
	for {
		clusterIDs, err := extServices.Tasks.GetClusterIDsFromActiveTasks()
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
			if _, seenClusterID := firstTimeSeenCluster[clusterID]; !seenClusterID {
				// if this is the first time we've polled this cluster ID, first clean up anything that might have been left behind
				// from a past run, before this autoscaler started
				firstTimeSeenCluster[clusterID] = false
				if err := cleanupBatchJobs(extServices, clusterID); err != nil {
					return fmt.Errorf("Could not clean up cluster %q: %v\n", clusterID, err)
				}
			}

			if err := Poll(clusterID, extServices.Compute, extServices.Cluster, extServices.Tasks, extServices.CreateWorkerCommand); err != nil {
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

	log.Printf("Autoscaler shutting down")
	// calling cancel to shut down anything else still running
	cancel()

	return nil
}

func cleanupBatchJobs(extServices *backend.ExternalServices, clusterID string) error {
	// delete any batch jobs which are complete so they don't confuse the health check
	cluster, err := extServices.Cluster.GetClusterConfig(clusterID)
	if err != nil {
		return fmt.Errorf("In cleanup, getting cluster error: %v\n", err)
	}

	batchJobs, err := extServices.Compute.ListBatchJobs(cluster.Region, clusterID)
	if err != nil {
		return fmt.Errorf("List Batch jobs error: %v\n", err)
	}
	jobsNeedingDeletion := make(map[string]string)
	for _, batchJob := range batchJobs {
		if batchJob.State == backend.Failed || batchJob.State == backend.Complete {
			jobsNeedingDeletion[batchJob.ID] = batchJob.ID
		}
	}

	if len(jobsNeedingDeletion) > 0 {
		log.Printf("Cleaning up %d batch jobs from previous run", len(jobsNeedingDeletion))
		for jobID := range jobsNeedingDeletion {
			err = extServices.DeleteBatchJob(jobID)
			if err != nil {
				return fmt.Errorf("Deletion of batch job %q failed: %s", jobID, err)
			}
		}
	}

	// now, the deletion takes some time, so poll until we see that the job is in fact gone
	sleepDuration := 1 * time.Second
	attempts := 0
	for {
		attempts += 1
		if attempts > 10 {
			return fmt.Errorf("Timeout waiting for deleted jobs to disappear")
		}
		time.Sleep(sleepDuration)
		sleepDuration = min(sleepDuration*2, 20*time.Second)

		batchJobs, err = extServices.Compute.ListBatchJobs(cluster.Region, clusterID)
		jobsDeleted := true
		for _, batchJob := range batchJobs {
			// if we see a job in jobsNeedingDeletion, then not everything we deleted has disappeared. Try again
			if _, inSet := jobsNeedingDeletion[batchJob.ID]; inSet {
				jobsDeleted = false
				break
			}
		}

		if jobsDeleted {
			break
		}
	}

	return nil
}
