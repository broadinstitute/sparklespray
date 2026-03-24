package autoscaler

import (
	"context"
	"fmt"
	"os"
	"time"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/firestore"
	compute "cloud.google.com/go/compute/apiv1"
	"github.com/urfave/cli"
)

const MonitorVersion = "0.1.0"

var AutoscalerCmd = cli.Command{
	Name:  "autoscaler",
	Usage: "Create instances of worker based on cluster parameters",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "project", Usage: "Google Cloud project ID"},
		cli.StringFlag{Name: "cluster", Usage: "Cluster ID to monitor"},
		cli.DurationFlag{Name: "poll-interval", Usage: "Delay between poll cycles", Value: 5 * time.Second},
	},
	Action: run,
}

func Main() error {
	app := cli.NewApp()
	app.Name = "sparkles-monitor"
	app.Version = MonitorVersion
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{
			Name:  "Philip Montgomery",
			Email: "pmontgom@broadinstitute.org",
		},
	}
	app.Usage = "Monitor and manage Sparklespray clusters"

	app.Flags = []cli.Flag{}

	app.Action = run

	return app.Run(os.Args)
}

func run(c *cli.Context) error {
	project := c.String("project")
	cluster := c.String("cluster")
	pollInterval := c.Duration("poll-interval")

	fmt.Printf("sparkles-monitor\n")
	fmt.Printf("  project: %s\n", project)
	fmt.Printf("  cluster: %s\n", cluster)
	fmt.Printf("  poll-interval: %s\n", pollInterval)

	ctx := context.Background()

	firestoreClient, err := firestore.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer firestoreClient.Close()

	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return fmt.Errorf("creating compute client: %w", err)
	}
	defer instancesClient.Close()

	batchClient, err := batch.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("creating batch client: %w", err)
	}
	defer batchClient.Close()

	gshim := &GCPMethodsForPoll{
		projectID:       project,
		ctx:             ctx,
		instancesClient: instancesClient,
		batchClient:     batchClient,
	}
	sshim := &FirestoreSparklesMethodsForPoll{client: firestoreClient, ctx: ctx}

	for {
		if err := Poll(cluster, gshim, sshim); err != nil {
			fmt.Fprintf(os.Stderr, "poll error: %v\n", err)
		}
		time.Sleep(pollInterval)
	}
}
