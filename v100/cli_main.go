package v100

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/broadinstitute/sparklespray/v100/scheduler"
	"github.com/urfave/cli"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const defaultDB = "sparkles"

// NewApp creates and configures the sparkles CLI app with the worker and
// monitor commands. Dev commands are registered by cmd/sparkles/main.go to
// keep the v100 package free of a v100/dev import cycle.
func NewApp() *cli.App {
	app := cli.NewApp()
	app.Name = "sparkles"
	app.Version = "1.0.0"
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{
			Name:  "Philip Montgomery",
			Email: "pmontgom@broadinstitute.org",
		},
	}

	app.Commands = []cli.Command{
		{
			Name: "worker",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "project"},
				cli.StringFlag{Name: "db", Value: defaultDB},
				cli.StringFlag{Name: "workpool"},
				cli.StringFlag{Name: "resources"},
				cli.BoolFlag{Name: "no-gcp", Usage: "local development mode: skip GCP metadata server"},
			},
			Action: runWorker,
		},
		{
			Name:      "kill",
			ArgsUsage: "<job-id>",
			Usage:     "Kill all pending and running tasks for a job",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "project"},
				cli.StringFlag{Name: "db", Value: defaultDB},
				cli.BoolFlag{Name: "no-wait", Usage: "exit immediately after sending kill signal without polling for completion"},
			},
			Action: runKill,
		},
		{
			Name:  "monitor",
			Usage: "Start the monitor",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "project", Usage: "GCP project ID (required)"},
				cli.StringFlag{Name: "db", Value: defaultDB, Usage: "Firestore database"},
				cli.BoolFlag{Name: "verbose, v", Usage: "log a message at the start of every poll"},
			},
			Action: runMonitor,
		},
	}

	return app
}

// Main runs the complete sparkles CLI. Provided for backward compatibility;
// callers that need dev commands should use NewApp() and append them instead.
func Main() error {
	return NewApp().Run(os.Args)
}

func runMonitor(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	log.Printf("Connecting to project %s, database %s", project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	pools := monitor.NewFirestoreWorkPoolStore(fsClient)
	batches := monitor.NewFirestoreBatchRequestStore(fsClient)
	workers := monitor.NewFirestoreWorkerStore(fsClient)
	tasks := monitor.NewFirestoreTaskStore(fsClient)

	var batchAPI monitor.BatchAPIClient
	if emulatorURL := os.Getenv("SPARKLES_BATCH_API_EMULATOR"); emulatorURL != "" {
		batchAPI = monitor.NewRemoteBatchAPIClient(emulatorURL)
	} else {
		batchAPI, err = monitor.NewGCPBatchAPIClient(ctx, project)
		if err != nil {
			return fmt.Errorf("creating batch API client: %w", err)
		}
	}

	pubsubReceiver, err := monitor.NewGCPPubSubReceiver(ctx, project, batches)
	if err != nil {
		return fmt.Errorf("creating pubsub receiver: %w", err)
	}

	jobEventReceiver, err := monitor.NewGCPJobEventReceiver(ctx, project)
	if err != nil {
		return fmt.Errorf("creating job event receiver: %w", err)
	}

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer psClient.Close()

	topicName := fmt.Sprintf("projects/%s/topics/sparkles-events", project)
	if _, err := psClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName}); err != nil {
		if grpcstatus.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("ensuring sparkles-events topic: %w", err)
		}
	}

	ep := NewEventPublisher(psClient.Publisher("sparkles-events"), fsClient)
	defer ep.Stop()

	jobSummaries := monitor.NewFirestoreJobSummaryStore(fsClient)

	m := monitor.New(scheduler.RealClock, batchAPI, pools, batches, workers, tasks, pubsubReceiver)
	m.SetVerbose(c.Bool("verbose"))
	m.SetJobEventReceiver(jobEventReceiver)
	m.SetJobSummaryStore(jobSummaries)
	m.SetJobTerminatedPublisher(ep)
	m.RunMonitorLoop(ctx)
	return nil
}
