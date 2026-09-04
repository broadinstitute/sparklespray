package dev

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
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/broadinstitute/sparklespray/v100/scheduler"
	"github.com/urfave/cli"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// newMonitor wires up a Monitor against already-created Firestore and Pub/Sub
// clients, so a caller can run the monitor alongside other components in one
// process (see runServe) or on its own (see runDevMonitor).
//
// The returned stop function must be called when the monitor is done; it
// flushes the event publisher. The caller owns fsClient/psClient and must not
// close them until after the monitor loop has returned.
func newMonitor(
	ctx context.Context,
	project, db string,
	fsClient *firestore.Client,
	psClient *pubsub.Client,
	verbose bool,
	linger time.Duration,
) (*monitor.Monitor, func(), error) {
	pools := monitor.NewFirestoreWorkPoolStore(fsClient)
	batches := monitor.NewFirestoreBatchRequestStore(fsClient)
	workers := monitor.NewFirestoreWorkerStore(fsClient)
	tasks := monitor.NewFirestoreTaskStore(fsClient, nil)

	var batchAPI monitor.BatchAPIClient
	if emulatorURL := os.Getenv("SPARKLES_BATCH_API_EMULATOR"); emulatorURL != "" {
		batchAPI = monitor.NewRemoteBatchAPIClient(emulatorURL)
	} else {
		var err error
		batchAPI, err = monitor.NewGCPBatchAPIClient(ctx, project)
		if err != nil {
			return nil, nil, fmt.Errorf("creating batch API client: %w", err)
		}
	}

	pubsubReceiver, err := monitor.NewGCPPubSubReceiver(ctx, project, batches)
	if err != nil {
		return nil, nil, fmt.Errorf("creating pubsub receiver: %w", err)
	}

	jobEventReceiver, err := monitor.NewGCPJobEventReceiver(ctx, project)
	if err != nil {
		return nil, nil, fmt.Errorf("creating job event receiver: %w", err)
	}

	topicName := fmt.Sprintf("projects/%s/topics/sparkles-events", project)
	if _, err := psClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName}); err != nil {
		if grpcstatus.Code(err) != codes.AlreadyExists {
			return nil, nil, fmt.Errorf("ensuring sparkles-events topic: %w", err)
		}
	}

	ep := v100.NewEventPublisher(psClient.Publisher("sparkles-events"), fsClient)
	tasks.SetPublisher(ep)

	jobSummaries := monitor.NewFirestoreJobSummaryStore(fsClient)
	workPoolSummaries := monitor.NewFirestoreWorkPoolSummaryStore(fsClient)
	eventStore := monitor.NewFirestoreEventStore(fsClient)

	m := monitor.New(scheduler.RealClock, batchAPI, pools, batches, workers, tasks, pubsubReceiver, db)
	m.SetVerbose(verbose)
	m.SetJobEventReceiver(jobEventReceiver)
	m.SetJobSummaryStore(jobSummaries)
	m.SetWorkPoolSummaryStore(workPoolSummaries)
	m.SetEventStore(eventStore)
	m.SetJobTerminatedPublisher(ep)
	m.SetWorkpoolStatePublisher(ep)
	m.SetBatchOutcomePublisher(ep)
	m.SetWorkpoolIncidentPublisher(ep)
	m.SetWorkerEventPublisher(ep)
	if linger > 0 {
		m.SetLingerDuration(linger)
	}

	return m, ep.Stop, nil
}

func runDevMonitor(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("Connecting to project %s, database %s", project, db)
	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer psClient.Close()

	m, stopMonitor, err := newMonitor(ctx, project, db, fsClient, psClient,
		c.Bool("verbose"), time.Duration(c.Int("linger"))*time.Minute)
	if err != nil {
		return err
	}
	defer stopMonitor()

	m.RunMonitorLoop(ctx)
	return nil
}
