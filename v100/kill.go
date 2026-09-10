package v100

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"github.com/urfave/cli"
	"google.golang.org/api/iterator"
)

func runKill(c *cli.Context) error {
	jobID := c.Args().First()
	if jobID == "" {
		return fmt.Errorf("usage: sparkles kill <job-id>")
	}
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")
	noWait := c.Bool("no-wait")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := KillJob(ctx, project, db, jobID); err != nil {
		return err
	}

	if noWait {
		return nil
	}

	// Poll until all tasks reach a terminal state.
	var fsClient *firestore.Client
	var err error
	if db != "" {
		fsClient, err = firestore.NewClientWithDatabase(ctx, project, db)
	} else {
		fsClient, err = firestore.NewClient(ctx, project)
	}
	if err != nil {
		return fmt.Errorf("creating firestore client for polling: %w", err)
	}
	defer fsClient.Close()

	return waitForJobTerminal(ctx, fsClient, jobID)
}

// KillJob marks all pending tasks for jobID as killed and sends a kill_job
// signal to workers so they cancel any in-flight tasks. Exported for use by
// tests and tooling. GCS/Firestore/PubSub emulator env vars are honoured.
func KillJob(ctx context.Context, project, db, jobID string) error {
	var fsClient *firestore.Client
	var err error
	if db != "" {
		fsClient, err = firestore.NewClientWithDatabase(ctx, project, db)
	} else {
		fsClient, err = firestore.NewClient(ctx, project)
	}
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer psClient.Close()

	_, err = KillJobWithClients(ctx, fsClient, psClient, jobID)
	return err
}

// KillJobWithClients marks all pending tasks for jobID as killed and sends a
// kill_job signal to workers so they cancel any in-flight tasks, using the
// given (already-created) Firestore/PubSub clients rather than opening new
// ones. Returns the number of pending tasks that were killed synchronously;
// claimed/running/writing tasks are killed best-effort via the kill_job
// broadcast and converge to killed asynchronously as workers observe it.
// Exported so both the CLI (KillJob) and the dashboard API's cancel-job
// endpoint can reuse this without duplicating the logic.
func KillJobWithClients(ctx context.Context, fsClient *firestore.Client, psClient *pubsub.Client, jobID string) (int, error) {
	queue := NewFirestoreTaskQueue(fsClient, newNoopEventPublisher())

	// Step 1: mark all pending tasks for this job as killed.
	iter := fsClient.Collection(taskCollection).
		Where("job_id", "==", jobID).
		Where("status", "==", StatusPending).
		Documents(ctx)
	defer iter.Stop()

	killedCount := 0
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return killedCount, fmt.Errorf("querying pending tasks: %w", err)
		}
		var t Task
		if err := doc.DataTo(&t); err != nil {
			return killedCount, fmt.Errorf("reading task: %w", err)
		}
		if err := queue.RecordKilled(ctx, t.TaskID, true); err != nil {
			log.Printf("skipping task %s: %v", t.TaskID, err)
			continue
		}
		killedCount++
	}
	log.Printf("Marked %d pending task(s) as killed", killedCount)

	// Step 2: notify workers so they cancel any running tasks for this job.
	msg := workerControlMessage{Type: "kill_job", JobID: jobID}
	data, err := json.Marshal(msg)
	if err != nil {
		return killedCount, fmt.Errorf("marshalling kill_job message: %w", err)
	}
	if _, err := psClient.Publisher(workerInTopic).Publish(ctx, &pubsub.Message{Data: data}).Get(ctx); err != nil {
		return killedCount, fmt.Errorf("publishing kill_job message: %w", err)
	}
	log.Printf("Sent kill_job signal to workers")
	return killedCount, nil
}

// waitForJobTerminal polls Firestore every 2 s until all tasks for jobID reach
// a terminal state (success/error/failed/killed) or ctx is cancelled.
func waitForJobTerminal(ctx context.Context, fsClient *firestore.Client, jobID string) error {
	terminalStatuses := map[string]bool{
		StatusSuccess: true,
		StatusError:   true,
		StatusFailed:  true,
		StatusKilled:  true,
	}

	log.Printf("Waiting for all tasks to reach a terminal state...")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}

		docs, err := fsClient.Collection(taskCollection).
			Where("job_id", "==", jobID).
			Documents(ctx).
			GetAll()
		if err != nil {
			return fmt.Errorf("polling task status: %w", err)
		}

		nonTerminal := 0
		for _, doc := range docs {
			var t Task
			if err := doc.DataTo(&t); err != nil {
				continue
			}
			if !terminalStatuses[t.Status] {
				nonTerminal++
			}
		}

		log.Printf("Non-terminal tasks remaining: %d", nonTerminal)
		if nonTerminal == 0 {
			break
		}
	}

	log.Printf("All tasks are done.")
	return nil
}

func newNoopEventPublisher() *EventPublisher {
	return NewEventPublisher(nil, nil)
}
