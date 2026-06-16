package v100

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/urfave/cli"
)

const heartbeatPeriod = 1 * time.Minute

const workerOutTopic = "sparkles-events"
const workerInTopic = "sparkles-worker-in"
const workerCollection = "Workers"

type WorkerRecord struct {
	WorkerID        string    `firestore:"worker_id"`
	WorkpoolID      string    `firestore:"workpool_id"`
	Status          string    `firestore:"status"`
	Expiry          time.Time `firestore:"expiry"`
	HeartbeatExpiry time.Time `firestore:"heartbeat_expiry"`
}

func Main() error {
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
				cli.StringFlag{Name: "db"},
				cli.StringFlag{Name: "workpool"},
				cli.StringFlag{Name: "resources"},
			},
			Action: runWorker,
		},
	}

	return app.Run(os.Args)
}

func runHeartbeat(ctx context.Context, doc *firestore.DocumentRef) {
	ticker := time.NewTicker(heartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := doc.Update(ctx, []firestore.Update{
				{Path: "heartbeat_expiry", Value: time.Now().Add(heartbeatPeriod)},
			})
			if err != nil {
				log.Printf("heartbeat update failed: %v", err)
			} else {
				log.Printf("heartbeat updated")
			}
		}
	}
}

func parseResources(s string) (*Resources, error) {
	r := NewResources()
	if s == "" {
		s = "slots=1"
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid resource %q: expected name=value", part)
		}
		v, err := strconv.ParseFloat(kv[1], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid resource value %q: %w", kv[1], err)
		}
		r.Set(kv[0], v)
	}
	return &r, nil
}

func runWorker(c *cli.Context) error {
	project := c.String("project")
	db := c.String("db")
	workpoolID := c.String("workpool")

	if project == "" {
		return fmt.Errorf("--project is required")
	}
	if workpoolID == "" {
		return fmt.Errorf("--workpool is required")
	}

	resources, err := parseResources(c.String("resources"))
	if err != nil {
		return fmt.Errorf("--resources: %w", err)
	}

	workerID := uuid.New().String()
	log.Printf("Starting worker %s in workpool %s", workerID, workpoolID)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	ws, err := startWorker(ctx, project, db, workerID, workpoolID)
	if err != nil {
		return err
	}
	defer ws.cleanup()

	// Main poll loop
	if err := ws.mainLoop(ctx, resources); err != nil {
		panic(fmt.Sprintf("Internal error in mainLoop: %v", err))
	}

	log.Printf("Shutting down worker %s", workerID)
	cancel()

	ws.shutdown()

	log.Printf("Worker %s stopped", ws.workerID)
	return nil
}

type taskCompletion struct {
	taskID    string
	resources Resources
	err       error
}

func executeTask(task *Task, resources Resources, completions chan<- taskCompletion) {
	log.Printf("Stub: executing: %v", task)
	go func() {
		completions <- taskCompletion{taskID: task.TaskID, resources: resources}
	}()
}

func (ws *workerState) mainLoop(ctx context.Context, resources *Resources) error {
	queue := NewTaskQueue(ws.fsClient, ws.publisher)
	completions := make(chan taskCompletion, 100)
	runningCount := 0
	curResources := resources

	// curResources is the single-goroutine mutable running total of available capacity.
	// Only waitForCompletion and the claim path touch it, both on this goroutine.
	waitForCompletion := func() error {
		select {
		case c := <-completions:
			curResources = curResources.Add(c.resources)
			runningCount--
			if c.err != nil {
				log.Printf("task %s failed: %v", c.taskID, c.err)
				if err := queue.RecordFailed(ctx, c.taskID, c.err.Error(), StatusClaimed); err != nil {
					log.Printf("recording task %s as failed: %v", c.taskID, err)
				}
			} else {
				if err := queue.RecordSuccess(ctx, c.taskID); err != nil {
					log.Printf("recording task %s as success: %v", c.taskID, err)
				}
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// loop until we're out of jobs with tasks
	for {
		// first, find a job which has at least one task
		pendingTask, err := queue.GetFirstPendingTask(ctx, ws.workpoolID)
		if err != nil {
			return fmt.Errorf("getting first pending task: %w", err)
		}
		if pendingTask == nil {
			break
		}

		job, err := queue.GetJob(ctx, pendingTask.JobID)
		if err != nil {
			return fmt.Errorf("getting job %s: %w", pendingTask.JobID, err)
		}

		jobResources := NewResources()
		for _, entry := range job.Resources {
			jobResources.Set(entry.Name, entry.Value)
		}

		// check to make sure that this job can run at least one task if we used our full allocation of resources
		if !resources.Sub(jobResources).IsValid() {
			log.Printf("Job %s requires more resources than this worker can provide; failing all pending tasks", job.JobID)
			for {
				task, err := queue.ClaimTask(ctx, job.JobID, ws.workerID)
				if err != nil {
					return fmt.Errorf("claiming task to fail for job %s: %w", job.JobID, err)
				}
				if task == nil {
					break
				}
				if err := queue.RecordFailed(ctx, task.TaskID, "Task requires more resources than allowed by worker pool", StatusClaimed); err != nil {
					return fmt.Errorf("recording task %s as failed: %w", task.TaskID, err)
				}
			}
			// upon reaching here, all the tasks associated with the jobID are no longer marked pending,
			// so when we call queue.GetFirstPendingTask, we're guarenteed to get a different job.
			continue
		}

		for {
			// drain any completions that have already arrived
			for len(completions) > 0 {
				if err := waitForCompletion(); err != nil {
					return err
				}
			}

			remaining := curResources.Sub(jobResources)
			if !remaining.IsValid() {
				// wait for a running task to free up resources
				if err := waitForCompletion(); err != nil {
					return err
				}
				continue
			}

			task, err := queue.ClaimTask(ctx, job.JobID, ws.workerID)
			if err != nil {
				return fmt.Errorf("claiming task: %w", err)
			}
			if task == nil {
				// job's tasks were exhausted, so we're done with this job
				break
			}

			curResources = remaining
			runningCount++
			executeTask(task, jobResources, completions)
		}
	}

	// drain all remaining running tasks
	for runningCount > 0 {
		if err := waitForCompletion(); err != nil {
			return err
		}
	}

	return nil
}

type workerState struct {
	fsClient  *firestore.Client
	psClient  *pubsub.Client
	publisher *EventPublisher
	workerDoc *firestore.DocumentRef
	sub       *pubsub.Subscription
	subName   string
	workerID  string
	workpoolID string
}

func (ws *workerState) cleanup() {
	ws.publisher.Stop()
	ws.psClient.Close()
	ws.fsClient.Close()
}

func startWorker(ctx context.Context, project, db, workerID, workpoolID string) (*workerState, error) {
	var fsClient *firestore.Client
	var err error
	if db != "" {
		fsClient, err = firestore.NewClientWithDatabase(ctx, project, db)
	} else {
		fsClient, err = firestore.NewClient(ctx, project)
	}
	if err != nil {
		return nil, fmt.Errorf("creating firestore client: %w", err)
	}

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		fsClient.Close()
		return nil, fmt.Errorf("creating pubsub client: %w", err)
	}

	now := time.Now()
	workerDoc := fsClient.Collection(workerCollection).Doc(workerID)
	_, err = workerDoc.Set(ctx, WorkerRecord{
		WorkerID:        workerID,
		WorkpoolID:      workpoolID,
		Status:          "started",
		Expiry:          now.Add(7 * 24 * time.Hour),
		HeartbeatExpiry: now.Add(heartbeatPeriod),
	})
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("registering worker in firestore: %w", err)
	}
	log.Printf("Registered worker %s in Firestore", workerID)

	publisher := NewEventPublisher(psClient.Topic(workerOutTopic), fsClient)

	if err := publisher.PublishWorkerEvent(ctx, WorkerEvent{
		Type:       "worker_started",
		WorkerID:   workerID,
		WorkpoolID: workpoolID,
	}); err != nil {
		publisher.Stop()
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("publishing worker_started: %w", err)
	}
	log.Printf("Published worker_started event")

	subName := fmt.Sprintf("%s-%s", workerInTopic, workerID)
	inTopic := psClient.Topic(workerInTopic)
	sub, err := psClient.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
		Topic: inTopic,
	})
	inTopic.Stop()
	if err != nil {
		publisher.Stop()
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("creating subscription %s: %w", subName, err)
	}
	log.Printf("Created subscription %s", subName)

	go runHeartbeat(ctx, workerDoc)

	go func() {
		recvErr := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			log.Printf("Received message on %s: %s", workerInTopic, string(msg.Data))
			msg.Ack()
		})
		if recvErr != nil && ctx.Err() == nil {
			log.Printf("subscription receive error: %v", recvErr)
		}
	}()

	return &workerState{
		fsClient:   fsClient,
		psClient:   psClient,
		publisher:  publisher,
		workerDoc:  workerDoc,
		sub:        sub,
		subName:    subName,
		workerID:   workerID,
		workpoolID: workpoolID,
	}, nil
}

func (ws *workerState) shutdown() {
	// Use a fresh context for shutdown operations since the main context is cancelled.
	ctx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	shutdownNow := time.Now()
	_, err := ws.workerDoc.Update(ctx, []firestore.Update{
		{Path: "expiry", Value: shutdownNow},
		{Path: "heartbeat_expiry", Value: shutdownNow},
		{Path: "status", Value: "stopped"},
	})
	if err != nil {
		log.Printf("Failed to update worker record on shutdown: %v", err)
	}

	if err := ws.publisher.PublishWorkerEvent(ctx, WorkerEvent{
		Type:       "worker_stopped",
		WorkerID:   ws.workerID,
		WorkpoolID: ws.workpoolID,
	}); err != nil {
		log.Printf("Failed to publish worker_stopped: %v", err)
	}

	if err := ws.sub.Delete(ctx); err != nil {
		log.Printf("Failed to delete subscription %s: %v", ws.subName, err)
	}
}
