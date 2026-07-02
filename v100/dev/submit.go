package dev

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/google/uuid"
	"github.com/urfave/cli"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// JobSpec is the JSON schema for the job spec file passed to "dev submit".
type JobSpec struct {
	Name            string                `json:"name"`
	Resources       []v100.ResourceEntry  `json:"resources"`
	FilesToLocalize []v100.FileToLocalize `json:"filesToLocalize"`
	Labels          []v100.Label          `json:"labels"`
	Tasks           []JobSpecTask         `json:"tasks"`
}

// JobSpecTask describes a single task within a JobSpec.
type JobSpecTask struct {
	Command     []string `json:"command"`
	DockerImage string   `json:"dockerImage"`
}

// WorkpoolSpec is the JSON schema for the workpool spec file passed to "dev submit".
type WorkpoolSpec struct {
	ID                    string               `json:"id"`
	MachineType           string               `json:"machineType"`
	RootDir               string               `json:"rootDir"`
	SparklesWorkerGCSPath string               `json:"sparklesWorkerGCSPath"`
	Resources             []v100.ResourceEntry `json:"resources"`
	EmptyVolumes          []v100.EmptyVolume   `json:"emptyVolumes"`
	Region                string               `json:"region"`
	Zones                 []string             `json:"zones"`
	ServiceAccount        string               `json:"serviceAccount"`

	MaxWorkerCount               int `json:"maxWorkerCount"`
	MaxPreemptibleWorkerAttempts int `json:"maxPreemptibleWorkerAttempts"`
	MaxWorkersPerRequest         int `json:"maxWorkersPerRequest"`

	VMShutdownGracePeriodSec    int `json:"vmShutdownGracePeriodSec"`
	MaxZombiesBeforeAbort       int `json:"maxZombiesBeforeAbort"`
	MaxConsecutiveFailedBatches int `json:"maxConsecutiveFailedBatches"`
	LingerTimeSec               int `json:"lingerTimeSec"`
}

func runDevSubmit(c *cli.Context) error {
	args := c.Args()
	jobSpecFile := args.Get(0)
	if jobSpecFile == "" {
		return fmt.Errorf("job spec json file path is required")
	}
	workpoolSpecFile := args.Get(1)
	if workpoolSpecFile == "" {
		return fmt.Errorf("workpool spec json file path is required")
	}
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	gcsPrefix := c.String("gcs-prefix")
	if gcsPrefix == "" {
		return fmt.Errorf("--gcs-prefix is required")
	}
	return devSubmit(jobSpecFile, workpoolSpecFile, project, c.String("db"), gcsPrefix)
}

func readJSON[T any](path string) (*T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return &v, nil
}

func resolveWorkpoolID(workpoolSpec *WorkpoolSpec) (string, error) {
	if workpoolSpec.ID != "" {
		return workpoolSpec.ID, nil
	}
	canonical, err := json.Marshal(workpoolSpec)
	if err != nil {
		return "", fmt.Errorf("canonicalizing workpool spec: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func devSubmit(jobSpecFile, workpoolSpecFile, project, db, gcsPrefix string) error {
	jobSpec, err := readJSON[JobSpec](jobSpecFile)
	if err != nil {
		return err
	}

	workpoolSpec, err := readJSON[WorkpoolSpec](workpoolSpecFile)
	if err != nil {
		return err
	}

	ctx := context.Background()

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

	// Ensure sparkles-events topic exists before subscribing or publishing.
	topicName := fmt.Sprintf("projects/%s/topics/sparkles-events", project)
	if _, err := psClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName}); err != nil {
		if grpcstatus.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("ensuring sparkles-events topic: %w", err)
		}
	}

	// Create an ephemeral subscription so we receive all events published during this run.
	monitorSubID := fmt.Sprintf("devsubmit-monitor-%s", uuid.New().String()[:8])
	monitorSubName := fmt.Sprintf("projects/%s/subscriptions/%s", project, monitorSubID)
	if _, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  monitorSubName,
		Topic: topicName,
	}); err != nil {
		log.Printf("devSubmit: creating monitor subscription: %v", err)
	} else {
		monCtx, monCancel := context.WithCancel(ctx)
		defer func() {
			monCancel()
			psClient.SubscriptionAdminClient.DeleteSubscription(context.Background(), &pubsubpb.DeleteSubscriptionRequest{Subscription: monitorSubName})
		}()
		go func() {
			sub := psClient.Subscriber(monitorSubID)
			sub.Receive(monCtx, func(ctx context.Context, msg *pubsub.Message) {
				msg.Ack()
				eventID := msg.Attributes["event_id"]
				if eventID == "" {
					return
				}
				doc, err := fsClient.Collection(v100.EventCollection).Doc(eventID).Get(ctx)
				if err != nil {
					log.Printf("devSubmit: monitor: looking up event %s: %v", eventID, err)
					return
				}
				var record v100.EventRecord
				if err := doc.DataTo(&record); err != nil {
					log.Printf("devSubmit: monitor: parsing event %s: %v", eventID, err)
					return
				}
				log.Printf("event received: id=%s type=%s", record.EventID, record.Type)
			})
		}()
	}

	workpoolID, err := resolveWorkpoolID(workpoolSpec)
	if err != nil {
		return err
	}
	workpool := v100.WorkPool{
		WorkpoolID:            workpoolID,
		MachineType:           workpoolSpec.MachineType,
		RootDir:               workpoolSpec.RootDir,
		SparklesWorkerGCSPath: workpoolSpec.SparklesWorkerGCSPath,
		ServiceAccount:        workpoolSpec.ServiceAccount,
		Resources:             workpoolSpec.Resources,
		EmptyVolumes:          workpoolSpec.EmptyVolumes,
		Expiry:                time.Now().Add(7 * 24 * time.Hour),
		Region:                workpoolSpec.Region,
		Zones:                 workpoolSpec.Zones,

		MaxWorkerCount:               workpoolSpec.MaxWorkerCount,
		MaxPreemptibleWorkerAttempts: workpoolSpec.MaxPreemptibleWorkerAttempts,
		MaxWorkersPerRequest:         workpoolSpec.MaxWorkersPerRequest,

		VMShutdownGracePeriodSec:    workpoolSpec.VMShutdownGracePeriodSec,
		MaxZombiesBeforeAbort:       workpoolSpec.MaxZombiesBeforeAbort,
		MaxConsecutiveFailedBatches: workpoolSpec.MaxConsecutiveFailedBatches,
	}

	_, err = fsClient.Collection(v100.WorkpoolCollection).Doc(workpoolID).Set(ctx, workpool)
	if err != nil {
		return fmt.Errorf("writing workpool to firestore: %w", err)
	}
	fmt.Printf("workpool %s written\n", workpoolID)

	jobID := uuid.New().String()
	now := time.Now()
	job := v100.Job{
		JobID:      jobID,
		Name:       jobSpec.Name,
		WorkpoolID: workpoolID,
		CreatedAt:  now,
		Expiry:     now.Add(7 * 24 * time.Hour),
		TaskCount:  len(jobSpec.Tasks),
		Resources:  jobSpec.Resources,
		Labels:     jobSpec.Labels,
	}

	// Pre-generate task IDs outside the transaction so retries are idempotent.
	taskIDs := make([]string, len(jobSpec.Tasks))
	for i := range taskIDs {
		taskIDs[i] = uuid.New().String()
	}

	err = fsClient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := tx.Set(fsClient.Collection(v100.JobCollection).Doc(jobID), job); err != nil {
			return err
		}
		for i, t := range jobSpec.Tasks {
			taskPrefix := fmt.Sprintf("%s/%s/%d", gcsPrefix, jobSpec.Name, i)
			task := v100.Task{
				JobID:           jobID,
				TaskID:          taskIDs[i],
				TaskIndex:       i,
				WorkpoolID:      workpoolID,
				Status:          v100.StatusPending,
				Command:         t.Command,
				DockerImage:     t.DockerImage,
				FilesToLocalize: jobSpec.FilesToLocalize,
				ResultPath:      taskPrefix,
				LogPath:         taskPrefix + "/stdout.txt",
				Expiry:          now.Add(7 * 24 * time.Hour),
			}
			if err := tx.Set(fsClient.Collection(v100.TaskCollection).Doc(taskIDs[i]), task); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("writing job and tasks to firestore: %w", err)
	}
	fmt.Printf("job %s written with %d tasks\n", jobID, len(jobSpec.Tasks))

	// Create the initial JobSummary so the monitor's job-summary poll can track it.
	summaryStore := monitor.NewFirestoreJobSummaryStore(fsClient)
	if err := summaryStore.Create(ctx, &monitor.JobSummary{
		JobID:      jobID,
		WorkpoolID: workpoolID,
		CreatedAt:  now,
		Expiry:     now.Add(7 * 24 * time.Hour),
		State:      monitor.JobStatusPending,
		Tasks:      []monitor.StateCount{{State: "pending", Count: len(jobSpec.Tasks)}},
		Labels:     toMonitorLabels(jobSpec.Labels),
	}); err != nil {
		return fmt.Errorf("creating job summary: %w", err)
	}

	// Publish job_created event so the monitor can react immediately.
	// Non-fatal: the monitor's periodic poll will pick up the job if this fails.
	ep := v100.NewEventPublisher(psClient.Publisher("sparkles-events"), fsClient)
	defer ep.Stop()
	if err := ep.PublishJobCreated(ctx, v100.JobCreatedEvent{JobID: jobID, WorkpoolID: workpoolID}); err != nil {
		log.Printf("devSubmit: publishing job_created event: %v", err)
	}

	terminalStatuses := map[string]bool{
		v100.StatusSuccess: true,
		v100.StatusError:   true,
		v100.StatusFailed:  true,
		v100.StatusKilled:  true,
	}

	lastEventTime := now
	printNewEvents := func() {
		eventDocs, err := fsClient.Collection(v100.EventCollection).
			Where("job_id", "==", jobID).
			Where("timestamp", ">", lastEventTime).
			OrderBy("timestamp", firestore.Asc).
			Documents(ctx).GetAll()
		if err != nil {
			log.Printf("querying Events: %v", err)
			return
		}
		for _, doc := range eventDocs {
			var record v100.EventRecord
			if err := doc.DataTo(&record); err != nil {
				continue
			}
			lastEventTime = record.Timestamp
			fmt.Printf("%s [event] %s\n", record.Timestamp.Format("15:04:05"), record.Type)
		}
	}

	lastFetchedTime := now
	printNewLogEntries := func() {
		logDocs, err := fsClient.Collection("TaskLog").
			Where("timestamp", ">", lastFetchedTime).
			OrderBy("timestamp", firestore.Asc).
			Documents(ctx).GetAll()
		if err != nil {
			log.Printf("querying TaskLog: %v", err)
			return
		}
		for _, doc := range logDocs {
			var entry v100.OutputTaskEvent
			if err := doc.DataTo(&entry); err != nil {
				continue
			}
			lastFetchedTime = entry.Timestamp
			if entry.Type == "output" {
				fmt.Printf("%s [%s] %s: %s", entry.Timestamp.Format("15:04:05"), entry.Type, entry.TaskID, entry.Content)
			} else {
				fmt.Printf("%s [%s] %s\n", entry.Timestamp.Format("15:04:05"), entry.Type, entry.TaskID)
			}
		}
	}

	for {
		printNewEvents()
		printNewLogEntries()

		docs, err := fsClient.Collection(v100.TaskCollection).
			Where("job_id", "==", jobID).
			Documents(ctx).GetAll()
		if err != nil {
			return fmt.Errorf("querying tasks: %w", err)
		}

		counts := make(map[string]int)
		for _, doc := range docs {
			var t v100.Task
			if err := doc.DataTo(&t); err != nil {
				continue
			}
			counts[t.Status]++
		}

		parts := make([]string, 0, len(counts))
		for status, count := range counts {
			parts = append(parts, fmt.Sprintf("%s: %d", status, count))
		}
		fmt.Printf("%s %s\n", time.Now().Format("15:04:05"), strings.Join(parts, ", "))

		totalTerminal := 0
		for status, count := range counts {
			if terminalStatuses[status] {
				totalTerminal += count
			}
		}
		if totalTerminal == len(docs) {
			break
		}

		time.Sleep(3 * time.Second)
	}

	printNewLogEntries()

	return nil
}
