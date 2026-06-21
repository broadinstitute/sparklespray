package v100

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"net/http"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/broadinstitute/sparklespray/v100/autoscaler"
	"github.com/broadinstitute/sparklespray/v100/autoscaler/emulator"
	"github.com/broadinstitute/sparklespray/v100/scheduler"
	"github.com/google/uuid"
	"github.com/urfave/cli"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const defaultDB = "sparkles"

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
				cli.StringFlag{Name: "db", Value: defaultDB},
				cli.StringFlag{Name: "workpool"},
				cli.StringFlag{Name: "resources"},
				cli.BoolFlag{Name: "no-gcp", Usage: "local development mode: skip GCP metadata server"},
			},
			Action: runWorker,
		},
		{
			Name:  "autoscale",
			Usage: "Start the autoscaler loop",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "project", Usage: "GCP project ID (required)"},
				cli.StringFlag{Name: "db", Value: defaultDB, Usage: "Firestore database"},
				cli.BoolFlag{Name: "verbose, v", Usage: "log a message at the start of every poll"},
			},
			Action: runAutoscale,
		},
		{
			Name: "dev",
			Subcommands: []cli.Command{
				{
					Name:      "submit",
					ArgsUsage: "<job-spec-json> <workpool-spec-json>",
					Flags: []cli.Flag{
						cli.StringFlag{Name: "project"},
						cli.StringFlag{Name: "db", Value: defaultDB},
					},
					Action: runDevSubmit,
				},
				{
					Name:  "dumpdb",
					Usage: "Print all tasks and workpools from Firestore",
					Flags: []cli.Flag{
						cli.StringFlag{Name: "project"},
						cli.StringFlag{Name: "db", Value: defaultDB},
					},
					Action: runDevDumpDB,
				},
				{
					Name:  "batchapi-emulator",
					Usage: "Run a local batch API emulator for testing",
					Flags: []cli.Flag{
						cli.StringFlag{Name: "addr", Value: ":8742", Usage: "address to listen on"},
						cli.DurationFlag{Name: "queueTime", Value: 0, Usage: "how long jobs sit in QUEUED state before containers are started"},
						cli.BoolFlag{Name: "no-docker", Usage: "run commands directly in batch-api-procs/<instance> instead of Docker"},
					},
					Action: runBatchAPIEmulator,
				},
			},
		},
	}

	return app.Run(os.Args)
}

func runDevDumpDB(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")

	ctx := context.Background()
	log.Printf("Connecting to project %s, db %s", project, db)
	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	fmt.Println("=== Tasks ===")
	taskDocs, err := fsClient.Collection(taskCollection).Documents(ctx).GetAll()
	if err != nil {
		return fmt.Errorf("listing tasks: %w", err)
	}
	for _, doc := range taskDocs {
		var t Task
		if err := doc.DataTo(&t); err != nil {
			fmt.Printf("  %s  (error reading fields: %v)\n", doc.Ref.ID, err)
			continue
		}
		fmt.Printf("  %s  job=%s  workpool=%s  status=%s\n", t.TaskID, t.JobID, t.WorkpoolID, t.Status)
	}
	fmt.Printf("  (%d total)\n\n", len(taskDocs))

	fmt.Println("=== WorkPools ===")
	poolDocs, err := fsClient.Collection(workpoolCollection).Documents(ctx).GetAll()
	if err != nil {
		return fmt.Errorf("listing workpools: %w", err)
	}
	for _, doc := range poolDocs {
		var p WorkPool
		if err := doc.DataTo(&p); err != nil {
			fmt.Printf("  %s  (error reading fields: %v)\n", doc.Ref.ID, err)
			continue
		}
		fmt.Printf("  %s  machine=%s  region=%s  status=%s\n", p.WorkpoolID, p.MachineType, p.Region, p.Status)
	}
	fmt.Printf("  (%d total)\n", len(poolDocs))

	if emulatorURL := os.Getenv("SPARKLES_BATCH_API_EMULATOR"); emulatorURL != "" {
		fmt.Println()
		if err := dumpBatchAPI(ctx, emulatorURL); err != nil {
			fmt.Printf("batch API dump error: %v\n", err)
		}
	}

	return nil
}

type dumpJobInfo struct {
	JobID  string `json:"jobID"`
	Status string `json:"status"`
	Labels []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"labels"`
	VMs []struct {
		InstanceName string `json:"instanceName"`
		Zone         string `json:"zone"`
		Done         bool   `json:"done"`
		ExitCode     int    `json:"exitCode"`
	} `json:"vms"`
}

type dumpVMInfo struct {
	InstanceName string `json:"instanceName"`
	Zone         string `json:"zone"`
}

func batchAPIGet(ctx context.Context, url string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func dumpBatchAPI(ctx context.Context, baseURL string) error {
	fmt.Println("=== Batch API Jobs ===")
	var jobsResp struct {
		Jobs []dumpJobInfo `json:"jobs"`
	}
	if err := batchAPIGet(ctx, baseURL+"/jobs", &jobsResp); err != nil {
		return fmt.Errorf("GET /jobs: %w", err)
	}
	for _, job := range jobsResp.Jobs {
		labels := ""
		for _, l := range job.Labels {
			labels += fmt.Sprintf(" %s=%s", l.Name, l.Value)
		}
		fmt.Printf("  %s  status=%s  vms=%d%s\n", job.JobID, job.Status, len(job.VMs), labels)
		for _, vm := range job.VMs {
			doneStr := ""
			if vm.Done {
				doneStr = fmt.Sprintf(" done(exit=%d)", vm.ExitCode)
			}
			fmt.Printf("    vm=%s  zone=%s%s\n", vm.InstanceName, vm.Zone, doneStr)
		}
	}
	fmt.Printf("  (%d total)\n\n", len(jobsResp.Jobs))

	fmt.Println("=== Batch API VMs (by zone) ===")
	var zonesResp struct {
		Zones []string `json:"zones"`
	}
	if err := batchAPIGet(ctx, baseURL+"/region/emulator/zones", &zonesResp); err != nil {
		return fmt.Errorf("GET /region/emulator/zones: %w", err)
	}
	totalVMs := 0
	for _, zone := range zonesResp.Zones {
		var vmsResp struct {
			VMs map[string]dumpVMInfo `json:"vms"`
		}
		if err := batchAPIGet(ctx, baseURL+"/vms/"+zone, &vmsResp); err != nil {
			fmt.Printf("  zone=%s  error: %v\n", zone, err)
			continue
		}
		fmt.Printf("  zone=%s  vms=%d\n", zone, len(vmsResp.VMs))
		for _, vm := range vmsResp.VMs {
			fmt.Printf("    %s\n", vm.InstanceName)
		}
		totalVMs += len(vmsResp.VMs)
	}
	fmt.Printf("  (%d total across %d zones)\n", totalVMs, len(zonesResp.Zones))

	return nil
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
	return devSubmit(jobSpecFile, workpoolSpecFile, project, c.String("db"))
}

type JobSpec struct {
	Name            string           `json:"name"`
	Resources       []ResourceEntry  `json:"resources"`
	FilesToLocalize []FileToLocalize `json:"filesToLocalize"`
	Tasks           []JobSpecTask    `json:"tasks"`
}

type JobSpecTask struct {
	Command     []string `json:"command"`
	DockerImage string   `json:"dockerImage"`
}

type WorkpoolSpec struct {
	// if specified, will be used as the id, if not, we'll compute a hash of this spec to determine the ID
	ID           string          `json:"id"`
	MachineType  string          `json:"machineType"`
	RootDir      string          `json:"rootDir"`
	Resources    []ResourceEntry `json:"resources"`
	EmptyVolumes []EmptyVolume   `json:"emptyVolumes"`
	Region       string          `json:"region"`
	Zones        []string        `json:"zones"`

	// Provisioning parameters
	MaxWorkerCount               int `json:"maxWorkerCount"`
	MaxPreemptibleWorkerAttempts int `json:"maxPreemptibleWorkerAttempts"`
	MaxWorkersPerRequest         int `json:"maxWorkersPerRequest"`

	// Watchdog parameters (zero value → autoscaler uses its own defaults)
	MinTimeBetweenPollsSec      int `json:"minTimeBetweenPollsSec"`
	MaxTimeBetweenPollsSec      int `json:"maxTimeBetweenPollsSec"`
	MaxTimeToStartWorkerSec     int `json:"maxTimeToStartWorkerSec"`
	MaxTimeInQueueSec           int `json:"maxTimeInQueueSec"`
	VMShutdownGracePeriodSec    int `json:"vmShutdownGracePeriodSec"`
	MaxZombiesBeforeAbort       int `json:"maxZombiesBeforeAbort"`
	MaxConsecutiveFailedBatches int `json:"maxConsecutiveFailedBatches"`
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
	// Canonicalize by re-serializing, then hash.
	canonical, err := json.Marshal(workpoolSpec)
	if err != nil {
		return "", fmt.Errorf("canonicalizing workpool spec: %w", err)
	}
	sum := sha256.Sum256(canonical)
	workpoolID := hex.EncodeToString(sum[:])
	return workpoolID, nil
}

func devSubmit(jobSpecFile, workpoolSpecFile, project, db string) error {
	jobSpec, err := readJSON[JobSpec](jobSpecFile)
	if err != nil {
		return err
	}
	_ = jobSpec

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
				doc, err := fsClient.Collection(eventCollection).Doc(eventID).Get(ctx)
				if err != nil {
					log.Printf("devSubmit: monitor: looking up event %s: %v", eventID, err)
					return
				}
				var record EventRecord
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
	workpool := WorkPool{
		WorkpoolID:   workpoolID,
		MachineType:  workpoolSpec.MachineType,
		RootDir:      workpoolSpec.RootDir,
		Resources:    workpoolSpec.Resources,
		EmptyVolumes: workpoolSpec.EmptyVolumes,
		Expiry:       time.Now().Add(7 * 24 * time.Hour),
		Region:       workpoolSpec.Region,
		Zones:        workpoolSpec.Zones,

		MaxWorkerCount:               workpoolSpec.MaxWorkerCount,
		MaxPreemptibleWorkerAttempts: workpoolSpec.MaxPreemptibleWorkerAttempts,
		MaxWorkersPerRequest:         workpoolSpec.MaxWorkersPerRequest,

		MinTimeBetweenPollsSec:      workpoolSpec.MinTimeBetweenPollsSec,
		MaxTimeBetweenPollsSec:      workpoolSpec.MaxTimeBetweenPollsSec,
		MaxTimeToStartWorkerSec:     workpoolSpec.MaxTimeToStartWorkerSec,
		MaxTimeInQueueSec:           workpoolSpec.MaxTimeInQueueSec,
		VMShutdownGracePeriodSec:    workpoolSpec.VMShutdownGracePeriodSec,
		MaxZombiesBeforeAbort:       workpoolSpec.MaxZombiesBeforeAbort,
		MaxConsecutiveFailedBatches: workpoolSpec.MaxConsecutiveFailedBatches,
	}

	_, err = fsClient.Collection(workpoolCollection).Doc(workpoolID).Set(ctx, workpool)
	if err != nil {
		return fmt.Errorf("writing workpool to firestore: %w", err)
	}
	fmt.Printf("workpool %s written\n", workpoolID)

	jobID := uuid.New().String()
	job := Job{
		JobID:      jobID,
		Name:       jobSpec.Name,
		WorkpoolID: workpoolID,
		Resources:  jobSpec.Resources,
	}

	// Pre-generate task IDs outside the transaction so retries are idempotent.
	taskIDs := make([]string, len(jobSpec.Tasks))
	for i := range taskIDs {
		taskIDs[i] = uuid.New().String()
	}

	err = fsClient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := tx.Set(fsClient.Collection(jobCollection).Doc(jobID), job); err != nil {
			return err
		}
		for i, t := range jobSpec.Tasks {
			task := Task{
				JobID:           jobID,
				TaskID:          taskIDs[i],
				TaskIndex:       i,
				WorkpoolID:      workpoolID,
				Status:          StatusPending,
				Command:         t.Command,
				DockerImage:     t.DockerImage,
				FilesToLocalize: jobSpec.FilesToLocalize,
			}
			if err := tx.Set(fsClient.Collection(taskCollection).Doc(taskIDs[i]), task); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("writing job and tasks to firestore: %w", err)
	}
	fmt.Printf("job %s written with %d tasks\n", jobID, len(jobSpec.Tasks))

	// Publish job_created event so the autoscaler can react immediately.
	// Non-fatal: the autoscaler's periodic poll will pick up the job if this fails.
	ep := NewEventPublisher(psClient.Publisher("sparkles-events"), fsClient)
	defer ep.Stop()
	if err := ep.PublishJobCreated(ctx, JobCreatedEvent{JobID: jobID, WorkpoolID: workpoolID}); err != nil {
		log.Printf("devSubmit: publishing job_created event: %v", err)
	}

	return nil
}

func runBatchAPIEmulator(c *cli.Context) error {
	return emulator.Run(c.String("addr"), c.Duration("queueTime"), c.Bool("no-docker"))
}

func runAutoscale(c *cli.Context) error {
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

	pools := autoscaler.NewFirestoreWorkPoolStore(fsClient)
	batches := autoscaler.NewFirestoreBatchRequestStore(fsClient)
	workers := autoscaler.NewFirestoreWorkerStore(fsClient)
	tasks := autoscaler.NewFirestoreTaskStore(fsClient)

	var batchAPI autoscaler.BatchAPIClient
	if emulatorURL := os.Getenv("SPARKLES_BATCH_API_EMULATOR"); emulatorURL != "" {
		batchAPI = autoscaler.NewRemoteBatchAPIClient(emulatorURL)
	} else {
		batchAPI, err = autoscaler.NewGCPBatchAPIClient(ctx, project)
		if err != nil {
			return fmt.Errorf("creating batch API client: %w", err)
		}
	}

	pubsubReceiver, err := autoscaler.NewGCPPubSubReceiver(ctx, project, batches)
	if err != nil {
		return fmt.Errorf("creating pubsub receiver: %w", err)
	}

	jobEventReceiver, err := autoscaler.NewGCPJobEventReceiver(ctx, project)
	if err != nil {
		return fmt.Errorf("creating job event receiver: %w", err)
	}

	as := autoscaler.New(scheduler.RealClock, batchAPI, pools, batches, workers, tasks, pubsubReceiver)
	as.SetVerbose(c.Bool("verbose"))
	as.SetJobEventReceiver(jobEventReceiver)
	as.RunAutoscalerLoop(ctx)
	return nil
}
