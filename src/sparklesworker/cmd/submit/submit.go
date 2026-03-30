package submit

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	aetherclient "github.com/pgm/aether/client"

	"github.com/broadinstitute/sparklesworker/backend"
	gcp_backend "github.com/broadinstitute/sparklesworker/backend/gcp"
	redis_backend "github.com/broadinstitute/sparklesworker/backend/redis"
	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/urfave/cli"
)

type JobVolumeMountSpec struct {
	VolumeType string
	MountPath  string
	SizeInGB   int
}

type FileToStage struct {
	LocalPath string
	Name      string
}

// cluster runtime requirements
type ClusterSpec struct {
	machineType string

	bootVolumeInGB int
	bootVolumeType string
	volumeMounts   []*JobVolumeMountSpec

	// needed by the autoscaler
	MaxPreemptableAttempts int32
	TargetNodeCount        int32
}

type JobSpec struct {
	// Name of job. Must follow google's ID conventions.
	Name string

	// either clusterSpec or clusterID must be provided but not both
	ClusterSpec *ClusterSpec
	ClusterID   string

	// per task properties
	DockerImage string
	Command     string

	// information required for submitting
	FilesToStage []*FileToStage

	// used to determine pubsub topics
	TopicPrefix string
}

var SubmitCmd = cli.Command{
	Name:  "submit",
	Usage: "Submit a job and execute it locally until complete",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "projectID", Usage: "Google Cloud project ID"},
		cli.StringFlag{Name: "database", Usage: "Firestore Database ID"},
		cli.StringFlag{Name: "redisAddr", Usage: "Redis server address (e.g., localhost:6379); if set, uses Redis instead of Datastore"},
		cli.StringFlag{Name: "file", Usage: "Path to JSON file containing a JobSpec"},
		cli.StringFlag{Name: "dir", Value: "./sparklesworker", Usage: "Base directory for worker data"},
		cli.StringFlag{Name: "cacheDir", Usage: "Cache directory (defaults to dir/cache)"},
		cli.StringFlag{Name: "tasksDir", Usage: "Tasks directory (defaults to dir/tasks)"},
		cli.StringFlag{Name: "aetherRoot", Usage: "Aether store root (gs://bucket/prefix or local path)"},
		cli.Int64Flag{Name: "aetherMaxSizeToBundle", Value: 0, Usage: "Max file size in bytes eligible for bundling (0 = disable bundling)"},
		cli.Int64Flag{Name: "aetherMaxBundleSize", Value: 0, Usage: "Target max bundle size in bytes"},
		cli.IntFlag{Name: "aetherWorkers", Value: 1, Usage: "Parallel upload workers"},
		cli.StringFlag{Name: "expiry", Value: "24h", Usage: "Duration until job/task data expires (e.g. 24h, 7d)"},
		cli.StringFlag{Name: "topicPrefix", Value: "sparkles", Usage: "Prefix for the log topic name (topic will be <topicPrefix>-log)"},
	},
	Action: submit,
}

// clusterIDFromSpec derives a deterministic cluster ID by hashing the ClusterSpec JSON.
func clusterIDFromSpec(spec *ClusterSpec) (string, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshaling cluster spec: %w", err)
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:8]), nil
}

func makeTask(jobSpec *JobSpec, expiry time.Duration, rootID string) (*task_queue.Job, *task_queue.Task, error) {
	clusterID := jobSpec.ClusterID
	if clusterID == "" {
		if jobSpec.ClusterSpec == nil {
			return nil, nil, fmt.Errorf("either ClusterID or ClusterSpec must be provided")
		}
		var err error
		clusterID, err = clusterIDFromSpec(jobSpec.ClusterSpec)
		if err != nil {
			return nil, nil, err
		}
		panic("Actually using cluster spec is not implemented")
	}

	expiryTime := time.Now().Add(expiry)

	job := &task_queue.Job{
		Name:       jobSpec.Name,
		ClusterID:  clusterID,
		Status:     task_queue.StatusPending,
		SubmitTime: float64(time.Now().UnixMilli()) / 1000.0,
		TaskCount:  1,
		Expiry:     expiryTime,
	}

	taskSpec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", jobSpec.Command},
		DockerImage:  jobSpec.DockerImage,
		AetherFSRoot: rootID,
	}

	task := &task_queue.Task{
		TaskID:    jobSpec.Name + ".1",
		TaskIndex: 0,
		JobID:     jobSpec.Name,
		ClusterID: clusterID,
		Status:    task_queue.StatusPending,
		TaskSpec:  taskSpec,
		Expiry:    expiryTime,
	}

	return job, task, nil
}

func submit(c *cli.Context) error {
	ctx := context.Background()

	filePath := c.String("file")
	projectID := c.String("projectId")
	database := c.String("database")
	redisAddr := c.String("redisAddr")
	dir := c.String("dir")
	cacheDir := c.String("cacheDir")
	if cacheDir == "" {
		cacheDir = path.Join(dir, "cache")
	}
	tasksDir := c.String("tasksDir")
	if tasksDir == "" {
		tasksDir = path.Join(dir, "tasks")
	}
	topicName := c.String("topicPrefix") + "-log"

	aetherCfg := backend.AetherConfig{
		Root:            c.String("aetherRoot"),
		MaxSizeToBundle: c.Int64("aetherMaxSizeToBundle"),
		MaxBundleSize:   c.Int64("aetherMaxBundleSize"),
		Workers:         c.Int("aetherWorkers"),
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("reading file %s: %w", filePath, err)
	}

	var jobSpec JobSpec
	if err := json.Unmarshal(data, &jobSpec); err != nil {
		return fmt.Errorf("parsing job from %s: %w", filePath, err)
	}

	expiryDuration, err := time.ParseDuration(c.String("expiry"))
	if err != nil {
		return fmt.Errorf("invalid --expiry value: %w", err)
	}

	var files []aetherclient.FileInput
	for _, f := range jobSpec.FilesToStage {
		files = append(files, aetherclient.FileInput{Path: f.LocalPath, ManifestName: f.Name})
	}
	mkfsStats, err := aetherclient.MakeFilesystem(ctx, aetherclient.MakeFilesystemOptions{
		Root:            aetherCfg.Root,
		Files:           files,
		MaxSizeToBundle: aetherCfg.MaxSizeToBundle,
		MaxBundleSize:   aetherCfg.MaxBundleSize,
		Workers:         aetherCfg.Workers,
		Expiry:          expiryDuration,
	})
	if err != nil {
		return fmt.Errorf("staging files: %w", err)
	}
	rootID := "sha256:" + mkfsStats.ManifestKey
	log.Printf("Staged %d files, manifest key: %s", mkfsStats.FilesUploaded, rootID)

	job, task, err := makeTask(&jobSpec, expiryDuration, rootID)
	if err != nil {
		return fmt.Errorf("building job: %w", err)
	}

	var extServices *backend.ExternalServices
	if redisAddr != "" {
		log.Printf("Using Redis backend at %s", redisAddr)
		extServices, err = redis_backend.CreateMockServices(ctx, redisAddr, false)
		if err != nil {
			return fmt.Errorf("Creating redis backed services failed: %s", err)
		}
	} else {
		extServices, err = gcp_backend.CreateGCPServices(ctx, projectID, database)
	}

	log.Printf("Submitting job %s to cluster %s", job.Name, job.ClusterID)

	_, err = extServices.Cluster.GetClusterConfig(job.ClusterID)
	if err != nil {
		return fmt.Errorf("Attempted to submit job to cluster %s but got error fetching its config: %w", job.ClusterID, err)
	}

	queue := extServices.NewTaskStore(job.ClusterID)
	channel := extServices.Channel
	defer extServices.Close()

	if err := queue.AddJob(ctx, job, []*task_queue.Task{task}); err != nil {
		return fmt.Errorf("adding job: %w", err)
	}
	log.Printf("Successfully submitted job %s", job.Name)

	defer backend.StartLogStream(ctx, channel, topicName)()

	if job.ClusterID == "local" {
		executor := func(taskId string, taskSpec *task_queue.TaskSpec, expiry time.Time) (*consumer.ExecuteTaskResult, error) {
			return consumer.ExecuteTask(ctx, &aetherCfg, taskId, taskSpec, dir, cacheDir, tasksDir, nil, nil, expiry)
		}
		sleepUntilNotify := func(sleepTime time.Duration) {
			time.Sleep(sleepTime)
		}

		err = consumer.RunLoop(ctx, queue, sleepUntilNotify, executor, 1*time.Second, 10*time.Second)
		if err != nil {
			return fmt.Errorf("RunLoop failed: %s", err)
		}
	}

	for {
		t, err := queue.GetTask(ctx, task.TaskID)
		if err != nil {
			return fmt.Errorf("polling task %s: %w", task.TaskID, err)
		}
		if t.Status != task_queue.StatusPending && t.Status != task_queue.StatusClaimed {
			log.Printf("Task %s finished with status %s", task.TaskID, t.Status)
			break
		}
		log.Printf("Task %s status: %s — waiting...", task.TaskID, t.Status)
		time.Sleep(1 * time.Second)
	}

	return nil
}
