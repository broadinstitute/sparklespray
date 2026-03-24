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

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/ext_channel"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/redis/go-redis/v9"
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

	// either clusterSpec or clusterID must be provided
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

func makeTask(jobSpec *JobSpec, expiry time.Duration) (*task_queue.Job, *task_queue.Task, error) {
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
		Command:     []string{"/bin/sh", "-c", jobSpec.Command},
		DockerImage: jobSpec.DockerImage,
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

	aetherCfg := consumer.AetherConfig{
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

	job, task, err := makeTask(&jobSpec, expiryDuration)
	if err != nil {
		return fmt.Errorf("building job: %w", err)
	}

	log.Printf("Submitting job %s to cluster %s", job.Name, job.ClusterID)

	var channel ext_channel.ExtChannel
	var queue task_queue.TaskQueue
	if redisAddr != "" {
		log.Printf("Using Redis backend at %s", redisAddr)
		redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := redisClient.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("connecting to Redis at %s: %w", redisAddr, err)
		}
		defer redisClient.Close()
		channel = ext_channel.NewRedisChannel(redisClient)
		queue = task_queue.NewRedisQueue(redisClient, job.ClusterID, "", 0, 0)
	} else {
		log.Printf("Using Firestore backend (project=%s)", projectID)
		client, err := firestore.NewClientWithDatabase(ctx, projectID, database)
		if err != nil {
			return fmt.Errorf("creating firestore client: %w", err)
		}
		channel = ext_channel.NewPubSubChannel(projectID)
		queue = task_queue.NewFirestoreQueue(client, job.ClusterID, "", 0, 0)
	}

	if err := queue.AddJob(ctx, job, []*task_queue.Task{task}); err != nil {
		return fmt.Errorf("adding job: %w", err)
	}
	log.Printf("Successfully submitted job %s", job.Name)

	defer ext_channel.StartLogStream(ctx, channel, topicName)()

	if job.ClusterID == "local" {
		executor := func(taskId string, taskSpec *task_queue.TaskSpec) (*consumer.ExecuteTaskResult, error) {
			return consumer.ExecuteTask(ctx, &aetherCfg, taskId, taskSpec, dir, cacheDir, tasksDir, nil)
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
