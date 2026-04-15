package consume

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"time"

	"context"

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/backend"
	gcp_backend "github.com/broadinstitute/sparklesworker/backend/gcp"
	redis_backend "github.com/broadinstitute/sparklesworker/backend/redis"
	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/monitor"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/broadinstitute/sparklesworker/watchdog"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli"
)

// WorkerVersion is set by the main package before running commands.
var WorkerVersion string

var ConsumeCmd = cli.Command{
	Name:  "consume",
	Usage: "Consume and execute tasks from the queue",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "projectId", Usage: "Google Cloud project ID"},
		cli.StringFlag{Name: "dir", Value: "./sparklesworker", Usage: "Base directory for worker data (used as default for cacheDir and tasksDir)"},
		cli.StringFlag{Name: "cacheDir", Usage: "Directory for caching downloaded files (defaults to dir/cache)"},
		cli.StringFlag{Name: "cluster", Usage: "Cluster ID to consume tasks from"},
		cli.StringFlag{Name: "tasksDir", Usage: "Directory for task working directories (defaults to dir/tasks)"},
		cli.StringFlag{Name: "database", Usage: "Firestore Database ID"},
		cli.IntFlag{Name: "timeout", Value: 5, Usage: "Watchdog timeout in minutes; process is killed after 2x this value if main loop doesn't check in"},
		cli.IntFlag{Name: "shutdownAfter", Value: 0, Usage: "Seconds to wait for new tasks before shutting down (0 = wait indefinitely)"},
		cli.BoolFlag{Name: "localhost", Usage: "If set, does not try to look up instance name and IP from metadata service, but assumes localhost"},
		cli.StringFlag{Name: "expectedVersion", Usage: "Expected worker version; exits with error if version does not match"},
		cli.StringFlag{Name: "redisAddr", Usage: "Redis server address (e.g., localhost:6379) for control channel; if empty, Redis control channel is disabled"},
		cli.StringFlag{Name: "aetherRoot", Usage: "Aether store root (gs://bucket/prefix or local path)"},
		cli.Int64Flag{Name: "aetherMaxSizeToBundle", Value: 0, Usage: "Max file size in bytes eligible for bundling (0 = disable bundling)"},
		cli.Int64Flag{Name: "aetherMaxBundleSize", Value: 0, Usage: "Target max bundle size in bytes"},
		cli.IntFlag{Name: "aetherWorkers", Value: 1, Usage: "Parallel upload workers"},
	},
	Action: consume,
}

func getMetadata(url string) (string, error) {
	var client http.Client
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Got status=%d from fetching %s", resp.StatusCode, url)
		return "", errors.New("fetching metadata failed")
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	return string(bodyBytes), err
}

func getInstanceName() (string, error) {
	return getMetadata("http://metadata.google.internal/computeMetadata/v1/instance/name")
}

func getInstanceZone() (string, error) {
	return getMetadata("http://metadata.google.internal/computeMetadata/v1/instance/zone")
}

func consume(c *cli.Context) error {
	log.Printf("Starting consume")

	projectID := c.String("projectId")
	dir := c.String("dir")
	cacheDir := c.String("cacheDir")
	if cacheDir == "" {
		cacheDir = path.Join(dir, "cache")
	}
	cluster := c.String("cluster")
	tasksDir := c.String("tasksDir")
	if tasksDir == "" {
		tasksDir = path.Join(dir, "tasks")
	}
	shutdownAfter := c.Int("shutdownAfter")
	expectedVersion := c.String("expectedVersion")
	watchdogTimeout := time.Duration(c.Int("timeout")) * time.Minute
	database := c.String("database")

	if expectedVersion != "" && expectedVersion != WorkerVersion {
		errMsg := fmt.Sprintf("Job was submitted for worker version %s but this worker's version is %s", expectedVersion, WorkerVersion)
		log.Print(errMsg)
		return errors.New(errMsg)
	}

	watchdog.Enable(watchdogTimeout)

	ctx := context.Background()

	var err error

	isLocalRun := c.Bool("localhost")
	log.Printf("isLocal = %v (cluster=%s)", isLocalRun, cluster)
	var workerID string
	if !isLocalRun {
		log.Printf("Querying metadata to get host instance name")
		instanceName, err := getInstanceName()
		if err != nil {
			log.Printf("getInstanceName failed: %v", err)
			return err
		} else {
			log.Printf("Got instance name: %s", instanceName)
		}

		zone, err := getInstanceZone()
		if err != nil {
			log.Printf("getInstanceZone failed: %v", err)
			return err
		} else {
			log.Printf("Got zone: %s", zone)
		}

		workerID = zone + "/" + instanceName
	} else {
		log.Printf("Does not appear to be running under GCP, assuming localhost should be used as the name")
		workerID = "localhost"
	}

	options := &consumer.Options{
		ClaimTimeout:       30 * time.Second,
		InitialClaimRetry:  1 * time.Second,
		SleepOnEmpty:       1 * time.Second,
		MaxWaitForNewTasks: time.Duration(shutdownAfter) * time.Second,
		WorkerID:           workerID}

	aetherCfg := backend.AetherConfig{
		Root:            c.String("aetherRoot"),
		MaxSizeToBundle: c.Int64("aetherMaxSizeToBundle"),
		MaxBundleSize:   c.Int64("aetherMaxBundleSize"),
		Workers:         c.Int("aetherWorkers"),
	}

	var taskCache task_queue.TaskCache
	var eventPublisher backend.EventPublisher

	sleepUntilNotify := func(sleepTime time.Duration) {
		log.Printf("Nothing to do. Sleeping for %d ms...", sleepTime/time.Millisecond)
		time.Sleep(sleepTime)
	}

	var queue task_queue.TaskQueue

	var monitor_ *monitor.Monitor
	redisAddr := c.String("redisAddr")
	if redisAddr != "" {
		log.Printf("Using Redis backend at %s", redisAddr)

		redisClient := redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})

		if err := redisClient.Ping(ctx).Err(); err != nil {
			log.Printf("Failed to connect to Redis at %s: %v", redisAddr, err)
			return err
		}
		defer redisClient.Close()

		clusterConfig, err := redis_backend.NewRedisClusterStore(ctx, redisClient).GetClusterConfig(cluster)
		if err != nil {
			log.Printf("Failed to get cluster config from Redis: %v", err)
			return err
		}
		log.Printf("Got cluster config: pub_sub_out_topic=%s", clusterConfig.EventsTopic)

		redisQueue := task_queue.NewRedisQueue(redisClient, cluster, workerID, options.InitialClaimRetry, options.ClaimTimeout)
		redisQueue.WatchdogNotifier = watchdog.Notify
		queue = redisQueue

		redisChannel := redis_backend.NewRedisChannel(redisClient)
		eventPublisher = redis_backend.NewRedisEventPublisher(redisClient, redisChannel, clusterConfig.EventsTopic)
	} else {
		log.Printf("Using Google Cloud backend (Pub/Sub + Firestore)")

		client, err := firestore.NewClientWithDatabase(ctx, projectID, database)
		if err != nil {
			log.Printf("Creating firestore client failed: %v", err)
			return err
		}

		clusterConfig, err := gcp_backend.NewFirestoreClusterStore(ctx, client).GetClusterConfig(cluster)
		if err != nil {
			log.Printf("Failed to get cluster config: %v", err)
			return err
		}
		log.Printf("Got cluster config: control_message_topic=%s, control_response_topic=%s, events_topic=%s", clusterConfig.ControlMessageTopic, clusterConfig.ControlResponseTopic, clusterConfig.EventsTopic)

		fsQueue := task_queue.NewFirestoreQueue(client, cluster, workerID, options.InitialClaimRetry, options.ClaimTimeout)
		fsQueue.WatchdogNotifier = watchdog.Notify
		queue = fsQueue
		taskCache = task_queue.NewFirestoreTaskCache(client)

		pubsubChannel := gcp_backend.NewPubSubChannel(projectID)
		eventPublisher = gcp_backend.NewFirestoreEventPublisher(client, pubsubChannel, clusterConfig.EventsTopic)
		monitor_ = monitor.NewMonitor(ctx, pubsubChannel, clusterConfig.ControlResponseTopic, 1*time.Second, 5*time.Second)
		pubsubChannel.Subscribe(ctx, clusterConfig.ControlMessageTopic, func(b []byte) {
			var cmd backend.Command
			if err := json.Unmarshal(b, &cmd); err != nil {
				log.Printf("Failed to deserialize control message: %v", err)
				return
			}
			switch cmd.Type {
			case backend.CmdStartStatusStream:
				var startCmd backend.StartStatusStreamCommand
				if err := json.Unmarshal(b, &startCmd); err != nil {
					log.Printf("Failed to deserialize StartStatusStreamCommand: %v", err)
					return
				}
				if err := monitor_.StartListeningIfTaskID(startCmd.TaskID); err != nil {
					log.Printf("Failed to start listening for task %s: %v", startCmd.TaskID, err)
				}
			case backend.CmdStopStatusStream:
				var stopCmd backend.StopStatusStreamCommand
				if err := json.Unmarshal(b, &stopCmd); err != nil {
					log.Printf("Failed to deserialize StopStatusStreamCommand: %v", err)
					return
				}
				monitor_.StopListeningIfTaskID(stopCmd.TaskID)
			default:
				log.Printf("Ignoring unknown control message type: %s", cmd.Type)
			}
		})
	}

	var executor consumer.Executor
	executor = func(taskId, jobID string, taskSpec *task_queue.TaskSpec, expiry time.Time) (*consumer.ExecuteTaskResult, error) {
		return consumer.ExecuteTask(ctx, &aetherCfg, taskId, jobID, taskSpec, dir, cacheDir, tasksDir, monitor_, taskCache, expiry, eventPublisher)
	}

	if err := eventPublisher.PublishEvent(ctx, backend.NewWorkerEvent(backend.EventTypeWorkerStarted, cluster, workerID)); err != nil {
		log.Printf("Failed to publish worker_started event: %v", err)
		return err
	}

	err = consumer.RunLoop(ctx, cluster, queue, sleepUntilNotify, executor, options.SleepOnEmpty, options.MaxWaitForNewTasks, eventPublisher)
	if err != nil {
		log.Printf("consumerRunLoop exited with: %v\n", err)
		return err
	}

	if err := eventPublisher.PublishEvent(ctx, backend.NewWorkerEvent(backend.EventTypeWorkerCompleted, cluster, workerID)); err != nil {
		log.Printf("Failed to publish worker_completed event: %v", err)
		return err
	}

	return nil
}
