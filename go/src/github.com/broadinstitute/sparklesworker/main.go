package sparklesworker

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/control"
	"github.com/broadinstitute/sparklesworker/monitor"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/broadinstitute/sparklesworker/watchdog"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const WorkerVersion = "5.8.2"

func Main() error {
	app := cli.NewApp()
	app.Name = "sparklesworker"
	app.Version = WorkerVersion
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		cli.Author{
			Name:  "Philip Montgomery",
			Email: "pmontgom@broadinstitute.org",
		}}

	app.Commands = []cli.Command{
		cli.Command{
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
				cli.IntFlag{Name: "ftShutdownAfter", Value: 30, Usage: "Shutdown delay in seconds for the first task in a batch job"},
				cli.BoolFlag{Name: "localhost", Usage: "If set, does not try to look up instance name and IP from metadata service, but assumes localhost"},
				cli.StringFlag{Name: "expectedVersion", Usage: "Expected worker version; exits with error if version does not match"},
				cli.StringFlag{Name: "redisAddr", Usage: "Redis server address (e.g., localhost:6379) for control channel; if empty, Redis control channel is disabled"},
				cli.StringFlag{Name: "aetherRoot", Usage: "Aether store root (gs://bucket/prefix or local path)"},
			},
			Action: consume},
		cli.Command{
			Name:  "submit",
			Usage: "Submit tasks from a JSON file to the queue",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "projectId", Usage: "Google Cloud project ID"},
				cli.StringFlag{Name: "cluster", Usage: "Cluster ID to submit tasks to"},
				cli.StringFlag{Name: "database", Usage: "Firestore Database ID"},
				cli.StringFlag{Name: "redisAddr", Usage: "Redis server address (e.g., localhost:6379); if set, uses Redis instead of Datastore"},
				cli.StringFlag{Name: "file", Usage: "Path to JSON file containing a list of Task objects"},
			},
			Action: submit},
		cli.Command{
			Name:  "copyexe",
			Usage: "Copy the worker executable to a destination path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "dst", Usage: "Destination path for the copied executable"},
			},
			Action: copyexe},
		cli.Command{
			Name:  "exec",
			Usage: "Execute a single task directly from the command line",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "command", Usage: "Shell command to run (required)"},
				cli.StringFlag{Name: "commandResultURL", Value: "results.json", Usage: "Local path to write result JSON"},
				cli.StringFlag{Name: "aetherRoot", Usage: "Aether store root (gs://bucket/prefix or local path)"},
				cli.StringFlag{Name: "aetherFSRoot", Usage: "Input aether manifest ref for downloads"},
				cli.StringFlag{Name: "dir", Value: "./sparklesworker", Usage: "Base directory for worker data"},
				cli.StringFlag{Name: "cacheDir", Usage: "Cache directory (defaults to dir/cache)"},
				cli.StringFlag{Name: "tasksDir", Usage: "Tasks directory (defaults to dir/tasks)"},
				cli.StringFlag{Name: "taskId", Value: "exec-task", Usage: "Task identifier"},
				cli.StringFlag{Name: "workingDir", Usage: "Working subdirectory within task dir"},
				cli.StringFlag{Name: "dockerImage", Usage: "Docker image for execution"},
				cli.StringSliceFlag{Name: "includePattern", Usage: "Upload include glob pattern (repeatable)"},
				cli.StringSliceFlag{Name: "excludePattern", Usage: "Upload exclude glob pattern (repeatable)"},
				cli.StringSliceFlag{Name: "param", Usage: "Parameter as key=value (repeatable)"},
				cli.StringFlag{Name: "preDownloadScript", Usage: "Script to run before download"},
				cli.StringFlag{Name: "postDownloadScript", Usage: "Script to run after download"},
				cli.StringFlag{Name: "preExecScript", Usage: "Script to run before exec"},
				cli.StringFlag{Name: "postExecScript", Usage: "Script to run after exec"},
			},
			Action: execTask},
		cli.Command{
			Name:  "fetch",
			Usage: "Download a file from Google Cloud Storage with MD5 verification",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "expectMD5", Usage: "Expected MD5 hash (hex-encoded) of the downloaded file"},
				cli.StringFlag{Name: "src", Usage: "Source GCS path (gs://bucket/object)"},
				cli.StringFlag{Name: "dst", Usage: "Destination local file path"},
			},
			Action: fetch}}

	return app.Run(os.Args)
}

func copyexe(c *cli.Context) error {
	dst := c.String("dst")
	executablePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("couldn't get path to executable: %s", err)
	}

	log.Printf("Installing (copying %s to %s)", executablePath, dst)

	parentDir := path.Dir(dst)
	// create parent dir if it doesn't already exist
	os.MkdirAll(parentDir, 0777)

	reader, err := os.Open(executablePath)
	if err != nil {
		return fmt.Errorf("could open %s for reading: %s", executablePath, err)
	}
	defer reader.Close()

	// create executable file
	writer, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)

	if err != nil {
		return fmt.Errorf("could open %s for writing: %s", dst, err)
	}
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	if err != nil {
		return fmt.Errorf("failed copying %s to %s writing: %s", executablePath, dst, err)
	}

	log.Printf("Installed %s to %s", executablePath, dst)

	return nil
}

func fetch(c *cli.Context) error {
	log.Printf("Starting fetch")
	certs := initCerts()

	expectMD5 := c.String("expectMD5")
	src := c.String("src")
	dst := c.String("dst")

	ctx := context.Background()

	var err error

	httpClient, err := clientWithCerts(ctx, certs, "https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		log.Printf("Could not create default client: %v", err)
		return err
	}

	client, err := storage.NewClient(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return fmt.Errorf("could not get storage client: %s", err)
	}

	bucketName, objectName := splitGCSPath(src)
	if bucketName == "" {
		return fmt.Errorf("expected source to be gs://<bucket>/<object> but source was \"%s\"", src)
	}

	object := client.Bucket(bucketName).Object(objectName)
	reader, err := object.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("could not open %s for reading: %s", src, err)
	}

	err = CopyToFile(reader, dst, expectMD5)
	if err != nil {
		return fmt.Errorf("copy of %s failed: %s", src, err)
	}

	return nil
}

func splitGCSPath(path string) (bucket, key string) {
	re := regexp.MustCompile(`^gs://([^/]+)/(.+)$`)
	matches := re.FindStringSubmatch(path)
	if len(matches) == 3 {
		return matches[1], matches[2]
	}
	return "", ""
}

func CopyToFile(src io.Reader, dstPath string, expectedMD5 string) error {
	// Create temp file in same directory as destination
	dir := filepath.Dir(dstPath)
	tmpFile, err := os.CreateTemp(dir, "*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Clean up temp file on failure
	success := false
	defer func() {
		tmpFile.Close()
		if !success {
			os.Remove(tmpPath)
		}
	}()

	wrapper := NewMD5Writer(tmpFile)

	// Copy data to temp file
	if _, err := io.Copy(wrapper, src); err != nil {
		return fmt.Errorf("copying data: %w", err)
	}

	// Close before rename
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	// check md5 of written file matches expectation
	md5 := hex.EncodeToString(wrapper.MD5())
	if md5 != expectedMD5 {
		return fmt.Errorf("MD5 hash did not match expected")
	}

	// Atomic rename
	if err := os.Rename(tmpPath, dstPath); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}

	success = true
	return nil
}

func initCerts() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(pemCerts))
	return pool
}

func clientWithCerts(ctx context.Context, certs *x509.CertPool, scope ...string) (*http.Client, error) {
	return google.DefaultClient(ctx, scope...)
}
func submit(c *cli.Context) error {
	ctx := context.Background()

	filePath := c.String("file")
	cluster := c.String("cluster")
	projectID := c.String("projectId")
	database := c.String("database")
	redisAddr := c.String("redisAddr")

	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("reading file %s: %w", filePath, err)
	}

	var tasks []*task_queue.Task
	if err := json.Unmarshal(data, &tasks); err != nil {
		return fmt.Errorf("parsing tasks from %s: %w", filePath, err)
	}

	log.Printf("Submitting %d tasks to cluster %s", len(tasks), cluster)

	var queue task_queue.TaskQueue
	if redisAddr != "" {
		log.Printf("Using Redis backend at %s", redisAddr)
		redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := redisClient.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("connecting to Redis at %s: %w", redisAddr, err)
		}
		defer redisClient.Close()
		queue = task_queue.NewRedisQueue(redisClient, cluster, "", 0, 0)
	} else {
		log.Printf("Using Datastore backend (project=%s)", projectID)
		client, err := datastore.NewClientWithDatabase(ctx, projectID, database)
		if err != nil {
			return fmt.Errorf("creating datastore client: %w", err)
		}
		queue = task_queue.NewDataStoreQueue(client, cluster, "", 0, 0)
	}

	if err := queue.AddTasks(ctx, tasks); err != nil {
		return fmt.Errorf("adding tasks: %w", err)
	}

	log.Printf("Successfully submitted %d tasks", len(tasks))
	return nil
}

func execTask(c *cli.Context) error {
	command := c.String("command")
	if command == "" {
		return fmt.Errorf("--command is required")
	}

	ctx := context.Background()

	dir := c.String("dir")
	cacheDir := c.String("cacheDir")
	if cacheDir == "" {
		cacheDir = path.Join(dir, "cache")
	}
	tasksDir := c.String("tasksDir")
	if tasksDir == "" {
		tasksDir = path.Join(dir, "tasks")
	}

	commandResultURL := c.String("commandResultURL")

	params := map[string]string{}
	for _, p := range c.StringSlice("param") {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) == 2 {
			params[parts[0]] = parts[1]
		}
	}

	taskSpec := &task_queue.TaskSpec{
		Command:            command,
		AetherFSRoot:       c.String("aetherFSRoot"),
		WorkingDir:         c.String("workingDir"),
		DockerImage:        c.String("dockerImage"),
		Parameters:         params,
		Uploads: &task_queue.UploadSpec{
			IncludePatterns: c.StringSlice("includePattern"),
			ExcludePatterns: c.StringSlice("excludePattern"),
		},
		PreDownloadScript:  c.String("preDownloadScript"),
		PostDownloadScript: c.String("postDownloadScript"),
		PreExecScript:      c.String("preExecScript"),
		PostExecScript:     c.String("postExecScript"),
	}

	writeResult := func(data []byte) error {
		return os.WriteFile(commandResultURL, data, 0644)
	}

	aetherCfg := consumer.AetherConfig{Root: c.String("aetherRoot")}
	taskId := c.String("taskId")

	retcode, err := consumer.ExecuteTask(ctx, writeResult, aetherCfg, taskId, taskSpec, dir, cacheDir, tasksDir, nil)
	if err != nil {
		return fmt.Errorf("task failed: %w", err)
	}
	log.Printf("Task completed with exit code: %s", retcode)
	log.Printf("Result written to: %s", commandResultURL)
	return nil
}

func consume(c *cli.Context) error {
	log.Printf("Starting consume")

	certs := initCerts()
	http.DefaultTransport = &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: certs},
	}

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
	firstTaskShutdownAfter := c.Int("ftShutdownAfter")
	expectedVersion := c.String("expectedVersion")
	watchdogTimeout := time.Duration(c.Int("timeout")) * time.Minute
	database := c.String("database")

	batchTaskIndex := os.Getenv("BATCH_TASK_INDEX")
	if batchTaskIndex == "0" {
		shutdownAfter = firstTaskShutdownAfter
		log.Printf("First task in batch. Updated shutdownAfter to %d", shutdownAfter)
	}

	if expectedVersion != "" && expectedVersion != WorkerVersion {
		errMsg := fmt.Sprintf("Job was submitted for worker version %s but this worker's version is %s", expectedVersion, WorkerVersion)
		log.Print(errMsg)
		return errors.New(errMsg)
	}

	watchdog.Enable(watchdogTimeout)

	ctx := context.Background()

	var err error
	httpclient, err := clientWithCerts(ctx, certs, "https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		log.Printf("Could not create default client: %v", err)
		return err
	}

	ioc, err := NewIOClient(ctx, httpclient)
	if err != nil {
		log.Printf("Creating io client failed: %v", err)
		return err
	}

	isLocalRun := c.Bool("localhost")
	log.Printf("isLocal = %v (cluster=%s)", isLocalRun, cluster)
	var workerID string
	if !isLocalRun {
		log.Printf("Querying metadata to get host instance name")
		instanceName, err := GetInstanceName()
		if err != nil {
			log.Printf("GetInstanceName failed: %v", err)
			return err
		} else {
			log.Printf("Got instance name: %s", instanceName)
		}

		zone, err := GetInstanceZone()
		if err != nil {
			log.Printf("GetInstanceZone failed: %v", err)
			return err
		} else {
			log.Printf("Got zone: %s", zone)
		}

		workerID = zone + "/" + instanceName
	} else {
		log.Printf("Does not appear to be running under GCP, assuming localhost should be used as the name")
		workerID = "localhost"
	}

	mon := monitor.New()

	options := &consumer.Options{
		ClaimTimeout:       30 * time.Second,                           // how long do we keep trying if we get an error claiming a task
		InitialClaimRetry:  1 * time.Second,                            // if we get an error claiming, how long until we try again?
		SleepOnEmpty:       1 * time.Second,                            // how often to poll the queue if is empty
		MaxWaitForNewTasks: time.Duration(shutdownAfter) * time.Second, // how long to wait for a new task to arrive if the queue is empty
		WorkerID:           workerID}

	aetherCfg := consumer.AetherConfig{Root: c.String("aetherRoot")}

	executor := func(taskId string, taskSpec *task_queue.TaskSpec) (string, error) {
		writeResult := func(data []byte) error {
			return ioc.UploadBytes(taskSpec.CommandResultURL, data)
		}
		return consumer.ExecuteTask(ctx, writeResult, aetherCfg, taskId, taskSpec, dir, cacheDir, tasksDir, mon)
	}

	sleepUntilNotify := func(sleepTime time.Duration) {
		log.Printf("Nothing to do. Sleeping for %d ms...", sleepTime/time.Millisecond)
		time.Sleep(sleepTime)
	}

	// Create message handler for control channel
	msgHandler := monitor.NewHandler(mon, workerID)

	// Start control channel and queue - use Redis if configured, otherwise use Pub/Sub and Datastore
	var notifier *control.Notifier
	var cleanupControlChannel func()
	var queue task_queue.TaskQueue

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

		notifier, cleanupControlChannel, err = control.StartRedisChannel(ctx, redisClient, cluster, workerID, msgHandler.HandleMessage)
		if err != nil {
			log.Printf("Failed to start Redis control channel: %v", err)
			return err
		}

		redisQueue := task_queue.NewRedisQueue(redisClient, cluster, workerID, options.InitialClaimRetry, options.ClaimTimeout)
		redisQueue.WatchdogNotifier = watchdog.Notify
		queue = redisQueue
	} else {
		log.Printf("Using Google Cloud backend (Pub/Sub + Datastore)")

		client, err := datastore.NewClientWithDatabase(ctx, projectID, database)
		if err != nil {
			log.Printf("Creating datastore client failed: %v", err)
			return err
		}

		clusterConfig, err := task_queue.GetCluster(ctx, client, cluster)
		if err != nil {
			log.Printf("Failed to get cluster config: %v", err)
			return err
		}
		log.Printf("Got cluster config: incoming_topic=%s, response_topic=%s", clusterConfig.IncomingTopic, clusterConfig.ResponseTopic)

		notifier, cleanupControlChannel, err = control.StartPubSubChannel(ctx, projectID, clusterConfig.IncomingTopic, clusterConfig.ResponseTopic, workerID, msgHandler.HandleMessage)
		if err != nil {
			log.Printf("Failed to start pub/sub subscriber: %v", err)
			return err
		}

		dsQueue := task_queue.NewDataStoreQueue(client, cluster, workerID, options.InitialClaimRetry, options.ClaimTimeout)
		dsQueue.WatchdogNotifier = watchdog.Notify
		queue = dsQueue
	}
	defer cleanupControlChannel()

	// Notify that worker has started
	notifier.NotifyWorkerStarted()

	err = consumer.RunLoop(ctx, queue, sleepUntilNotify, executor, options.SleepOnEmpty, options.MaxWaitForNewTasks, notifier)
	if err != nil {
		log.Printf("consumerRunLoop exited with: %v\n", err)
		notifier.NotifyWorkerStopping()
		return err
	}

	// Notify that worker is stopping
	notifier.NotifyWorkerStopping()

	return nil
}
