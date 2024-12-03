package sparklesworker

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/urfave/cli"
	"google.golang.org/api/option"
)

func Main() {
	app := cli.NewApp()
	app.Name = "sparklesworker"
	app.Version = "5.0.0"
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		cli.Author{
			Name:  "Philip Montgomery",
			Email: "pmontgom@broadinstitute.org",
		}}

	app.Commands = []cli.Command{
		cli.Command{
			Name: "consume",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "projectId"},
				cli.StringFlag{Name: "cacheDir"},
				cli.StringFlag{Name: "cluster"},
				cli.StringFlag{Name: "tasksDir"},
				cli.StringFlag{Name: "tasksFile"},
				cli.StringFlag{Name: "port"},
				cli.IntFlag{Name: "timeout", Value: 5}, // watchdog timeout: 5 minutes means the process will be killed after 10 minutes if the main loop doesn't check in
				cli.IntFlag{Name: "shutdownAfter", Value: 30},
			},
			Action: consume},
		cli.Command{Name: "copyexe",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "dst"},
			},
			Action: copyexe},
		cli.Command{
			Name: "fetch",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "expectMD5"},
				cli.StringFlag{Name: "src"},
				cli.StringFlag{Name: "dst"},
			},
			Action: fetch}}

	app.Run(os.Args)
}

func clientWithCerts(ctx context.Context, scope ...string) (*http.Client, error) {
	return google.DefaultClient(ctx, scope...)
}

func copyexe(c *cli.Context) error {
	dst := c.String("dst")
	executablePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("couldn't get path to executable: %s", err)
	}

	log.Printf("Installing (copying %s to %s)", executablePath, dst)

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

	log.Printf("Installing (copying %s to %s)", executablePath, dst)

	return nil
}

func fetch(c *cli.Context) error {
	log.Printf("Starting fetch")

	expectMD5 := c.String("expectMD5")
	src := c.String("src")
	dst := c.String("dst")

	ctx := context.Background()

	var err error

	httpClient, err := clientWithCerts(ctx, "https://www.googleapis.com/auth/compute.readonly")
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

func consume(c *cli.Context) error {
	log.Printf("Starting consume")

	projectID := c.String("projectId")
	cacheDir := c.String("cacheDir")
	cluster := c.String("cluster")
	tasksDir := c.String("tasksDir")
	tasksFile := c.String("tasksFile")
	port := c.String("port")
	shutdownAfter := c.Int("shutdownAfter")
	watchdogTimeout := time.Duration(c.Int("timeout")) * time.Minute

	EnableWatchdog(watchdogTimeout)

	ctx := context.Background()

	var err error
	httpclient, err := clientWithCerts(ctx, "https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		log.Printf("Could not create default client: %v", err)
		return err
	}

	ioc, err := NewIOClient(ctx, httpclient)
	if err != nil {
		log.Printf("Creating io client failed: %v", err)
		return err
	}

	isLocalRun := strings.HasPrefix(cluster, "local-")
	log.Printf("isLocal = %s (cluster=%s)", isLocalRun, cluster)
	var owner string
	var externalIP string
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

		owner = zone + "/" + instanceName
		externalIP, err = GetExternalIP()
		if err != nil {
			log.Printf("GetExternalIP failed: %v", err)
			return err
		} else {
			log.Printf("Got externalIP: %s", externalIP)
		}
	} else {
		log.Printf("Does not appear to be running under GCP, assuming localhost should be used as the name")
		externalIP = "localhost"
		owner = "localhost"
	}

	monitor := NewMonitor()

	options := &Options{
		ClaimTimeout:       30 * time.Second,                           // how long do we keep trying if we get an error claiming a task
		InitialClaimRetry:  1 * time.Second,                            // if we get an error claiming, how long until we try again?
		SleepOnEmpty:       1 * time.Second,                            // how often to poll the queue if is empty
		MaxWaitForNewTasks: time.Duration(shutdownAfter) * time.Second, // how long to wait for a new task to arrive if the queue is empty
		Owner:              owner}

	executor := func(taskId string, taskParam string) (string, error) {
		return ExecuteTaskFromUrl(ioc, taskId, taskParam, cacheDir, tasksDir, monitor)
	}

	sleepUntilNotify := func(sleepTime time.Duration) {
		log.Printf("Going to sleep (max: %d milliseconds)", sleepTime/time.Millisecond)
		time.Sleep(sleepTime)
	}

	httpClient, err := clientWithCerts(ctx, "https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		log.Printf("Could not create default client: %v", err)
		return err
	}
	client, err := datastore.NewClient(ctx, projectID, option.WithHTTPClient(httpClient))
	if err != nil {
		log.Printf("Creating datastore client failed: %v", err)
		return err
	}

	monitorAddress := ""
	if port != "" {
		entityKey := datastore.NameKey("ClusterKeys", "sparklespray", nil)
		var clusterKeys ClusterKeys
		err := client.Get(ctx, entityKey, &clusterKeys)
		if err != nil {
			log.Printf("failed to get cluster keys: %v\n", err)
			return err
		}

		err = monitor.StartServer(":"+port, clusterKeys.Cert, clusterKeys.PrivateKey, clusterKeys.SharedSecret)
		if err != nil {
			log.Printf("Failed to start grpc server: %v", err)
			return err
		}
		monitorAddress = externalIP + ":" + port
	}

	var queue Queue
	if tasksFile != "" {
		queue, err = CreatePreloadedQueue(tasksFile)
	} else {
		queue, err = CreateDataStoreQueue(client, cluster, owner, options.InitialClaimRetry, options.ClaimTimeout, monitorAddress)
	}
	if err != nil {
		log.Printf("failed to initialize queue: %v\n", err)
		return err
	}

	err = ConsumerRunLoop(ctx, queue, sleepUntilNotify, executor, options.SleepOnEmpty, options.MaxWaitForNewTasks)
	if err != nil {
		log.Printf("consumerRunLoop exited with: %v\n", err)
		return err
	}

	return nil
}
