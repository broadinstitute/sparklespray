package kubequeconsume

import (
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/urfave/cli"
	compute "google.golang.org/api/compute/v1"
)

func Main() {
	app := cli.NewApp()
	app.Name = "kubequeconsume"
	app.Version = "0.1"
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
				cli.StringFlag{Name: "zones"},
				cli.StringFlag{Name: "port"},
				cli.IntFlag{Name: "timeout", Value: 5}, // 5 minutes means the process will be killed after 10 minutes
				cli.IntFlag{Name: "restimeout",
					Value: 10}},
			Action: consume}}

	app.Run(os.Args)
}

func consume(c *cli.Context) error {
	log.Printf("Starting consume")

	projectID := c.String("projectId")
	cacheDir := c.String("cacheDir")
	cluster := c.String("cluster")
	tasksDir := c.String("tasksDir")
	tasksFile := c.String("tasksFile")
	port := c.String("port")
	zones := strings.Split(c.String("zones"), ",")
	ReservationTimeout := time.Duration(c.Int("restimeout")) * time.Minute
	watchdogTimeout := time.Duration(c.Int("timeout")) * time.Minute

	EnableWatchdog(watchdogTimeout)

	ctx := context.Background()

	var err error

	ioc, err := NewIOClient(ctx)
	if err != nil {
		log.Printf("Creating io client failed: %v", err)
		return err
	}

	externalIP := owner
	if metadata.OnGCP() {
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
		ClaimTimeout:      30 * time.Second, // how long do we keep trying if we get an error claiming a task
		InitialClaimRetry: 1 * time.Second,  // if we get an error claiming, how long until we try again?
		SleepOnEmpty:      1 * time.Second,  // how often to poll the queue if is empty
		Owner:             owner}

	executor := func(taskId string, taskParam string) (string, error) {
		return ExecuteTaskFromUrl(ioc, taskId, taskParam, cacheDir, tasksDir, monitor)
	}

	Timeout := 1 * time.Second
	ReservationSize := 1

	httpclient, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		log.Printf("Could not create default client: %v", err)
		return err
	}
	service, err := compute.New(httpclient)
	if err != nil {
		log.Printf("Could not create compute service: %v", err)
		return err
	}

	timeout := NewClusterTimeout(service, cluster, zones, projectID, owner, Timeout,
		ReservationSize, ReservationTimeout)

	var sleepUntilNotify func(sleepTime time.Duration)
	sleepUntilNotify = func(sleepTime time.Duration) {
		log.Printf("Going to sleep (max: %d milliseconds)", sleepTime/time.Millisecond)
		time.Sleep(sleepTime)
	}

	client, err := datastore.NewClient(ctx, projectID)
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

	err = ConsumerRunLoop(ctx, queue, sleepUntilNotify, executor, timeout, options.SleepOnEmpty)
	if err != nil {
		log.Printf("consumerRunLoop exited with: %v\n", err)
		return err
	}

	return nil
}
