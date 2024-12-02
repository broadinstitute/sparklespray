package sparklesworker

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/urfave/cli"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
			Action: consume}}

	app.Run(os.Args)
}

func initCerts() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(pemCerts))
	return pool
}

func clientWithCerts(ctx context.Context, certs *x509.CertPool, scope ...string) (*http.Client, error) {
	// func NewClient(ctx context.Context, src TokenSource) *http.Client {
	// 	if src == nil {
	// 		c, err := internal.ContextClient(ctx)
	// 		if err != nil {
	// 			return &http.Client{Transport: internal.ErrorTransport{Err: err}}
	// 		}
	// 		return c
	// 	}
	// 	return &http.Client{
	// 		Transport: &Transport{
	// 			Base:   internal.ContextTransport(ctx),
	// 			Source: ReuseTokenSource(nil, src),
	// 		},
	// 	}
	// }

	// func DefaultClient(ctx context.Context, scope ...string) (*http.Client, error) {
	// 	ts, err := DefaultTokenSource(ctx, scope...)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return NewClient(ctx, ts), nil
	// }

	// return DefaultClient(ctx, scope)

	return google.DefaultClient(ctx, scope...)
}

func consume(c *cli.Context) error {
	log.Printf("Starting consume")
	certs := initCerts()
	http.DefaultTransport = &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: certs},
	}
	// http.DefaultClient = &http.Client{
	// 	Transport: &http.Transport{
	// 		TLSClientConfig: &tls.Config{RootCAs: certs},
	// 	},
	// }

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
	httpclient, err := clientWithCerts(ctx, certs, "https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		log.Printf("Could not create default client: %v", err)
		return err
	}

	ioc, err := NewIOClient(ctx, certs, httpclient)
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
		ClaimTimeout:       30 * time.Second,            // how long do we keep trying if we get an error claiming a task
		InitialClaimRetry:  1 * time.Second,             // if we get an error claiming, how long until we try again?
		SleepOnEmpty:       1 * time.Second,             // how often to poll the queue if is empty
		MaxWaitForNewTasks: shutdownAfter * time.Second, // how long to wait for a new task to arrive if the queue is empty
		Owner:              owner}

	executor := func(taskId string, taskParam string) (string, error) {
		return ExecuteTaskFromUrl(ioc, taskId, taskParam, cacheDir, tasksDir, monitor)
	}

	sleepUntilNotify := func(sleepTime time.Duration) {
		log.Printf("Going to sleep (max: %d milliseconds)", sleepTime/time.Millisecond)
		time.Sleep(sleepTime)
	}

	transportCreds := grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(certs, ""))
	client, err := datastore.NewClient(ctx, projectID, option.WithGRPCDialOption(transportCreds))
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
