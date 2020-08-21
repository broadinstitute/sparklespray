package kubequeconsume

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	// "fmt"
	"log"
	"net/http"

	// "path"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

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

func Consume(projectID string, cacheDir string, bucketDir string,
	cluster string, tasksDir string, tasksFile string,
	port int, ReservationTimeoutMinutes int, watchdogTimeoutMinutes int, isLocal bool) error {
	log.Printf("Starting consume")
	certs := initCerts()
	http.DefaultTransport = &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: certs},
	}
	ReservationTimeout := time.Duration(ReservationTimeoutMinutes) * time.Minute
	watchdogTimeout := time.Duration(watchdogTimeoutMinutes) * time.Minute

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

	isLocalRun := isLocal || strings.HasPrefix(cluster, "local-")
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
		ClaimTimeout:      30 * time.Second, // how long do we keep trying if we get an error claiming a task
		InitialClaimRetry: 1 * time.Second,  // if we get an error claiming, how long until we try again?
		SleepOnEmpty:      1 * time.Second,  // how often to poll the queue if is empty
		Owner:             owner}

	gcsMounts, err := MountGCSBuckets(bucketDir)
	if err != nil {
		return err
	}

	executor := func(taskId string, taskParam string) (string, error) {
		return ExecuteTaskFromUrl(ioc, taskId, taskParam, cacheDir, tasksDir, monitor, gcsMounts)
	}

	Timeout := 1 * time.Second

	service, err := compute.New(httpclient)
	if err != nil {
		log.Printf("Could not create compute service: %v", err)
		return err
	}

	transportCreds := grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(certs, ""))
	client, err := datastore.NewClient(ctx, projectID, option.WithGRPCDialOption(transportCreds))
	if err != nil {
		log.Printf("Creating datastore client failed: %v", err)
		return err
	}

	timeout := NewClusterTimeout(service, cluster, projectID, owner, Timeout,
		ReservationTimeout, client)

	var sleepUntilNotify func(sleepTime time.Duration)
	sleepUntilNotify = func(sleepTime time.Duration) {
		log.Printf("Going to sleep (max: %d milliseconds)", sleepTime/time.Millisecond)
		time.Sleep(sleepTime)
	}

	monitorAddress := ""
	if port != 0 {
		entityKey := datastore.NameKey("ClusterKeys", "sparklespray", nil)
		var clusterKeys ClusterKeys
		err := client.Get(ctx, entityKey, &clusterKeys)
		if err != nil {
			log.Printf("failed to get cluster keys: %v\n", err)
			return err
		}

		err = monitor.StartServer(fmt.Sprintf(":%d", port), clusterKeys.Cert, clusterKeys.PrivateKey, clusterKeys.SharedSecret)
		if err != nil {
			log.Printf("Failed to start grpc server: %v", err)
			return err
		}
		monitorAddress = fmt.Sprintf("%s:%d", externalIP, port)
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
