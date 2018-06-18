package kubequeconsume

import (
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/urfave/cli"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
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
				cli.StringFlag{Name: "owner"},
				cli.BoolFlag{Name: "loglive"},
				cli.StringFlag{Name: "projectId"},
				cli.StringFlag{Name: "cacheDir"},
				cli.StringFlag{Name: "cluster"},
				cli.StringFlag{Name: "tasksDir"},
				cli.StringFlag{Name: "zones"},
				cli.IntFlag{Name: "timeout", Value: 5}, // 5 minutes means the process will be killed after 10 minutes
				cli.IntFlag{Name: "restimeout",
					Value: 10}},
			Action: consume}}

	err := app.Run(os.Args)
	if err != nil {
		log.Printf("Exiting retcode == 1 because: %s", err)
		os.Exit(1)
	} else {
		log.Println("Exiting cleanly")
		os.Exit(0)
	}
}

func consume(c *cli.Context) error {
	log.Printf("Starting consume")

	owner := c.String("owner")
	projectID := c.String("projectId")
	cacheDir := c.String("cacheDir")
	cluster := c.String("cluster")
	tasksDir := c.String("tasksDir")
	zones := strings.Split(c.String("zones"), ",")
	ReservationTimeout := time.Duration(c.Int("restimeout")) * time.Minute
	watchdogTimeout := time.Duration(c.Int("timeout")) * time.Minute
	usePubSub := false
	logLive := c.Bool("loglive")

	EnableWatchdog(watchdogTimeout)

	ctx := context.Background()

	var loggingClient *logging.Client
	var err error
	if logLive {
		log.Printf("Creating log client")
		ctx := context.Background()
		loggingClient, err = logging.NewClient(ctx, projectID)
		if err != nil {
			log.Printf("Creating log client failed: %v", err)
			return err
		}
	}

	creds, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		log.Printf("Could not find default credentials: %v", err)
		return err
	}

	client, err := datastore.NewClient(ctx, projectID, option.WithCredentials(creds))
	if err != nil {
		log.Printf("Creating datastore client failed: %v", err)
		return err
	}

	ioc, err := NewIOClient(ctx, creds.TokenSource)
	if err != nil {
		log.Printf("Creating io client failed: %v", err)
		return err
	}

	// Attempting to work around issue where the metadata server becomes unavailible
	// when running >8 hr jobs. Work around by proactively trying to always have a valid token
	// and if we get an error, keep retrying to get a good token.
	pollTokenForever := func() {
		errorCount := 0
		lastAccessToken := ""
		// loop as long as the process is alive
		for {
			token, err := creds.TokenSource.Token()
			if err != nil {
				log.Printf("Got error fetching token (attempt %d): %v", errorCount, err)
				time.Sleep(5 * time.Minute)
				errorCount += 1
				if errorCount > 1000 {
					log.Printf("Giving up")
					break
				}
				continue
			}
			errorCount = 0

			if token.AccessToken != lastAccessToken {
				lastAccessToken = token.AccessToken
				log.Printf("Got new token with expiry: %v", token.Expiry)
			}

			time.Sleep(5 * time.Minute)
		}
	}

	go pollTokenForever()

	if owner == "" {
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
	}

	options := &Options{
		ClaimTimeout:      30 * time.Second, // how long do we keep trying if we get an error claiming a task
		InitialClaimRetry: 1 * time.Second,  // if we get an error claiming, how long until we try again?
		SleepOnEmpty:      1 * time.Second,  // how often to poll the queue if is empty
		Owner:             owner}

	executor := func(taskId string, taskParam string) (string, error) {
		return ExecuteTaskFromUrl(ioc, taskId, taskParam, cacheDir, tasksDir, loggingClient)
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
	if usePubSub {
		// set up notify
		notifyChannel := make(chan bool, 100)
		log.Printf("Creating pubsub client...")
		pubsubClient, err := pubsub.NewClient(ctx, projectID)

		if err != nil {
			log.Printf("Could not create pubsub client: %v", err)
			return err
		}
		log.Printf("pubsub client err=%v", err)

		topic := pubsubClient.Topic("kubeque-global")
		subCtx, subCancel := context.WithCancel(ctx)
		sub, err := pubsubClient.CreateSubscription(subCtx, "sub-name",
			pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			log.Printf("CreateSubscription failed: %v", err)
		} else {
			go (func() {
				err := sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
					log.Printf("Got message: %s", m.Data)
					m.Ack()
					notifyChannel <- true
				})
				if err != nil {
					log.Printf("Subscription receive failed: %v", err)
				}
			})()

			deleteSubscription := func() {
				log.Printf("Deleting subscription")
				subCancel()
				err = sub.Delete(ctx)
				if err != nil {
					log.Printf("Got error while deleting subscription: %v", err)
				}
			}
			defer deleteSubscription()
		}

		sleepUntilNotify = func(sleepTime time.Duration) {
			log.Printf("Going to sleep (max: %d milliseconds)", sleepTime/time.Millisecond)
			select {
			case <-notifyChannel:
				log.Printf("Woke up due to pubsub notification")
			case <-time.After(sleepTime):
				log.Printf("Woke up due to timeout")
			}
		}
	} else {
		sleepUntilNotify = func(sleepTime time.Duration) {
			log.Printf("Going to sleep (max: %d milliseconds)", sleepTime/time.Millisecond)
			time.Sleep(sleepTime)
		}
	}

	err = ConsumerRunLoop(ctx, client, sleepUntilNotify, cluster, executor, timeout, options)
	if err != nil {
		log.Printf("consumerRunLoop exited with: %v\n", err)
		return err
	}

	if loggingClient != nil {
		err = loggingClient.Close()
		if err != nil {
			log.Printf("loggingClient Close returned: %v", err)
		}
	}
	return nil
}
