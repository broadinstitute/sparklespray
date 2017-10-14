package kubequeconsume

import (
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
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
				cli.StringFlag{Name: "owner"},
				cli.StringFlag{Name: "projectId"},
				cli.StringFlag{Name: "cacheDir"},
				cli.StringFlag{Name: "cluster"},
				cli.StringFlag{Name: "tasksDir"},
				cli.StringFlag{Name: "zones"},
				cli.IntFlag{Name: "timeout", Value: 5}, // 5 minutes means the process will be killed after 10 minutes
				cli.IntFlag{Name: "restimeout",
					Value: 10}},
			Action: consume}}

	app.Run(os.Args)
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

	EnableWatchdog(watchdogTimeout)

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Printf("Creating datastore client failed: %v", err)
		return err
	}

	ioc, err := NewIOClient(ctx)
	if err != nil {
		log.Printf("Creating io client failed: %v", err)
		return err
	}

	if owner == "" {
		log.Printf("Querying metadata to get host instance name")
		owner, err = GetInstanceName()
		if err != nil {
			log.Printf("Creating io client failed: %v", err)
			return err
		} else {
			log.Printf("Got hostname: %s", owner)
		}
	}

	options := &Options{
		ClaimTimeout:      30 * time.Second, // how long do we keep trying if we get an error claiming a task
		InitialClaimRetry: 1 * time.Second,  // if we get an error claiming, how long until we try again?
		SleepOnEmpty:      1 * time.Second,  // how often to poll the queue if is empty
		Owner:             owner}

	executor := func(taskId string, taskParam string) (string, error) {
		return ExecuteTaskFromUrl(ioc, taskId, taskParam, cacheDir, tasksDir)
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

	return nil
}
