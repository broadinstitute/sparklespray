package main

import (
	"log"
	"os"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"

	"github.com/broadinstitute/kubequeconsume"
	"github.com/urfave/cli"
)

func main() {
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
				cli.StringFlag{Name: "jobId"},
			},
			Action: consume}}

	app.Run(os.Args)
}

func consume(c *cli.Context) error {
	owner := c.String("owner")
	projectID := c.String("projectId")
	cacheDir := c.String("cacheDir")
	jobID := c.String("jobId")

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Printf("Creating datastore client failed: %v", err)
		return err
	}

	ioc, err := kubequeconsume.NewIOClient(ctx)
	if err != nil {
		log.Printf("Creating io client failed: %v", err)
		return err
	}

	options := &kubequeconsume.Options{MinTryTime: 1000,
		ClaimTimeout:      1000,
		InitialClaimRetry: 1000,
		Owner:             owner}

	executor := func(taskId string, taskParam string) (string, error) {
		return kubequeconsume.ExecuteTaskFromUrl(ioc, taskId, taskParam, cacheDir)
	}

	err = kubequeconsume.ConsumerRunLoop(ctx, client, jobID, executor, options)
	if err != nil {
		log.Printf("consumerRunLoop exited with: %v\n", err)
		return err
	}

	return nil
}
