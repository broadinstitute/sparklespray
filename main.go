package kubequeconsume


import (
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"

	"github.com/urfave/cli"
	compute "google.golang.org/api/compute/v1"
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
				cli.StringFlag{Name: "cluster"},
				cli.StringFlag{Name: "tasksDir"},
				cli.StringFlag{Name: "zones"}
			},
			Action: consume}}

	app.Run(os.Args)
}

func consume(c *cli.Context) error {
	owner := c.String("owner")
	projectID := c.String("projectId")
	cacheDir := c.String("cacheDir")
	cluster := c.String("cluster")
	tasksDir := c.String("tasksDir")
	zones := strings.Split(c.String("zones"), ",")

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
		}
	}

	options := &Options{MinTryTime: 1000,
		ClaimTimeout:      1000,
		InitialClaimRetry: 1000,
		Owner:             owner}

	executor := func(taskId string, taskParam string) (string, error) {
		return ExecuteTaskFromUrl(ioc, taskId, taskParam, cacheDir, tasksDir)
	}

	Timeout := 1 * time.Second
	ReservationSize := 1
	ReservationTimeout := 60 * time.Minute

	service, err := compute.New(&http.Client{})
	timeout := NewClusterTimeout(service, clusterName, zones, projectID, owner, timeout,
		ReservationSize, ReservationTimeout)

	err = ConsumerRunLoop(ctx, client, cluster, executor, options)
	if err != nil {
		log.Printf("consumerRunLoop exited with: %v\n", err)
		return err
	}

	return nil
}
