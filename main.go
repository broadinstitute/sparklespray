package kubequeconsume

import (
	"log"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"

	"github.com/urfave/cli"
)

/*


// def task_completed(self, task_id, was_successful, failure_reason=None, retcode=None):
//     if was_successful:
//         new_status = STATUS_SUCCESS
//         assert failure_reason is None
//     else:
//         new_status = STATUS_FAILED
//     self.updateTaskStatus(task_id, new_status, failure_reason, retcode)


func mainx() {
	ctx := context.Background()

	// Create a datastore client. In a typical application, you would create
	// a single client which is reused for every datastore operation.
	dsClient, err := datastore.NewClient(ctx, "my-project")
	if err != nil {
		// Handle error.
	}

	k := datastore.NameKey("Entity", "stringID", nil)
	e := new(Entity)
	if err := dsClient.Get(ctx, k, e); err != nil {
		// Handle error.
	}

	old := e.Value
	e.Value = "Hello World!"

	if _, err := dsClient.Put(ctx, k, e); err != nil {
		// Handle error.
	}

	fmt.Printf("Updated value from %q to %q\n", old, e.Value)
}


*/

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
	ioc, err := NewIOClient(ctx)
	if err != nil {
		log.Printf("Creating io client failed: %v", err)
		return err
	}
	options := &Options{minTryTime: 1000,
		claimTimeout:      1000,
		initialClaimRetry: 1000,
		owner:             owner}

	executor := func(taskId string, taskParam string) (string, error) {
		return executeTaskFromUrl(ioc, taskId, taskParam, cacheDir)
	}

	err = consumerRunLoop(ctx, client, jobID, executor, options)
	if err != nil {
		log.Printf("consumerRunLoop exited with: %v\n", err)
		return err
	}

	return nil
}
