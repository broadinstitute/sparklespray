package submit

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
)

var SubmitCmd = cli.Command{
	Name:  "submit",
	Usage: "Submit tasks from a JSON file to the queue",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "projectId", Usage: "Google Cloud project ID"},
		cli.StringFlag{Name: "cluster", Usage: "Cluster ID to submit tasks to"},
		cli.StringFlag{Name: "database", Usage: "Firestore Database ID"},
		cli.StringFlag{Name: "redisAddr", Usage: "Redis server address (e.g., localhost:6379); if set, uses Redis instead of Datastore"},
		cli.StringFlag{Name: "file", Usage: "Path to JSON file containing a list of Task objects"},
	},
	Action: submit,
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
		log.Printf("Using Firestore backend (project=%s)", projectID)
		client, err := firestore.NewClientWithDatabase(ctx, projectID, database)
		if err != nil {
			return fmt.Errorf("creating firestore client: %w", err)
		}
		queue = task_queue.NewFirestoreQueue(client, cluster, "", 0, 0)
	}

	if err := queue.AddTasks(ctx, tasks); err != nil {
		return fmt.Errorf("adding tasks: %w", err)
	}

	log.Printf("Successfully submitted %d tasks", len(tasks))
	return nil
}
