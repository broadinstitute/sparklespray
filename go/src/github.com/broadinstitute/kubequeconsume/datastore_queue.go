package kubequeconsume

import (
	"errors"
	"log"
	"math/rand"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
)

type DataStoreQueue struct {
	client            *datastore.Client
	cluster           string
	owner             string
	InitialClaimRetry time.Duration
	ClaimTimeout      time.Duration
}

func CreateDataStoreQueue(client *datastore.Client, cluster string, owner string, InitialClaimRetry time.Duration, ClaimTimeout time.Duration) (*DataStoreQueue, error) {
	return &DataStoreQueue{client: client, cluster: cluster, owner: owner, InitialClaimRetry: InitialClaimRetry, ClaimTimeout: ClaimTimeout}, nil
}

func getTasks(ctx context.Context, client *datastore.Client, cluster string, status string, maxFetch int) ([]*Task, error) {
	q := datastore.NewQuery("Task").Filter("cluster =", cluster).Filter("status =", status).Limit(maxFetch)
	var tasks []*Task
	keys, err := client.GetAll(ctx, q, &tasks)

	//log.Printf("getTasks got: %v\n", tasks)
	if err != nil {
		return nil, err
	}

	for i, key := range keys {
		tasks[i].TaskID = key.Name
	}

	// TODO: check state and job because saw in python sometimes getting tasks with wrong state
	return tasks, nil
}

func (q *DataStoreQueue) claimTask(ctx context.Context) (*Task, error) {
	//     "Returns None if no unclaimed ready tasks. Otherwise returns instance of Task"
	maxSleepTime := q.InitialClaimRetry
	claimStart := time.Now()
	for {
		NotifyWatchdog()
		// log.Println("getTask of pending")
		tasks, err := getTasks(ctx, q.client, q.cluster, STATUS_PENDING, 20)
		if err != nil {
			return nil, err
		}
		if len(tasks) == 0 {
			return nil, nil
		}

		//log.Println("Picking from possible tasks")
		// pick a random task to avoid contention
		task := tasks[rand.Int31n(int32(len(tasks)))]

		finalTask, err := updateTaskClaimed(ctx, q, task.TaskID, q.owner)
		if err == nil {
			maxSleepTime = INITIAL_CLAIM_RETRY_DELAY
			return finalTask, nil
		}

		// failed to claim task.
		claimEnd := time.Now()
		if claimEnd.Sub(claimStart) > q.ClaimTimeout {
			return nil, errors.New("Timed out trying to get task")
		}

		maxSleepTime *= 2
		// should set an upper bound on this
		timeUntilNextTry := time.Duration(rand.Int63n(int64(maxSleepTime)))
		log.Printf("Got error claiming task: %s, will retry after %d milliseconds", err, timeUntilNextTry/time.Millisecond)
		time.Sleep(timeUntilNextTry)
	}
}

func (q *DataStoreQueue) isJobKilled(ctx context.Context, jobID string) (bool, error) {
	jobKey := datastore.NameKey("Job", jobID, nil)
	var job Job
	err := q.client.Get(ctx, jobKey, &job)
	if err != nil {
		return false, err
	}

	return job.Status == JOB_STATUS_KILLED, nil
}

func (q *DataStoreQueue) atomicUpdateTask(ctx context.Context, task_id string, mutateTaskCallback func(task *Task) bool) (*Task, error) {
	var task Task
	client := q.client

	log.Printf("atomicUpdateTask of task %v", task_id)
	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		log.Printf("attempting update of task %s start", task_id)

		taskKey := datastore.NameKey("Task", task_id, nil)
		err := tx.Get(taskKey, &task)
		if err != nil {
			return err
		}
		task.TaskID = task_id

		log.Printf("Calling mutate on task %s with version %d", task.TaskID, task.Version)
		successfulUpdate := mutateTaskCallback(&task)
		if !successfulUpdate {
			log.Printf("Update failed on task %s", task.TaskID)
			return errors.New("Update failed")
		}

		task.Version = task.Version + 1
		log.Printf("Calling put on task %s with version %d", task.TaskID, task.Version)
		_, err = tx.Put(taskKey, &task)
		if err != nil {
			return err
		}

		log.Printf("Returning atomicUpdateTask success")
		return nil
	})

	if err != nil {
		return nil, err
	}

	// task_as_json = json.dumps(attr.asdict(task)).encode("utf8")

	// topic_name = self._job_id_to_topic(task.job_id)
	// topic = self.pubsub.topic(topic_name)
	// topic.publish(task_as_json)

	//	log.Printf("atomic update of task %s success", task_id)
	return &task, nil
}
