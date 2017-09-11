package kubequeconsume

import (
	"errors"
	"log"
	"math/rand"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
)

const STATUS_CLAIMED = "claimed"
const STATUS_PENDING = "pending"
const STATUS_COMPLETE = "complete"

type TaskHistory struct {
	Timestamp     int64  `datastore:"timestamp,noindex"`
	Status        string `datastore:"status,noindex"`
	FailureReason string `datastore:"failure_reason,noindex,omitempty"`
	Owner         string `datastore:"owner,noindex,omitempty"`
}

type Task struct {
	// will be of the form: job_id + task_index
	TaskID           string         `datastore:"task_id"`
	TaskIndex        int64          `datastore:"task_index"`
	JobID            string         `datastore:"job_id"`
	Status           string         `datastore:"status"`
	Owner            string         `datastore:"owner"`
	Args             string         `datastore:"args"`
	History          []*TaskHistory `datastore:"history"`
	CommandResultURL string         `datastore:"command_result_url"`
	FailureReason    string         `datastore:"failure_reason,omitempty"`
	Version          int32          `datastore:"version"`
	ExitCode         string         `datastore:"exit_code"`
}

type Job struct {
	JobID       int     `datastore:"job_id"`
	Tasks       []*Task `datastore:"tasks"`
	KubeJobSpec string  `datastore:"kube_job_spec"`
	Metadata    string  `datastore:"metadata"`
}

const INITIAL_CLAIM_RETRY_DELAY = 1000

type Options struct {
	MinTryTime        int
	ClaimTimeout      int
	InitialClaimRetry int
	Owner             string
}

type Executor func(taskId string, taskParam string) (string, error)

func getTimestampMillis() int64 {
	return int64(time.Now().UnixNano()) / int64(time.Millisecond)
}

func getTasks(ctx context.Context, client *datastore.Client, jobID string, status string, maxFetch int) ([]*Task, error) {
	q := datastore.NewQuery("Task").Filter("job_id =", jobID).Filter("status =", status).Limit(maxFetch)
	var tasks []*Task
	_, err := client.GetAll(ctx, q, &tasks)
	//log.Printf("getTasks got: %v\n", tasks)
	if err != nil {
		return nil, err
	}
	// TODO: check state and job because saw in python sometimes getting tasks with wrong state
	return tasks, nil
}

func claimTask(ctx context.Context, client *datastore.Client, jobID string, newOwner string, initialClaimRetry int, minTryTime int, claimTimeout int) (*Task, error) {
	//     "Returns None if no unclaimed ready tasks. Otherwise returns instance of Task"
	claimStart := getTimestampMillis()
	maxSleepTime := initialClaimRetry
	for {
		// log.Println("getTask of pending")
		tasks, err := getTasks(ctx, client, jobID, STATUS_PENDING, 20)
		if err != nil {
			return nil, err
		}
		if len(tasks) == 0 {
			// We might have tasks we can't see yet.
			log.Printf("Time since start of claim: %v\n", getTimestampMillis()-claimStart)
			if getTimestampMillis()-claimStart < int64(minTryTime) {
				log.Println("no tasks, sleeping...")
				time.Sleep(time.Second)
				log.Println("awake")
				continue
			} else {
				log.Println("no tasks and timeout expired")
				return nil, nil
			}
		}

		//log.Println("Picking from possible tasks")
		// pick a random task to avoid contention
		task := tasks[rand.Int31n(int32(len(tasks)))]

		finalTask, err := updateTaskClaimed(ctx, client, task.TaskID, newOwner)
		if err == nil {
			maxSleepTime = INITIAL_CLAIM_RETRY_DELAY
			return finalTask, nil
		}

		// failed to claim task.
		claimEnd := getTimestampMillis()
		if claimEnd-claimStart > int64(claimTimeout) {
			return nil, errors.New("Timed out trying to get task")
		}

		log.Printf("Got error claiming task: %s, will rety", err)

		maxSleepTime *= 2
		sleepMillis(rand.Int31n(int32(maxSleepTime)))
	}
}

func sleepMillis(milliseconds int32) {
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}

func ConsumerRunLoop(ctx context.Context, client *datastore.Client, jobId string, executor Executor, options *Options) error {
	for {
		claimed, err := claimTask(ctx, client, jobId, options.Owner, options.InitialClaimRetry, options.MinTryTime, options.ClaimTimeout)
		if err != nil {
			return err
		}
		if claimed == nil {
			break
		}

		log.Printf("Claimed task %s", claimed.TaskID)

		retcode, err := executor(claimed.TaskID, claimed.Args)
		if err != nil {
			log.Printf("Got error executing task %s: %v", claimed.TaskID, err)
			return err
		}

		_, err = updateTaskCompleted(ctx, client, claimed.TaskID, retcode)
		if err != nil {
			log.Printf("Got error updating task %s is complete: %v", claimed.TaskID, err)
			return err
		}
	}
	log.Printf("No more tasks to claim")

	return nil
}

func updateTaskClaimed(ctx context.Context, client *datastore.Client, task_id string, newOwner string) (*Task, error) {
	now := getTimestampMillis()
	event := TaskHistory{Timestamp: now,
		Status: STATUS_CLAIMED,
		Owner:  newOwner}

	mutate := func(task *Task) bool {
		if task.Status != STATUS_PENDING {
			log.Printf("Expected status to be pending but was '%s'\n", task.Status)
			return false
		}

		task.History = append(task.History, &event)
		task.Status = STATUS_CLAIMED
		task.Owner = newOwner

		return true
	}

	updatedTask, err := atomicUpdateTask(ctx, client, task_id, mutate)
	if err != nil {
		return nil, err
	}

	return updatedTask, nil
}

func updateTaskCompleted(ctx context.Context, client *datastore.Client, task_id string, retcode string) (*Task, error) {
	now := getTimestampMillis()
	taskHistory := &TaskHistory{Timestamp: now,
		Status: STATUS_COMPLETE}

	mutate := func(task *Task) bool {
		task.History = append(task.History, taskHistory)
		task.Status = STATUS_COMPLETE
		task.ExitCode = retcode

		return true
	}

	task, err := atomicUpdateTask(ctx, client, task_id, mutate)
	if err != nil {
		// I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
		return nil, err
	}

	return task, nil
}

func atomicUpdateTask(ctx context.Context, client *datastore.Client, task_id string, mutateTaskCallback func(task *Task) bool) (*Task, error) {
	var task Task

	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		//		log.Printf("attempting update of task %s start", task_id)

		taskKey := datastore.NameKey("Task", task_id, nil)
		err := tx.Get(taskKey, &task)
		if err != nil {
			return err
		}

		//		log.Printf("Calling mutate")
		successfulUpdate := mutateTaskCallback(&task)
		if !successfulUpdate {
			return errors.New("Update failed")
		}

		//		log.Printf("Calling put\n")
		task.Version = task.Version + 1
		_, err = tx.Put(taskKey, &task)
		if err != nil {
			return err
		}

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
