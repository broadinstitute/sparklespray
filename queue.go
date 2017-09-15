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
const STATUS_KILLED = "killed"
const JOB_STATUS_KILLED = "killed"

type TaskHistory struct {
	Timestamp     float64 `datastore:"timestamp,noindex"`
	Status        string  `datastore:"status,noindex"`
	FailureReason string  `datastore:"failure_reason,noindex,omitempty"`
	Owner         string  `datastore:"owner,noindex,omitempty"`
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
	Cluster          string         `datastore:"cluster"`
}

type Job struct {
	JobID       int      `datastore:"job_id"`
	Tasks       []string `datastore:"tasks"`
	KubeJobSpec string   `datastore:"kube_job_spec"`
	Metadata    string   `datastore:"metadata"`
	Cluster     string   `datastore:"cluster"`
	Status      string   `datastore:"status"`
	SubmitTime  float64  `datastore:"submit_time"`
}

const INITIAL_CLAIM_RETRY_DELAY = 1000

type Options struct {
	Owner             string
	InitialClaimRetry time.Duration
	SleepOnEmpty      time.Duration
	ClaimTimeout      time.Duration
}

type Executor func(taskId string, taskParam string) (string, error)

func getTimestampMillis() int64 {
	return int64(time.Now().UnixNano()) / int64(time.Millisecond)
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

func claimTask(ctx context.Context, client *datastore.Client, cluster string, newOwner string, initialClaimRetry time.Duration, claimTimeout time.Duration) (*Task, error) {
	//     "Returns None if no unclaimed ready tasks. Otherwise returns instance of Task"
	maxSleepTime := initialClaimRetry
	claimStart := time.Now()
	for {
		// log.Println("getTask of pending")
		tasks, err := getTasks(ctx, client, cluster, STATUS_PENDING, 20)
		if err != nil {
			return nil, err
		}
		if len(tasks) == 0 {
			return nil, nil
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
		claimEnd := time.Now()
		if claimEnd.Sub(claimStart) > claimTimeout {
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

func isJobKilled(ctx context.Context, client *datastore.Client, jobID string) (bool, error) {
	jobKey := datastore.NameKey("Job", jobID, nil)
	var job Job
	err := client.Get(ctx, jobKey, &job)
	if err != nil {
		return false, err
	}

	return job.Status == JOB_STATUS_KILLED, nil
}

func ConsumerRunLoop(ctx context.Context, client *datastore.Client, cluster string, executor Executor, timeout Timeout, options *Options) error {
	for {
		claimed, err := claimTask(ctx, client, cluster, options.Owner, options.InitialClaimRetry, options.ClaimTimeout)
		if err != nil {
			return err
		}

		now := time.Now()
		if claimed == nil {
			if timeout.HasTimeoutExpired(now) {
				return nil
			}
			time.Sleep(options.SleepOnEmpty)
			continue
		}
		timeout.Reset(now)

		log.Printf("Claimed task %s", claimed.TaskID)

		jobKilled, err := isJobKilled(ctx, client, claimed.JobID)
		if err != nil {
			log.Printf("Got error in isJobKilled for %s: %v", claimed.JobID, err)
			return err
		}

		if !jobKilled {
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
		} else {
			_, err = updateTaskKilled(ctx, client, claimed.TaskID)
			if err != nil {
				log.Printf("Got error updating task %s was killed: %v", claimed.TaskID, err)
				return err
			}
		}

	}
	log.Printf("No more tasks to claim")

	return nil
}

func updateTaskClaimed(ctx context.Context, client *datastore.Client, task_id string, newOwner string) (*Task, error) {
	now := getTimestampMillis()
	event := TaskHistory{Timestamp: float64(now) / 1000.0,
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
	log.Printf("updateTaskCompleted of task %v, retcode=%s", task_id, retcode)

	now := getTimestampMillis()
	taskHistory := &TaskHistory{Timestamp: float64(now) / 1000.0,
		Status: STATUS_COMPLETE}

	mutate := func(task *Task) bool {
		if task.Status != STATUS_CLAIMED {
			log.Printf("While attempting to mark task %v as complete, found task had status %v. Aborting", task.Status)
			return false
		}

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

func updateTaskKilled(ctx context.Context, client *datastore.Client, task_id string) (*Task, error) {
	log.Printf("updateTaskKilled of task %v", task_id)

	now := getTimestampMillis()
	taskHistory := &TaskHistory{Timestamp: float64(now) / 1000.0,
		Status: STATUS_KILLED}

	mutate := func(task *Task) bool {
		if task.Status != STATUS_CLAIMED {
			log.Printf("While attempting to mark task %v as killed, found task had status %v. Aborting", task.Status)
			return false
		}

		task.History = append(task.History, taskHistory)
		task.Status = STATUS_KILLED

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
