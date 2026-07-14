package sparklesworker

import (
	"log"
	"time"

	"cloud.google.com/go/logging"
	"golang.org/x/net/context"
)

const STATUS_CLAIMED = "claimed"
const STATUS_PENDING = "pending"
const STATUS_COMPLETE = "complete"
const STATUS_KILLED = "killed"
const JOB_STATUS_KILLED = "killed"
const STATUS_FAILED = "failed"

type TaskHistory struct {
	Timestamp     float64 `datastore:"timestamp,noindex"`
	Status        string  `datastore:"status,noindex"`
	FailureReason string  `datastore:"failure_reason,noindex,omitempty"`
	Owner         string  `datastore:"owner,noindex,omitempty"`
}

type Task struct {
	// will be of the form: job_id + task_index
	TaskID           string         `datastore:"task_id" json:"task_id"`
	TaskIndex        int64          `datastore:"task_index" json:"task_index"`
	JobID            string         `datastore:"job_id" json:"job_id"`
	Status           string         `datastore:"status" json:"status"`
	Owner            string         `datastore:"owner" json:"owner"`
	Args             string         `datastore:"args" json:"args"`
	History          []*TaskHistory `datastore:"history" json:"history"`
	CommandResultURL string         `datastore:"command_result_url" json:"command_result_url"`
	FailureReason    string         `datastore:"failure_reason,omitempty" json:"failure_reason"`
	Version          int32          `datastore:"version" json:"version"`
	ExitCode         string         `datastore:"exit_code" json:"exit_code"`
	Cluster          string         `datastore:"cluster" json:"cluster"`
	MonitorAddress   string         `datastore:"monitor_address" json:"monitor_address"`
	LogURL           string         `datastore:"log_url", json:"log_url"`
}

type TaskStatusNotification struct {
	TaskID        string `json:"task_id"`
	Status        string `json:"status"`
	ExitCode      string `json:"exit_code"`
	FailureReason string `json:"failure_reason"`
	Version       int32  `json:"version"`
}

type Job struct {
	JobID                  int      `datastore:"job_id"`
	Tasks                  []string `datastore:"tasks"`
	KubeJobSpec            string   `datastore:"kube_job_spec"`
	Metadata               string   `datastore:"metadata"`
	Cluster                string   `datastore:"cluster"`
	Status                 string   `datastore:"status"`
	SubmitTime             float64  `datastore:"submit_time"`
	MaxPreemptableAttempts int32    `datastore:"max_preemptable_attempts"`
	TargetNodeCount        int32    `datastore:"target_node_count"`
}

type ClusterKeys struct {
	Cert         []byte `datastore:"cert"`
	PrivateKey   []byte `datastore:"private_key"`
	SharedSecret string `datastore:"shared_secret"`
}

const INITIAL_CLAIM_RETRY_DELAY = 1000

type Options struct {
	Owner              string
	InitialClaimRetry  time.Duration
	SleepOnEmpty       time.Duration
	ClaimTimeout       time.Duration
	MaxWaitForNewTasks time.Duration
	LoggingClient      *logging.Client
}

type Queue interface {
	claimTask(ctx context.Context) (*Task, error)
	isJobKilled(ctx context.Context, JobID string) (bool, error)
	atomicUpdateTask(ctx context.Context, task_id string, mutateTaskCallback func(task *Task) bool) (*Task, error)
}

type Executor func(taskId string, taskParam string) (string, error)

func getTimestampMillis() int64 {
	return int64(time.Now().UnixNano()) / int64(time.Millisecond)
}

func ConsumerRunLoop(ctx context.Context, queue Queue, sleepUntilNotify func(sleepTime time.Duration), executor Executor, SleepOnEmpty time.Duration, MaxWaitForNewTasks time.Duration) error {
	firstClaim := true
	lastClaim := time.Now()
	log.Printf("Starting ConsumerRunLoop, sleeping %v once queue drains", SleepOnEmpty)
	for {
		claimed, err := queue.claimTask(ctx)
		if err != nil {
			return err
		}

		if claimed == nil {
			if firstClaim {
				firstClaim = false
				log.Printf("Special case: first poll returned no results. May be due to newly created tasks are not yet visible. Waiting a few seconds and trying again")
				sleepUntilNotify(time.Second * 10)
				continue
			}

			sleepUntilNotify(SleepOnEmpty)

			if time.Since(lastClaim) > MaxWaitForNewTasks {
				// if we've had more than SleepOnEmpty time elapse since the last time we got something from the queue, it's time to
				// gracefully shut down.
				break
			}

			continue
		}

		lastClaim = time.Now()

		log.Printf("Claimed task %s", claimed.TaskID)
		firstClaim = false

		jobKilled, err := queue.isJobKilled(ctx, claimed.JobID)
		if err != nil {
			log.Printf("Got error in isJobKilled for %s: %v", claimed.JobID, err)
			return err
		}

		if !jobKilled {
			retcode, err := executor(claimed.TaskID, claimed.Args)
			if err != nil {
				log.Printf("Got error executing task %s: %v, marking task as failed", claimed.TaskID, err)

				_, err = updateTaskFailed(ctx, queue, claimed.TaskID, err.Error())
				if err != nil {
					log.Printf("Got error updating task %s failed: %v", claimed.TaskID, err)
					return err
				}
			} else {
				_, err = updateTaskCompleted(ctx, queue, claimed.TaskID, retcode)
				if err != nil {
					log.Printf("Got error updating task %s is complete: %v", claimed.TaskID, err)
					return err
				}
			}
		} else {
			_, err = updateTaskKilled(ctx, queue, claimed.TaskID)
			if err != nil {
				log.Printf("Got error updating task %s was killed: %v", claimed.TaskID, err)
				return err
			}
		}

	}
	log.Printf("No more tasks to claim")

	return nil
}

func updateTaskClaimed(ctx context.Context, q *DataStoreQueue, task_id string, newOwner string, monitorAddress string) (*Task, error) {
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
		task.MonitorAddress = monitorAddress

		return true
	}

	updatedTask, err := q.atomicUpdateTask(ctx, task_id, mutate)
	if err != nil {
		return nil, err
	}

	notifyTaskStatusChanged(updatedTask)

	return updatedTask, nil
}

func updateTaskCompleted(ctx context.Context, q Queue, task_id string, retcode string) (*Task, error) {
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

	updatedTask, err := q.atomicUpdateTask(ctx, task_id, mutate)
	if err != nil {
		// I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
		return nil, err
	}

	notifyTaskStatusChanged(updatedTask)

	return updatedTask, nil
}

func updateTaskFailed(ctx context.Context, q Queue, task_id string, failure string) (*Task, error) {
	log.Printf("updateTaskFailed of task %v, failure=%s", task_id, failure)

	now := getTimestampMillis()
	taskHistory := &TaskHistory{Timestamp: float64(now) / 1000.0,
		Status: STATUS_FAILED}

	mutate := func(task *Task) bool {
		if task.Status != STATUS_CLAIMED {
			log.Printf("While attempting to mark task %v as complete, found task had status %v. Aborting", task.Status)
			return false
		}

		task.History = append(task.History, taskHistory)
		task.Status = STATUS_FAILED
		task.FailureReason = failure

		return true
	}

	updatedTask, err := q.atomicUpdateTask(ctx, task_id, mutate)
	if err != nil {
		// I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
		return nil, err
	}

	notifyTaskStatusChanged(updatedTask)

	return updatedTask, nil
}

func updateTaskKilled(ctx context.Context, q Queue, task_id string) (*Task, error) {
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

	updatedTask, err := q.atomicUpdateTask(ctx, task_id, mutate)
	if err != nil {
		// I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
		return nil, err
	}

	notifyTaskStatusChanged(updatedTask)

	return updatedTask, nil
}

func notifyTaskStatusChanged(task *Task) {
	// notification := TaskStatusNotification{TaskID = task.TaskID, Status = task.Status, ExitCode = task.ExitCode, FailureReason = task.FailureReason, Version = task.Version}
}
