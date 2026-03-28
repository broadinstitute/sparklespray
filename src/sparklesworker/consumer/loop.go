package consumer

import (
	"encoding/json"
	"log"
	"time"

	"context"

	"cloud.google.com/go/logging"
	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/task_queue"
)

type Options struct {
	WorkerID           string
	InitialClaimRetry  time.Duration
	SleepOnEmpty       time.Duration
	ClaimTimeout       time.Duration
	MaxWaitForNewTasks time.Duration
	LoggingClient      *logging.Client
}

type Executor func(taskId string, taskSpec *task_queue.TaskSpec, expiry time.Time) (*ExecuteTaskResult, error)

func RunLoop(ctx context.Context, queue task_queue.TaskQueue, sleepUntilNotify func(sleepTime time.Duration),
	executor Executor, SleepOnEmpty time.Duration, MaxWaitForNewTasks time.Duration) error {

	firstClaim := true
	lastClaim := time.Now()
	log.Printf("Starting ConsumerRunLoop, sleeping %v once queue drains", SleepOnEmpty)
	for {
		claimed, err := queue.ClaimTask(ctx)
		if err != nil {
			return err
		}

		if claimed == nil {
			if firstClaim {
				firstClaim = false
				log.Printf("Special case: first poll returned no results. May be due to newly created tasks are not yet visible. Waiting a few seconds and trying again")
				sleepUntilNotify(MaxWaitForNewTasks)
				continue
			}

			nextSleep := min(SleepOnEmpty, MaxWaitForNewTasks)
			sleepUntilNotify(nextSleep)

			if time.Since(lastClaim) > MaxWaitForNewTasks {
				// if we've had more than SleepOnEmpty time elapse since the last time we got something from the queue, it's time to
				// gracefully shut down.
				break
			}

			continue
		}

		lastClaim = time.Now()

		taskJSON, _ := json.MarshalIndent(claimed, "", "  ")
		log.Printf("Claimed task %s:\n%s", claimed.TaskID, taskJSON)
		firstClaim = false

		jobKilled, err := queue.IsJobKilled(ctx, claimed.JobID)
		if err != nil {
			log.Printf("Got error in IsJobKilled for %s: %v", claimed.JobID, err)
			return err
		}

		if !jobKilled {
			execTaskResult, err := executor(claimed.TaskID, claimed.TaskSpec, claimed.Expiry)
			if err != nil {
				log.Printf("Got error executing task %s: %v, marking task as failed", claimed.TaskID, err)

				_, updateErr := updateTaskFailed(ctx, queue, claimed.TaskID, err.Error())
				if updateErr != nil {
					log.Printf("Got error updating task %s failed: %v", claimed.TaskID, updateErr)
					return updateErr
				}
			} else {
				_, updateErr := UpdateTaskCompleted(ctx, queue, claimed.TaskID, execTaskResult.RetCode, execTaskResult.OutputsKey, execTaskResult.LogsKey, execTaskResult.UsedCacheResultFromTaskID)
				if updateErr != nil {
					log.Printf("Got error updating task %s is complete: %v", claimed.TaskID, updateErr)
					return updateErr
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

func UpdateTaskCompleted(ctx context.Context, q task_queue.TaskQueue, taskID string, retcode string, outputKey string, logsKey string, usedCacheResultFromTaskID string) (*task_queue.Task, error) {
	log.Printf("updateTaskCompleted of task %v, retcode=%s outputKey=%s logskey=%s", taskID, retcode, outputKey, logsKey)

	now := backend.GetTimestampMillis()
	taskHistory := &task_queue.TaskHistory{
		Timestamp: float64(now) / 1000.0,
		Status:    task_queue.StatusComplete,
	}

	mutate := func(task *task_queue.Task) bool {
		// taskJSON, _ := json.MarshalIndent(task, "", "  ")
		// log.Printf("In mutate of updateTaskCompleted:\n%s", taskJSON)

		if task.Status != task_queue.StatusClaimed {
			log.Printf("While attempting to mark task as complete, found task had status %v. Aborting", task.Status)
			return false
		}

		task.History = append(task.History, taskHistory)
		task.Status = task_queue.StatusComplete
		task.ExitCode = retcode
		task.LastUpdated = float64(now) / 1000.0
		task.OutputAetherFSRoot = outputKey
		task.LogAetherFSRoot = logsKey
		task.UsedCacheResultFromTaskID = usedCacheResultFromTaskID

		return true
	}

	return q.AtomicUpdateTask(ctx, taskID, mutate)
}

func updateTaskFailed(ctx context.Context, q task_queue.TaskQueue, taskID string, failure string) (*task_queue.Task, error) {
	log.Printf("updateTaskFailed of task %v, failure=%s", taskID, failure)

	now := backend.GetTimestampMillis()
	taskHistory := &task_queue.TaskHistory{
		Timestamp: float64(now) / 1000.0,
		Status:    task_queue.StatusFailed,
	}

	mutate := func(task *task_queue.Task) bool {
		if task.Status != task_queue.StatusClaimed {
			log.Printf("While attempting to mark task as failed, found task had status %v. Aborting", task.Status)
			return false
		}

		task.History = append(task.History, taskHistory)
		task.Status = task_queue.StatusFailed
		task.FailureReason = failure
		task.LastUpdated = float64(now) / 1000.0

		return true
	}

	return q.AtomicUpdateTask(ctx, taskID, mutate)
}

func updateTaskKilled(ctx context.Context, q task_queue.TaskQueue, taskID string) (*task_queue.Task, error) {
	log.Printf("updateTaskKilled of task %v", taskID)

	now := backend.GetTimestampMillis()
	taskHistory := &task_queue.TaskHistory{
		Timestamp: float64(now) / 1000.0,
		Status:    task_queue.StatusKilled,
	}

	mutate := func(task *task_queue.Task) bool {
		if task.Status != task_queue.StatusClaimed {
			log.Printf("While attempting to mark task as killed, found task had status %v. Aborting", task.Status)
			return false
		}

		task.History = append(task.History, taskHistory)
		task.Status = task_queue.StatusKilled
		task.LastUpdated = float64(now) / 1000.0

		return true
	}

	return q.AtomicUpdateTask(ctx, taskID, mutate)
}
