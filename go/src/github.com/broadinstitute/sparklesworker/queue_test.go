package sparklesworker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/datastore"

	"github.com/broadinstitute/sparklesworker/control"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/stretchr/testify/assert"
)

type MockTimeout struct {
}

func (t *MockTimeout) Reset(timestamp time.Time) {

}

func (t *MockTimeout) HasTimeoutExpired(timestamp time.Time) bool {
	return true
}

func spawnExecuteTasks(t *testing.T, projectID string, jobID string, index int, ready *sync.WaitGroup, done *sync.WaitGroup, taskParamPerClient [][]string) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, projectID)
	assert.Nil(t, err)

	workerID := fmt.Sprintf("thread-%d", index)
	cluster := "c"
	queue := task_queue.NewDataStoreQueue(client, cluster, workerID, 1*time.Second, 60*time.Second)

	run := func() {
		ready.Wait()
		executor := func(taskID string, taskParam string) (string, error) {
			// remember we executed this task
			taskParamPerClient[index] = append(taskParamPerClient[index], taskParam)
			log.Printf("client %d executed %s\n", index, taskParam)
			return "0", nil
		}
		sleepFunc := func(sleepTime time.Duration) {
			time.Sleep(sleepTime)
		}
		err := ConsumerRunLoop(ctx, queue, sleepFunc, executor, 1*time.Second, 10*time.Second, nil)
		if err != nil {
			log.Printf("consumerRunLoop returned error: %v\n", err)
		}
		done.Done()
	}
	go run()
}

func deleteTasks(t *testing.T, ctx context.Context, client *datastore.Client, jobID string) {
	q := datastore.NewQuery("Task").Filter("job_id =", jobID).Limit(100)
	for {
		var tasks []task_queue.Task
		keys, err := client.GetAll(ctx, q, &tasks)
		assert.Nil(t, err)
		if len(keys) == 0 {
			break
		}
		log.Printf("Deleting %d tasks...", len(keys))
		err = client.DeleteMulti(ctx, keys)
		assert.Nil(t, err)
	}
}

func submitTasks(t *testing.T, ctx context.Context, client *datastore.Client, tasks []*task_queue.Task) {
	keys := make([]*datastore.Key, len(tasks))
	for i, task := range tasks {
		keys[i] = datastore.NameKey("Task", task.TaskID, nil)
	}

	for i := 0; i < len(tasks); i += 500 {
		_, err := client.PutMulti(ctx, keys[i:i+500], tasks[i:i+500])
		assert.Nil(t, err)
		if err != nil {
			panic(err.Error())
		}
	}
}

func newTask(jobID string, index int) *task_queue.Task {
	taskID := fmt.Sprintf("%s.%d", jobID, index)
	task := task_queue.Task{
		TaskID:    taskID,
		TaskIndex: int64(index),
		JobID:     jobID,
		Status:    task_queue.StatusPending,
		Args:      fmt.Sprintf("param-%d", index),
		History: []*task_queue.TaskHistory{
			{
				Timestamp: float64(control.GetTimestampMillis()) / 1000.0,
				Status:    task_queue.StatusPending,
			},
		},
		Version: 0,
	}
	return &task
}

// test: populate lots of tasks.  Spawn a lot of threads which concurrently try to claim tasks and then mark them complete.
// afterwards, reconcile the task histories against the thread's logs of what they executed.
func runConcurrentClaims(t *testing.T, taskCount int, clientCount int) {
	projectID := "broad-achilles"
	jobID := "testjobid"

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, projectID)
	assert.Nil(t, err)

	// clear anything that may be left from old test
	deleteTasks(t, ctx, client, jobID)
	tasks := make([]*task_queue.Task, taskCount)
	for i := 0; i < taskCount; i++ {
		tasks[i] = newTask(jobID, i)
	}
	submitTasks(t, ctx, client, tasks)

	taskParamPerClient := make([][]string, clientCount)

	var done sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(1)

	for i := 0; i < clientCount; i++ {
		done.Add(1)
		spawnExecuteTasks(t, projectID, jobID, i, &ready, &done, taskParamPerClient)
	}

	// Release all the threads and let them run
	ready.Done()
	// wait for all threads to finish
	done.Wait()

	// now verify:
	// each task was executed only once
	taskExecCount := make(map[string]int)
	for i := 0; i < clientCount; i++ {
		for _, param := range taskParamPerClient[i] {
			if _, exists := taskExecCount[param]; !exists {
				taskExecCount[param] = 0
			}
			taskExecCount[param]++
		}
	}
	assert.Equal(t, taskCount, len(taskExecCount))
	for k, v := range taskExecCount {
		if v != 1 {
			log.Printf("ERROR: exec count for %v was %d\n", k, v)
		}
		assert.Equal(t, 1, v)
	}
}

func TestSimpleClaimTasks(t *testing.T) {
	runConcurrentClaims(t, 2, 1)
}

func TestConcurrentClaimTasks(t *testing.T) {
	runConcurrentClaims(t, 20, 5)
}

func TestLargeConcurrentClaimTasks(t *testing.T) {
	runConcurrentClaims(t, 1000, 50)
}
