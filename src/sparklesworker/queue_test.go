package sparklesworker

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/consumer"
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

func spawnExecuteTasks(t *testing.T, projectID string, jobID string, index int, ready *sync.WaitGroup, done *sync.WaitGroup, taskParamPerClient [][][]string) {
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, projectID)
	assert.Nil(t, err)

	workerID := fmt.Sprintf("thread-%d", index)
	cluster := "c"
	queue := task_queue.NewFirestoreQueue(client, cluster, workerID, 1*time.Second, 60*time.Second)

	run := func() {
		ready.Wait()
		executor := func(taskID string, taskSpec *task_queue.TaskSpec, expiry time.Time) (*consumer.ExecuteTaskResult, error) {
			// remember we executed this task
			taskParamPerClient[index] = append(taskParamPerClient[index], taskSpec.Command)
			log.Printf("client %d executed %s\n", index, taskSpec.Command)
			return &consumer.ExecuteTaskResult{RetCode: "0", TaskPaths: &consumer.TaskPaths{}}, nil
		}
		sleepFunc := func(sleepTime time.Duration) {
			time.Sleep(sleepTime)
		}
		err := consumer.RunLoop(ctx, cluster, queue, sleepFunc, executor, 1*time.Second, 10*time.Second)
		if err != nil {
			log.Printf("consumerRunLoop returned error: %v\n", err)
		}
		done.Done()
	}
	go run()
}

func deleteTasks(t *testing.T, ctx context.Context, client *firestore.Client, jobID string) {
	for {
		docs, err := client.Collection("Task").Where("job_id", "==", jobID).Limit(100).Documents(ctx).GetAll()
		assert.Nil(t, err)
		if len(docs) == 0 {
			break
		}
		log.Printf("Deleting %d tasks...", len(docs))
		wb := client.Batch()
		for _, doc := range docs {
			wb.Delete(doc.Ref)
		}
		_, err = wb.Commit(ctx)
		assert.Nil(t, err)
	}
}

func submitTasks(t *testing.T, ctx context.Context, client *firestore.Client, tasks []*task_queue.Task) {
	for i := 0; i < len(tasks); i += 500 {
		end := i + 500
		if end > len(tasks) {
			end = len(tasks)
		}
		wb := client.Batch()
		for _, task := range tasks[i:end] {
			docRef := client.Collection("Task").Doc(task.TaskID)
			wb.Set(docRef, task)
		}
		_, err := wb.Commit(ctx)
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
		TaskSpec:  &task_queue.TaskSpec{Command: []string{fmt.Sprintf("param-%d", index)}},
		History: []*task_queue.TaskHistory{
			{
				Timestamp: float64(backend.GetTimestampMillis()) / 1000.0,
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
	projectID := os.Getenv("GOOGLE_TEST_PROJECT_ID")

	jobID := "testjobid"

	ctx := context.Background()
	client, err := firestore.NewClient(ctx, projectID)
	assert.Nil(t, err)

	// clear anything that may be left from old test
	deleteTasks(t, ctx, client, jobID)
	tasks := make([]*task_queue.Task, taskCount)
	for i := 0; i < taskCount; i++ {
		tasks[i] = newTask(jobID, i)
	}
	submitTasks(t, ctx, client, tasks)

	taskParamPerClient := make([][][]string, clientCount)

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
			paramStr := strings.Join(param, " ")
			if _, exists := taskExecCount[paramStr]; !exists {
				taskExecCount[paramStr] = 0
			}
			taskExecCount[paramStr]++
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

func skipIf(t *testing.T) {
	projectID := os.Getenv("GOOGLE_TEST_PROJECT_ID")
	if projectID != "" {
		t.Skip("No env variable GOOGLE_TEST_PROJECT_ID set")
	}

}

// func TestSimpleClaimTasks(t *testing.T) {
// 	skipIf(t)
// 	runConcurrentClaims(t, 2, 1)
// }

// func TestConcurrentClaimTasks(t *testing.T) {
// 	skipIf(t)
// 	runConcurrentClaims(t, 20, 5)
// }

// func TestLargeConcurrentClaimTasks(t *testing.T) {
// 	skipIf(t)
// 	runConcurrentClaims(t, 1000, 50)
// }
