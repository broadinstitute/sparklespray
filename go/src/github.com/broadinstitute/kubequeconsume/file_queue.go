package kubequeconsume

import (
	"encoding/json"
	"io/ioutil"
	"log"

	"golang.org/x/net/context"
)

type memQueue struct {
	tasks          []*Task
	currentIndex   int
	retrievedTasks map[string]*Task
}

func (q *memQueue) claimTask(ctx context.Context) (*Task, error) {
	if q.currentIndex >= len(q.tasks) {
		return nil, nil
	}

	t := q.tasks[q.currentIndex]
	q.currentIndex++

	q.retrievedTasks[t.TaskID] = t

	return t, nil
}

func (q *memQueue) isJobKilled(ctx context.Context, JobID string) (bool, error) {
	return false, nil
}

func (q *memQueue) atomicUpdateTask(ctx context.Context, task_id string, mutateTaskCallback func(task *Task) bool) (*Task, error) {
	t := q.retrievedTasks[task_id]
	ok := mutateTaskCallback(t)
	if !ok {
		log.Fatalf("mutation failed")
	}
	return t, nil
}

func CreatePreloadedQueue(filename string) (*memQueue, error) {
	var tasks []*Task

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &tasks)
	if err != nil {
		return nil, err
	}

	return &memQueue{tasks: tasks, currentIndex: 0, retrievedTasks: make(map[string]*Task)}, nil
}
