package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/task_queue"
)

const clusterCollection = "Cluster"
const taskCollection = task_queue.TaskCollection

type SparklesMethodsForPoll interface {
	updateClusterMonitorState(clusterID string, state *MonitorState) error
	getNonCompleteTaskCount(clusterID string) (int, error)
	getClaimedTasks(clusterID string) ([]*task_queue.Task, error)
	markTasksPending(tasks []*task_queue.Task) error
	getClusterConfig(clusterID string) (Cluster, error)
	getPendingTaskCount(clusterID string) (int, error)
	getTasksCompletedBy(batchJobID string) int
}

type FirestoreSparklesMethodsForPoll struct {
	client *firestore.Client
	ctx    context.Context
}

func (s *FirestoreSparklesMethodsForPoll) getClusterConfig(clusterID string) (Cluster, error) {
	docSnap, err := s.client.Collection(clusterCollection).Doc(clusterID).Get(s.ctx)
	if err != nil {
		return Cluster{}, fmt.Errorf("getting cluster %s: %w", clusterID, err)
	}
	var cluster Cluster
	if err := docSnap.DataTo(&cluster); err != nil {
		return Cluster{}, fmt.Errorf("decoding cluster %s: %w", clusterID, err)
	}
	return cluster, nil
}

func (s *FirestoreSparklesMethodsForPoll) updateClusterMonitorState(clusterID string, state *MonitorState) error {
	wire := monitorStateJSON{
		BatchJobRequests:        state.batchJobRequests,
		CompletedJobIds:         state.completedJobIds,
		SuspiciouslyFailedToRun: state.suspiciouslyFailedToRun,
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("marshaling monitor state: %w", err)
	}
	_, err = s.client.Collection(clusterCollection).Doc(clusterID).Update(s.ctx, []firestore.Update{
		{Path: "monitor_state", Value: string(data)},
	})
	return err
}

func (s *FirestoreSparklesMethodsForPoll) getClaimedTasks(clusterID string) ([]*task_queue.Task, error) {
	docs, err := s.client.Collection(taskCollection).
		Where("cluster_id", "==", clusterID).
		Where("status", "==", task_queue.StatusClaimed).
		Documents(s.ctx).GetAll()
	if err != nil {
		return nil, err
	}
	tasks := make([]*task_queue.Task, 0, len(docs))
	for _, doc := range docs {
		var t task_queue.Task
		if err := doc.DataTo(&t); err != nil {
			return nil, err
		}
		t.TaskID = doc.Ref.ID
		tasks = append(tasks, &t)
	}
	return tasks, nil
}

func (s *FirestoreSparklesMethodsForPoll) markTasksPending(tasks []*task_queue.Task) error {
	const batchSize = 500
	for i := 0; i < len(tasks); i += batchSize {
		end := i + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		wb := s.client.Batch()
		for _, task := range tasks[i:end] {
			docRef := s.client.Collection(taskCollection).Doc(task.TaskID)
			wb.Update(docRef, []firestore.Update{
				{Path: "status", Value: task_queue.StatusPending},
				{Path: "owned_by_worker_id", Value: ""},
			})
		}
		if _, err := wb.Commit(s.ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *FirestoreSparklesMethodsForPoll) getPendingTaskCount(clusterID string) (int, error) {
	docs, err := s.client.Collection(taskCollection).
		Where("cluster_id", "==", clusterID).
		Where("status", "==", task_queue.StatusPending).
		Documents(s.ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return len(docs), nil
}

func (s *FirestoreSparklesMethodsForPoll) getNonCompleteTaskCount(clusterID string) (int, error) {
	docs, err := s.client.Collection(taskCollection).
		Where("cluster_id", "==", clusterID).
		Where("status", "not-in", []string{task_queue.StatusComplete, task_queue.StatusFailed, task_queue.StatusKilled}).
		Documents(s.ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return len(docs), nil
}

func (s *FirestoreSparklesMethodsForPoll) getTasksCompletedBy(batchJobID string) int {
	docs, err := s.client.Collection(taskCollection).
		Where("owned_by_batch_job_id", "==", batchJobID).
		Where("status", "==", task_queue.StatusComplete).
		Documents(s.ctx).GetAll()
	if err != nil {
		log.Printf("getTasksCompletedBy(%s): %v", batchJobID, err)
		return 0
	}
	return len(docs)
}
