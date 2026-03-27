package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/task_queue"
)

const clusterCollection = "Cluster"
const taskCollection = task_queue.TaskCollection

type FirestoreSparklesMethodsForPoll struct {
	client *firestore.Client
	ctx    context.Context
}

func (s *FirestoreSparklesMethodsForPoll) GetClusterConfig(clusterID string) (*backend.Cluster, error) {
	docSnap, err := s.client.Collection(clusterCollection).Doc(clusterID).Get(s.ctx)
	if err != nil {
		return nil, fmt.Errorf("getting cluster %s: %w", clusterID, err)
	}
	var cluster backend.Cluster
	if err := docSnap.DataTo(&cluster); err != nil {
		return nil, fmt.Errorf("decoding cluster %s: %w", clusterID, err)
	}
	return &cluster, nil
}

func (s *FirestoreSparklesMethodsForPoll) SetClusterConfig(clusterID string, cluster backend.Cluster) error {
	_, err := s.client.Collection(clusterCollection).Doc(clusterID).Set(s.ctx, cluster)
	return err
}

func (s *FirestoreSparklesMethodsForPoll) UpdateClusterMonitorState(clusterID string, state *backend.MonitorState) error {
	wire := backend.MonitorStateJSON{
		BatchJobRequests:        state.BatchJobRequests,
		CompletedJobIds:         state.CompletedJobIds,
		SuspiciouslyFailedToRun: state.SuspiciouslyFailedToRun,
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

func (s *FirestoreSparklesMethodsForPoll) GetClaimedTasks(clusterID string) ([]*task_queue.Task, error) {
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

func (s *FirestoreSparklesMethodsForPoll) MarkTasksPending(tasks []*task_queue.Task) error {
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

func (s *FirestoreSparklesMethodsForPoll) GetPendingTaskCount(clusterID string) (int, error) {
	docs, err := s.client.Collection(taskCollection).
		Where("cluster_id", "==", clusterID).
		Where("status", "==", task_queue.StatusPending).
		Documents(s.ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return len(docs), nil
}

func (s *FirestoreSparklesMethodsForPoll) GetNonCompleteTaskCount(clusterID string) (int, error) {
	docs, err := s.client.Collection(taskCollection).
		Where("cluster_id", "==", clusterID).
		Where("status", "not-in", []string{task_queue.StatusComplete, task_queue.StatusFailed, task_queue.StatusKilled}).
		Documents(s.ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return len(docs), nil
}

func (s *FirestoreSparklesMethodsForPoll) GetActiveClusterIDs() ([]string, error) {
	docs, err := s.client.Collection(taskCollection).
		Where("status", "in", []string{task_queue.StatusClaimed, task_queue.StatusPending}).
		Documents(s.ctx).GetAll()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for _, doc := range docs {
		var t task_queue.Task
		if err := doc.DataTo(&t); err != nil {
			return nil, err
		}
		seen[t.ClusterID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *FirestoreSparklesMethodsForPoll) GetTasksCompletedBy(batchJobID string) int {
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
