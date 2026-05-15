package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	batch "cloud.google.com/go/batch/apiv1"
	batchpb "cloud.google.com/go/batch/apiv1/batchpb"
	compute "cloud.google.com/go/compute/apiv1"
	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	iamcredentials "google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const EventCollection = "SparklesV6Event"
const ClusterCollection = "SparklesV6Cluster"
const JobCollection = "SparklesV6Job"
const TaskCollection = "SparklesV6Task"
const SummaryCollection = "SparklesV6JobSummary"
const ClusterStatusCollection = "SparklesV6ClusterStatus"
const DefaultLimit = 1000
const MaxLimit = 10000

const topicLifecycle = "sparkles-v6-events"
const topicTaskOut = "sparkles-v6-task-out"
const topicTaskIn = "sparkles-v6-task-in"

var dsClient *datastore.Client
var psClient *pubsub.Client
var iamSvc *iamcredentials.Service
var subscriberSA string
var gProjectID string

// Event is a flexible map that serialises cleanly to the JSON shape the spec requires.
type Event map[string]any

type Cluster struct {
	ClusterID   string    `datastore:"cluster_id"   json:"cluster_id"`
	MachineType string    `datastore:"machine_type" json:"machine_type"`
	CreatedAt   time.Time `datastore:"created_at"   json:"created_at"`
	Region      string    `datastore:"region"       json:"region"`
	LastUpdated time.Time `datastore:"last_updated" json:"last_updated"`
	Expiry      time.Time `datastore:"expiry"       json:"-"`
}

type ClusterStatus struct {
	ClusterID                   string    `datastore:"cluster_id"                     json:"clusterId"`
	LastUpdate                  time.Time `datastore:"last_update"                    json:"lastUpdate"`
	SubmittedWorkerRequests     int       `datastore:"submitted_worker_requests"      json:"submittedWorkerRequests"`
	ShortFailedWorkerRequests   int       `datastore:"short_failed_worker_requests"   json:"shortFailedWorkerRequests"`
	OtherFailedWorkerRequests   int       `datastore:"other_failed_worker_requests"   json:"otherFailedWorkerRequests"`
	CompletedWorkerRequests     int       `datastore:"completed_worker_requests"      json:"completedWorkerRequests"`
	SeenCompletions             []string  `datastore:"seen_completions,noindex"       json:"seenCompletions"`
	InstanceInUseCount          int       `datastore:"instance_in_use_count"          json:"instanceInUseCount"`
	OrphanedTaskCount           int       `datastore:"orphaned_task_count"            json:"orphanedTaskCount"`
	IdleInstanceCount           int       `datastore:"idle_instance_count"            json:"idleInstanceCount"`
	RunningTaskCount            int       `datastore:"running_task_count"             json:"runningTaskCount"`
	PreemptableInstanceCount    int       `datastore:"preemptable_instance_count"     json:"preemptableInstanceCount"`
	NonPreemptableInstanceCount int       `datastore:"non_preemptable_instance_count" json:"nonPreemptableInstanceCount"`
	Expiry                      time.Time `datastore:"expiry"                         json:"-"`
}

type ClusterMonitor struct {
	projectID       string
	dsClient        *datastore.Client
	instancesClient *compute.InstancesClient
	zonesClient     *compute.ZonesClient
	batchClient     *batch.Client
	zonesCache      map[string][]string
}

type Job struct {
	JobID                  string    `datastore:"job_id" json:"job_id"`
	Tasks                  []string  `datastore:"tasks,noindex" json:"tasks,omitempty"`
	KubeJobSpec            string    `datastore:"kube_job_spec,noindex" json:"kube_job_spec,omitempty"`
	Metadata               string    `datastore:"metadata,noindex" json:"-"`
	ClusterID              string    `datastore:"cluster_id" json:"cluster_id"`
	Status                 string    `datastore:"status" json:"status"`
	SubmitTime             time.Time `datastore:"submit_time" json:"submit_time"`
	TaskCount              int32     `datastore:"task_count" json:"task_count"`
	MaxPreemptableAttempts int32     `datastore:"max_preemptable_attempts" json:"max_preemptable_attempts"`
	TargetNodeCount        int32     `datastore:"target_node_count" json:"target_node_count"`
}

func (j *Job) MarshalJSON() ([]byte, error) {
	type Alias Job
	var metadataRaw json.RawMessage
	if j.Metadata != "" {
		if err := json.Unmarshal([]byte(j.Metadata), &metadataRaw); err != nil {
			metadataRaw = nil
		}
	}
	return json.Marshal(&struct {
		*Alias
		Metadata json.RawMessage `json:"metadata,omitempty"`
	}{
		Alias:    (*Alias)(j),
		Metadata: metadataRaw,
	})
}

type TaskHistory struct {
	Timestamp     float64 `datastore:"timestamp,noindex" json:"timestamp"`
	Status        string  `datastore:"status,noindex" json:"status"`
	FailureReason string  `datastore:"failure_reason,noindex,omitempty" json:"failure_reason,omitempty"`
	Owner         string  `datastore:"owner,noindex,omitempty" json:"owner,omitempty"`
}

type Task struct {
	TaskID           string         `datastore:"task_id" json:"task_id"`
	TaskIndex        int64          `datastore:"task_index" json:"task_index"`
	JobID            string         `datastore:"job_id" json:"job_id"`
	Status           string         `datastore:"status" json:"status"`
	Owner            string         `datastore:"owner" json:"owner,omitempty"`
	Args             string         `datastore:"args,noindex" json:"args,omitempty"`
	History          []*TaskHistory `datastore:"history,noindex" json:"history"`
	CommandResultURL string         `datastore:"command_result_url,noindex" json:"command_result_url,omitempty"`
	FailureReason    string         `datastore:"failure_reason,omitempty" json:"failure_reason,omitempty"`
	Version          int32          `datastore:"version" json:"version"`
	ExitCode         string         `datastore:"exit_code" json:"exit_code,omitempty"`
	ClusterID        string         `datastore:"cluster_id" json:"cluster_id"`
	MonitorAddress   string         `datastore:"monitor_address,noindex" json:"monitor_address,omitempty"`
	LogURL           string         `datastore:"log_url,noindex" json:"log_url,omitempty"`
	LastUpdated      time.Time      `datastore:"last_updated" json:"last_updated"`
	Command          string         `datastore:"command,noindex" json:"command"`
	DockerImage      string         `datastore:"docker_image,noindex" json:"docker_image"`
}

type JobSummary struct {
	JobID        string    `datastore:"job_id"        json:"jobID"`
	SubmitTime   time.Time `datastore:"submit_time"   json:"submitTime"`
	ClusterID    string    `datastore:"cluster_id"    json:"clusterId"`
	Expiry       time.Time `datastore:"expiry"        json:"-"`
	TaskCount    int       `datastore:"task_count"    json:"taskCount"`
	SuccessCount int       `datastore:"success_count" json:"successCount"`
	FailureCount int       `datastore:"failure_count" json:"failureCount"`
}

// SubscriptionResponse is returned by all subscription-creation endpoints.
type SubscriptionResponse struct {
	SubscriptionID     string `json:"subscription_id"`
	PullURL            string `json:"pull_url"`
	AckURL             string `json:"ack_url"`
	AuthorizationToken string `json:"authorization_token"`
}

func propertyListToEvent(pl datastore.PropertyList) Event {
	e := make(Event, len(pl))
	for _, p := range pl {
		name := p.Name
		if name == "event_id" {
			name = "id"
		}
		if t, ok := p.Value.(time.Time); ok {
			e[name] = t.UTC().Format(time.RFC3339Nano)
		} else {
			e[name] = p.Value
		}
	}
	return e
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, httpCode int, code, msg string) {
	writeJSON(w, httpCode, map[string]string{"error": msg, "code": code})
}

func parseTimestamp(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func runQuery(ctx context.Context, w http.ResponseWriter, q *datastore.Query) ([]datastore.PropertyList, bool) {
	var plists []datastore.PropertyList
	if _, err := dsClient.GetAll(ctx, q, &plists); err != nil {
		log.Printf("Datastore query error: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore query failed")
		return nil, false
	}
	return plists, true
}

// newID returns 24 random hex characters suitable for use in a Pub/Sub subscription name.
func newID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// buildTypeFilter converts a comma-separated types string into a Pub/Sub filter expression.
func buildTypeFilter(types string) string {
	if types == "" {
		return ""
	}
	var clauses []string
	for _, t := range strings.Split(types, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			clauses = append(clauses, fmt.Sprintf("attributes.type = %q", t))
		}
	}
	if len(clauses) == 0 {
		return ""
	}
	return strings.Join(clauses, " OR ")
}

// generateSubscriberToken returns a short-lived access token for the subscriber service account,
// scoped only to Pub/Sub pull/ack operations.
func generateSubscriberToken(ctx context.Context) (string, error) {
	name := "projects/-/serviceAccounts/" + subscriberSA
	resp, err := iamSvc.Projects.ServiceAccounts.GenerateAccessToken(name,
		&iamcredentials.GenerateAccessTokenRequest{
			Scope: []string{"https://www.googleapis.com/auth/pubsub"},
		}).Context(ctx).Do()
	if err != nil {
		return "", err
	}
	return resp.AccessToken, nil
}

// createPubSubSubscription creates a new Pub/Sub subscription with a 24-hour self-expiry.
func createPubSubSubscription(ctx context.Context, topicName, filter string) (string, error) {
	subName := "sparkles-" + newID()
	fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", gProjectID, subName)
	fullTopicName := fmt.Sprintf("projects/%s/topics/%s", gProjectID, topicName)
	ttl := durationpb.New(24 * time.Hour)
	_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{
		Name:                     fullSubName,
		Topic:                    fullTopicName,
		AckDeadlineSeconds:       10,
		MessageRetentionDuration: ttl,
		ExpirationPolicy:         &pb.ExpirationPolicy{Ttl: ttl},
		Filter:                   filter,
	})
	if err != nil {
		return "", err
	}
	return subName, nil
}

func subscriptionURLs(subID string) (pullURL, ackURL string) {
	base := fmt.Sprintf("https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s", gProjectID, subID)
	return base + ":pull", base + ":acknowledge"
}

func buildSubscriptionResponse(ctx context.Context, subID string) (*SubscriptionResponse, error) {
	token, err := generateSubscriberToken(ctx)
	if err != nil {
		return nil, err
	}
	pullURL, ackURL := subscriptionURLs(subID)
	return &SubscriptionResponse{
		SubscriptionID:     subID,
		PullURL:            pullURL,
		AckURL:             ackURL,
		AuthorizationToken: token,
	}, nil
}

func handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	filter := buildTypeFilter(r.URL.Query().Get("types"))

	subID, err := createPubSubSubscription(ctx, topicLifecycle, filter)
	if err != nil {
		log.Printf("Failed to create lifecycle subscription: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create subscription")
		return
	}

	log.Printf("Created subscription %s", subID)

	resp, err := buildSubscriptionResponse(ctx, subID)
	if err != nil {
		log.Printf("Failed to generate subscriber token: %v", err)
		fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", gProjectID, subID)
		psClient.SubscriptionAdminClient.DeleteSubscription(context.Background(), &pb.DeleteSubscriptionRequest{Subscription: fullSubName})
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate token")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	subID := r.PathValue("subscription_id")
	fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", gProjectID, subID)
	if err := psClient.SubscriptionAdminClient.DeleteSubscription(ctx, &pb.DeleteSubscriptionRequest{Subscription: fullSubName}); err != nil {
		log.Printf("Failed to delete subscription %q: %v", subID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete subscription")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleCreateTaskSubscription(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	taskID := r.PathValue("task_id")

	filter := fmt.Sprintf("attributes.task_id = %q", taskID)
	if typeFilter := buildTypeFilter(r.URL.Query().Get("types")); typeFilter != "" {
		filter = filter + " AND (" + typeFilter + ")"
	}

	subID, err := createPubSubSubscription(ctx, topicTaskOut, filter)
	if err != nil {
		log.Printf("Failed to create task subscription for %q: %v", taskID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create subscription")
		return
	}

	log.Printf("Created task subscription %s", subID)

	resp, err := buildSubscriptionResponse(ctx, subID)
	if err != nil {
		log.Printf("Failed to generate subscriber token: %v", err)
		fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", gProjectID, subID)
		psClient.SubscriptionAdminClient.DeleteSubscription(context.Background(), &pb.DeleteSubscriptionRequest{Subscription: fullSubName})
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate token")
		return
	}

	// Fire-and-forget: tell the task to start publishing.
	go func() {
		pubCtx := context.Background()
		reqID := newID()
		data, _ := json.Marshal(map[string]string{
			"type":    "start_publishing",
			"req_id":  reqID,
			"task_id": taskID,
		})
		publisher := psClient.Publisher(topicTaskIn)
		res := publisher.Publish(pubCtx, &pubsub.Message{Data: data})
		publisher.Stop()
		if _, err := res.Get(pubCtx); err != nil {
			log.Printf("Failed to publish start_publishing for task %q: %v", taskID, err)
		}
	}()

	writeJSON(w, http.StatusOK, resp)
}

func handleTaskUnsubscribe(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	subID := r.PathValue("subscription_id")
	fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", gProjectID, subID)
	if err := psClient.SubscriptionAdminClient.DeleteSubscription(ctx, &pb.DeleteSubscriptionRequest{Subscription: fullSubName}); err != nil {
		log.Printf("Failed to delete task subscription %q: %v", subID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete subscription")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	limit := DefaultLimit
	if ls := q.Get("limit"); ls != "" {
		n, err := strconv.Atoi(ls)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("invalid limit: %q", ls))
			return
		}
		if n > MaxLimit {
			n = MaxLimit
		}
		limit = n
	}

	dq := datastore.NewQuery(EventCollection).Order("timestamp").Limit(limit)

	if s := q.Get("after"); s != "" {
		t, err := parseTimestamp(s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("invalid timestamp: %q", s))
			return
		}
		dq = dq.FilterField("timestamp", ">", t)
	}
	if s := q.Get("before"); s != "" {
		t, err := parseTimestamp(s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("invalid timestamp: %q", s))
			return
		}
		dq = dq.FilterField("timestamp", "<=", t)
	}
	if jobID := q.Get("job_id"); jobID != "" {
		dq = dq.FilterField("job_id", "=", jobID)
	}
	if clusterID := q.Get("cluster_id"); clusterID != "" {
		dq = dq.FilterField("cluster_id", "=", clusterID)
	}
	if ts := q.Get("types"); ts != "" {
		var types []any
		for _, t := range strings.Split(ts, ",") {
			if t = strings.TrimSpace(t); t != "" {
				types = append(types, t)
			}
		}
		if len(types) == 1 {
			dq = dq.FilterField("type", "=", types[0])
		} else if len(types) > 1 {
			dq = dq.FilterField("type", "in", types)
		}
	}

	plists, ok := runQuery(ctx, w, dq)
	if !ok {
		return
	}

	events := make([]Event, 0, len(plists))
	var nextAfter string
	for _, pl := range plists {
		e := propertyListToEvent(pl)
		events = append(events, e)
		if ts, ok := e["timestamp"].(string); ok {
			nextAfter = ts
		}
	}

	resp := map[string]any{"events": events}
	if nextAfter != "" {
		resp["next_after"] = nextAfter
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	taskID := r.PathValue("task_id")
	var task Task
	key := datastore.NameKey(TaskCollection, taskID, nil)
	if err := dsClient.Get(ctx, key, &task); err != nil {
		if errors.Is(err, datastore.ErrNoSuchEntity) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("task %q not found", taskID))
		} else {
			log.Printf("Datastore get error for task %q: %v", taskID, err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore get failed")
		}
		return
	}
	if task.Command == "" {
		task.Command = "missing"
	}
	if task.DockerImage == "" {
		task.DockerImage = "missing"
	}
	writeJSON(w, http.StatusOK, &task)
}

func handleTaskLog(w http.ResponseWriter, r *http.Request) {
	after := r.URL.Query().Get("after")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	content := ""
	if after == "" {
		content = "not yet implemented"
	}
	writeJSON(w, http.StatusOK, map[string]string{"content": content, "next_after": now})
}

func handleTaskMetrics(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	writeJSON(w, http.StatusOK, map[string]any{"metrics": []any{}, "next_after": now})
}

func handleCluster(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterID := r.PathValue("cluster_id")
	var cluster Cluster
	key := datastore.NameKey(ClusterCollection, clusterID, nil)
	if err := dsClient.Get(ctx, key, &cluster); err != nil {
		if errors.Is(err, datastore.ErrNoSuchEntity) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("cluster %q not found", clusterID))
		} else {
			log.Printf("Datastore get error for cluster %q: %v", clusterID, err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore get failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, &cluster)
}

func handleJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("job_id")
	var job Job
	key := datastore.NameKey(JobCollection, jobID, nil)
	if err := dsClient.Get(ctx, key, &job); err != nil {
		if errors.Is(err, datastore.ErrNoSuchEntity) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("job %q not found", jobID))
		} else {
			log.Printf("Datastore get error for job %q: %v", jobID, err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore get failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, &job)
}

// gcCollection deletes all entities in the given collection whose expiry field is <= now.
// Returns the number of entities deleted.
func gcCollection(ctx context.Context, collection string) (int, error) {
	q := datastore.NewQuery(collection).FilterField("expiry", "<=", time.Now()).KeysOnly()
	keys, err := dsClient.GetAll(ctx, q, nil)
	if err != nil {
		return 0, fmt.Errorf("gc query %q: %w", collection, err)
	}
	const batchSize = 500
	deleted := 0
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		if err := dsClient.DeleteMulti(ctx, keys[i:end]); err != nil {
			return deleted, fmt.Errorf("gc delete %q: %w", collection, err)
		}
		deleted += end - i
	}
	return deleted, nil
}

var gcCollections = []string{
	EventCollection,
	ClusterCollection,
	ClusterStatusCollection,
	SummaryCollection,
}

func handleGC(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	totalDeleted := 0
	for _, col := range gcCollections {
		n, err := gcCollection(ctx, col)
		totalDeleted += n
		if err != nil {
			log.Printf("GC error: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "gc failed")
			return
		}
	}
	log.Printf("GC deleted %d expired entities", totalDeleted)
	writeJSON(w, http.StatusOK, map[string]int{"deleted": totalDeleted})
}

func startGCWorker(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(15 * time.Minute):
			}

			totalDeleted := 0
			for _, col := range gcCollections {
				n, err := gcCollection(ctx, col)
				totalDeleted += n
				if err != nil {
					log.Printf("GC worker error: %v", err)
				}
			}
			if totalDeleted > 0 {
				log.Printf("GC worker deleted %d expired entities", totalDeleted)
			}
		}
	}()
}

func recomputeJobSummary(ctx context.Context, jobID string) error {
	var job Job
	jobKey := datastore.NameKey(JobCollection, jobID, nil)
	if err := dsClient.Get(ctx, jobKey, &job); err != nil {
		return fmt.Errorf("get job %q: %w", jobID, err)
	}

	dq := datastore.NewQuery(TaskCollection).FilterField("job_id", "=", jobID)
	var tasks []Task
	if _, err := dsClient.GetAll(ctx, dq, &tasks); err != nil {
		return fmt.Errorf("get tasks for job %q: %w", jobID, err)
	}

	summary := JobSummary{
		JobID:      jobID,
		SubmitTime: job.SubmitTime,
		ClusterID:  job.ClusterID,
		Expiry:     time.Now().Add(7 * 24 * time.Hour),
		TaskCount:  len(tasks),
	}
	for _, t := range tasks {
		if t.Status == "complete" && t.ExitCode == "0" {
			summary.SuccessCount++
		} else if t.Status == "complete" || t.Status == "failed" {
			summary.FailureCount++
		}
	}

	summaryKey := datastore.NameKey(SummaryCollection, jobID, nil)
	_, err := dsClient.Put(ctx, summaryKey, &summary)
	return err
}

func handleJobsSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dq := datastore.NewQuery(SummaryCollection).Order("submit_time")
	if s := r.URL.Query().Get("after"); s != "" {
		t, err := parseTimestamp(s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("invalid timestamp: %q", s))
			return
		}
		dq = dq.FilterField("submit_time", ">", t)
	}
	if s := r.URL.Query().Get("before"); s != "" {
		t, err := parseTimestamp(s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("invalid timestamp: %q", s))
			return
		}
		dq = dq.FilterField("submit_time", "<=", t)
	}
	summaries := make([]JobSummary, 0)
	if _, err := dsClient.GetAll(ctx, dq, &summaries); err != nil {
		log.Printf("Datastore query error for summaries: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore query failed")
		return
	}
	writeJSON(w, http.StatusOK, summaries)
}

func startSummaryUpdater(ctx context.Context) {
	subName := "sparkles-dashboard-summaries"
	fullSubName := fmt.Sprintf("projects/%s/subscriptions/%s", gProjectID, subName)
	fullTopicName := fmt.Sprintf("projects/%s/topics/%s", gProjectID, topicLifecycle)
	ttl := durationpb.New(7 * 24 * time.Hour)
	_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{
		Name:               fullSubName,
		Topic:              fullTopicName,
		AckDeadlineSeconds: 60,
		Filter:             `attributes:job_id`,
		ExpirationPolicy:   &pb.ExpirationPolicy{Ttl: ttl},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Printf("Summary updater: could not create subscription: %v", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			resp, err := psClient.SubscriptionAdminClient.Pull(ctx, &pb.PullRequest{
				Subscription: fullSubName,
				MaxMessages:  100,
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Summary updater: pull error: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			if len(resp.ReceivedMessages) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			jobIDs := make(map[string]struct{})
			ackIDs := make([]string, 0, len(resp.ReceivedMessages))
			for _, m := range resp.ReceivedMessages {
				ackIDs = append(ackIDs, m.AckId)
				if jobID := m.Message.Attributes["job_id"]; jobID != "" {
					jobIDs[jobID] = struct{}{}
				}
			}

			if err := psClient.SubscriptionAdminClient.Acknowledge(ctx, &pb.AcknowledgeRequest{
				Subscription: fullSubName,
				AckIds:       ackIDs,
			}); err != nil {
				log.Printf("Summary updater: acknowledge error: %v", err)
			}

			for jobID := range jobIDs {
				if err := recomputeJobSummary(ctx, jobID); err != nil {
					log.Printf("Summary updater: failed to recompute summary for job %q: %v", jobID, err)
				}
			}
		}
	}()
}

func isBatchJobTerminal(j *batchpb.Job) bool {
	s := j.GetStatus().GetState()
	return s == batchpb.JobStatus_SUCCEEDED ||
		s == batchpb.JobStatus_FAILED ||
		s == batchpb.JobStatus_CANCELLED
}

func (m *ClusterMonitor) zonesForRegion(ctx context.Context, region string) ([]string, error) {
	if zones, ok := m.zonesCache[region]; ok {
		return zones, nil
	}
	zonesFilter := fmt.Sprintf(`name:"%s-*"`, region)
	it := m.zonesClient.List(ctx, &computepb.ListZonesRequest{
		Project: m.projectID,
		Filter:  &zonesFilter,
	})
	var zones []string
	for {
		z, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list zones for region %q: %w", region, err)
		}
		zones = append(zones, z.GetName())
	}
	m.zonesCache[region] = zones
	return zones, nil
}

func (m *ClusterMonitor) poll(ctx context.Context, cluster Cluster) error {
	var status ClusterStatus
	key := datastore.NameKey(ClusterStatusCollection, cluster.ClusterID, nil)
	err := m.dsClient.Get(ctx, key, &status)
	if err == datastore.ErrNoSuchEntity {
		status = ClusterStatus{ClusterID: cluster.ClusterID}
	} else if err != nil {
		return fmt.Errorf("get ClusterStatus for %q: %w", cluster.ClusterID, err)
	}

	// Fetch tasks for this cluster.
	var tasks []Task
	dq := datastore.NewQuery(TaskCollection).FilterField("cluster_id", "=", cluster.ClusterID)
	if _, err := m.dsClient.GetAll(ctx, dq, &tasks); err != nil {
		return fmt.Errorf("query tasks for %q: %w", cluster.ClusterID, err)
	}

	// Fetch running GCE instances per zone and Cloud Batch jobs.
	// Skipped for local clusters, which have no real region or GCP resources.
	vmNames := make(map[string]bool) // "zone/instanceName" → isPreemptable
	var batchJobs []*batchpb.Job
	if cluster.Region != "local" {
		zones, err := m.zonesForRegion(ctx, cluster.Region)
		if err != nil {
			return err
		}
		instanceFilter := fmt.Sprintf(`labels.sparkles-cluster="%s"`, cluster.ClusterID)
		for _, zone := range zones {
			it := m.instancesClient.List(ctx, &computepb.ListInstancesRequest{
				Project: m.projectID,
				Zone:    zone,
				Filter:  &instanceFilter,
			})
			for {
				inst, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return fmt.Errorf("list instances zone %q: %w", zone, err)
				}
				isSpot := inst.GetScheduling().GetProvisioningModel() == "SPOT"
				vmNames[zone+"/"+inst.GetName()] = isSpot
			}
		}

		parent := fmt.Sprintf("projects/%s/locations/%s", m.projectID, cluster.Region)
		batchFilter := fmt.Sprintf(`labels.sparkles-cluster = "%s"`, cluster.ClusterID)
		bit := m.batchClient.ListJobs(ctx, &batchpb.ListJobsRequest{
			Parent: parent,
			Filter: batchFilter,
		})
		for {
			job, err := bit.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return fmt.Errorf("error listing batch jobs in %s where labels.sparkles-cluster=\"%s\": %w", parent, cluster.ClusterID, err)
			}
			batchJobs = append(batchJobs, job)
		}
	}
	status.SubmittedWorkerRequests = len(batchJobs)

	// Identify new completions.
	allBatchJobNames := make(map[string]struct{}, len(batchJobs))
	for _, j := range batchJobs {
		allBatchJobNames[j.GetName()] = struct{}{}
	}
	seenSet := make(map[string]struct{}, len(status.SeenCompletions))
	for _, id := range status.SeenCompletions {
		seenSet[id] = struct{}{}
	}
	var newCompletions []*batchpb.Job
	for _, j := range batchJobs {
		if !isBatchJobTerminal(j) {
			continue
		}
		if _, seen := seenSet[j.GetName()]; seen {
			continue
		}
		newCompletions = append(newCompletions, j)
		seenSet[j.GetName()] = struct{}{}
	}
	// Prune IDs no longer present in the live query, then append new ones.
	kept := status.SeenCompletions[:0]
	for _, id := range status.SeenCompletions {
		if _, exists := allBatchJobNames[id]; exists {
			kept = append(kept, id)
		}
	}
	for _, j := range newCompletions {
		kept = append(kept, j.GetName())
	}
	status.SeenCompletions = kept

	// Classify new completions.
	for _, j := range newCompletions {
		runtime := j.GetStatus().GetRunDuration().AsDuration()
		isShort := runtime < 10*time.Second
		isSuccess := j.GetStatus().GetState() == batchpb.JobStatus_SUCCEEDED
		if isShort {
			status.ShortFailedWorkerRequests++
		} else if isSuccess {
			status.CompletedWorkerRequests++
		} else {
			status.OtherFailedWorkerRequests++
		}
	}

	// Match VMs with claimed tasks.
	claimedByOwner := make(map[string]*Task)
	for i := range tasks {
		if tasks[i].Status == "claimed" {
			claimedByOwner[tasks[i].Owner] = &tasks[i]
		}
	}

	// Compute instance/task counts.
	status.InstanceInUseCount = 0
	status.IdleInstanceCount = 0
	status.PreemptableInstanceCount = 0
	status.NonPreemptableInstanceCount = 0
	for vmKey, isSpot := range vmNames {
		if isSpot {
			status.PreemptableInstanceCount++
		} else {
			status.NonPreemptableInstanceCount++
		}
		if _, hasClaimed := claimedByOwner[vmKey]; hasClaimed {
			status.InstanceInUseCount++
		} else {
			status.IdleInstanceCount++
		}
	}
	status.RunningTaskCount = len(claimedByOwner)
	status.OrphanedTaskCount = 0
	for owner := range claimedByOwner {
		if _, alive := vmNames[owner]; !alive {
			status.OrphanedTaskCount++
		}
	}

	status.LastUpdate = time.Now()
	status.Expiry = status.LastUpdate.Add(7 * 24 * time.Hour)
	if _, err := m.dsClient.Put(ctx, key, &status); err != nil {
		return fmt.Errorf("put ClusterStatus for %q: %w", cluster.ClusterID, err)
	}
	return nil
}

func (m *ClusterMonitor) start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			cutoff := time.Now().Add(-24 * time.Hour)
			dq := datastore.NewQuery(ClusterCollection).FilterField("last_updated", ">", cutoff)
			var clusters []Cluster
			if _, err := m.dsClient.GetAll(ctx, dq, &clusters); err != nil {
				log.Printf("ClusterHealthMonitor: failed to list active clusters: %v", err)
			} else {
				for _, c := range clusters {
					if err := m.poll(ctx, c); err != nil {
						log.Printf("ClusterHealthMonitor: poll(%q) error: %v", c.ClusterID, err)
					}
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(60 * time.Second):
			}
		}
	}()
}

func handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterID := r.PathValue("cluster_id")
	var status ClusterStatus
	key := datastore.NameKey(ClusterStatusCollection, clusterID, nil)
	if err := dsClient.Get(ctx, key, &status); errors.Is(err, datastore.ErrNoSuchEntity) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "no status for cluster")
		return
	} else if err != nil {
		log.Printf("Datastore get error for cluster status %q: %v", clusterID, err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore get failed")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func handleClusters(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusters := make([]Cluster, 0)
	if _, err := dsClient.GetAll(ctx, datastore.NewQuery(ClusterCollection), &clusters); err != nil {
		log.Printf("Datastore query error for clusters: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore query failed")
		return
	}
	writeJSON(w, http.StatusOK, clusters)
}

func handleClusterStatuses(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	statuses := make([]ClusterStatus, 0)
	if _, err := dsClient.GetAll(ctx, datastore.NewQuery(ClusterStatusCollection), &statuses); err != nil {
		log.Printf("Datastore query error for cluster statuses: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "datastore query failed")
		return
	}
	writeJSON(w, http.StatusOK, statuses)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func loadSubscriberSA(ctx context.Context) (string, error) {
	key := datastore.NameKey("SparklesV6ProjectConfig", "SparklesV6ProjectConfig", nil)
	var props datastore.PropertyList
	if err := dsClient.Get(ctx, key, &props); err != nil {
		return "", fmt.Errorf("get SparklesV6ProjectConfig: %w", err)
	}
	for _, p := range props {
		if p.Name == "dashboard_user_service_account" {
			if v, ok := p.Value.(string); ok && v != "" {
				return v, nil
			}
		}
	}
	return "", fmt.Errorf("dashboard_user_service_account not set in SparklesV6ProjectConfig")
}

func main() {
	projectID := flag.String("project", "", "GCP project ID (required)")
	addr := flag.String("addr", ":8080", "Listen address")
	flag.Parse()

	if *projectID == "" {
		log.Fatal("--project is required")
	}

	gProjectID = *projectID

	ctx := context.Background()
	var err error

	dsClient, err = datastore.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("Failed to create Datastore client: %v", err)
	}
	defer dsClient.Close()

	subscriberSA, err = loadSubscriberSA(ctx)
	if err != nil {
		log.Fatalf("Failed to load subscriber service account: %v", err)
	}
	log.Printf("Using subscriber SA: %s", subscriberSA)

	psClient, err = pubsub.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer psClient.Close()

	iamSvc, err = iamcredentials.NewService(ctx)
	if err != nil {
		log.Fatalf("Failed to create IAM credentials service: %v", err)
	}

	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create Compute instances client: %v", err)
	}
	defer instancesClient.Close()

	zonesClient, err := compute.NewZonesRESTClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create Compute zones client: %v", err)
	}
	defer zonesClient.Close()

	batchClient, err := batch.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create Batch client: %v", err)
	}
	defer batchClient.Close()

	monitor := &ClusterMonitor{
		projectID:       *projectID,
		dsClient:        dsClient,
		instancesClient: instancesClient,
		zonesClient:     zonesClient,
		batchClient:     batchClient,
		zonesCache:      make(map[string][]string),
	}

	startSummaryUpdater(ctx)
	monitor.start(ctx)
	startGCWorker(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs/summary", handleJobsSummary)
	mux.HandleFunc("GET /api/v1/events", handleEvents)
	mux.HandleFunc("GET /api/v1/task/{task_id}", handleTask)
	mux.HandleFunc("GET /api/v1/task/{task_id}/log", handleTaskLog)
	mux.HandleFunc("GET /api/v1/task/{task_id}/metrics", handleTaskMetrics)
	mux.HandleFunc("GET /api/v1/cluster/{cluster_id}", handleCluster)
	mux.HandleFunc("GET /api/v1/cluster/{cluster_id}/status", handleClusterStatus)
	mux.HandleFunc("GET /api/v1/clusters", handleClusters)
	mux.HandleFunc("GET /api/v1/clusters/summary", handleClusterStatuses)
	mux.HandleFunc("GET /api/v1/job/{job_id}", handleJob)
	mux.HandleFunc("POST /api/v1/subscription", handleCreateSubscription)
	mux.HandleFunc("POST /api/v1/subscription/{subscription_id}/unsubscribe", handleUnsubscribe)
	mux.HandleFunc("POST /api/v1/task/{task_id}/subscription", handleCreateTaskSubscription)
	mux.HandleFunc("POST /api/v1/task/{task_id}/subscription/{subscription_id}/unsubscribe", handleTaskUnsubscribe)
	//	mux.HandleFunc("POST /gc", handleGC)

	log.Printf("Listening on %s", *addr)
	if err := http.ListenAndServe(*addr, corsMiddleware(mux)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}
