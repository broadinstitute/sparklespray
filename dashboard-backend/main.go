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

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	iamcredentials "google.golang.org/api/iamcredentials/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const EventCollection = "SparklesV5Event"
const ClusterCollection = "SparklesV5Cluster"
const JobCollection = "SparklesV5Job"
const TaskCollection = "SparklesV5Task"
const DefaultLimit = 1000
const MaxLimit = 10000

const topicLifecycle = "sparkles-events"
const topicTaskOut = "sparkles-task-out"
const topicTaskIn = "sparkles-task-in"

var dsClient *datastore.Client
var psClient *pubsub.Client
var iamSvc *iamcredentials.Service
var subscriberSA string
var gProjectID string

// Event is a flexible map that serialises cleanly to the JSON shape the spec requires.
type Event map[string]any

type Cluster struct {
	ClusterID   string    `datastore:"cluster_id" json:"cluster_id"`
	MachineType string    `datastore:"machine_type" json:"machine_type"`
	CreatedAt   time.Time `datastore:"created_at" json:"created_at"`
}

type Job struct {
	JobID                  string    `datastore:"job_id" json:"job_id"`
	Tasks                  []string  `datastore:"tasks,noindex" json:"tasks,omitempty"`
	KubeJobSpec            string    `datastore:"kube_job_spec,noindex" json:"kube_job_spec,omitempty"`
	Metadata               string    `datastore:"metadata,noindex" json:"metadata,omitempty"`
	ClusterID              string    `datastore:"cluster_id" json:"cluster_id"`
	Status                 string    `datastore:"status" json:"status"`
	SubmitTime             time.Time `datastore:"submit_time" json:"submit_time"`
	TaskCount              int32     `datastore:"task_count" json:"task_count"`
	MaxPreemptableAttempts int32     `datastore:"max_preemptable_attempts" json:"max_preemptable_attempts"`
	TargetNodeCount        int32     `datastore:"target_node_count" json:"target_node_count"`
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

func handleGC(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := time.Now()
	q := datastore.NewQuery(EventCollection).FilterField("expiry", "<=", now).KeysOnly()
	keys, err := dsClient.GetAll(ctx, q, nil)
	if err != nil {
		log.Printf("GC query error: %v", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "gc query failed")
		return
	}
	const batchSize = 500
	deleted := 0
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		if err := dsClient.DeleteMulti(ctx, keys[i:end]); err != nil {
			log.Printf("GC delete error: %v", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "gc delete failed")
			return
		}
		deleted += end - i
	}
	log.Printf("GC deleted %d expired events", deleted)
	writeJSON(w, http.StatusOK, map[string]int{"deleted": deleted})
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

func main() {
	projectID := flag.String("project", "", "GCP project ID (required)")
	addr := flag.String("addr", ":8080", "Listen address")
	subSA := flag.String("subscriber-sa", "", "Service account email to impersonate for Pub/Sub subscriber tokens (required)")
	flag.Parse()

	if *projectID == "" {
		log.Fatal("--project is required")
	}
	if *subSA == "" {
		log.Fatal("--subscriber-sa is required")
	}

	gProjectID = *projectID
	subscriberSA = *subSA

	ctx := context.Background()
	var err error

	dsClient, err = datastore.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("Failed to create Datastore client: %v", err)
	}
	defer dsClient.Close()

	psClient, err = pubsub.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer psClient.Close()

	iamSvc, err = iamcredentials.NewService(ctx)
	if err != nil {
		log.Fatalf("Failed to create IAM credentials service: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events", handleEvents)
	mux.HandleFunc("GET /api/v1/task/{task_id}", handleTask)
	mux.HandleFunc("GET /api/v1/task/{task_id}/log", handleTaskLog)
	mux.HandleFunc("GET /api/v1/task/{task_id}/metrics", handleTaskMetrics)
	mux.HandleFunc("GET /api/v1/cluster/{cluster_id}", handleCluster)
	mux.HandleFunc("GET /api/v1/job/{job_id}", handleJob)
	mux.HandleFunc("POST /api/v1/subscription", handleCreateSubscription)
	mux.HandleFunc("POST /api/v1/subscription/{subscription_id}/unsubscribe", handleUnsubscribe)
	mux.HandleFunc("POST /api/v1/task/{task_id}/subscription", handleCreateTaskSubscription)
	mux.HandleFunc("POST /api/v1/task/{task_id}/subscription/{subscription_id}/unsubscribe", handleTaskUnsubscribe)
	mux.HandleFunc("POST /gc", handleGC)

	log.Printf("Listening on %s", *addr)
	if err := http.ListenAndServe(*addr, corsMiddleware(mux)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}
