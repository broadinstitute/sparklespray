package main

import (
	"context"
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
)

const EventCollection = "SparklesV5Event"
const ClusterCollection = "SparklesV5Cluster"
const JobCollection = "SparklesV5Job"
const TaskCollection = "SparklesV5Task"
const DefaultLimit = 1000
const MaxLimit = 10000

var dsClient *datastore.Client

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

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
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
	flag.Parse()

	if *projectID == "" {
		log.Fatal("--project is required")
	}

	ctx := context.Background()
	var err error
	dsClient, err = datastore.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("Failed to create Datastore client: %v", err)
	}
	defer dsClient.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events", handleEvents)
	mux.HandleFunc("GET /api/v1/task/{task_id}", handleTask)
	mux.HandleFunc("GET /api/v1/task/{task_id}/log", handleTaskLog)
	mux.HandleFunc("GET /api/v1/task/{task_id}/metrics", handleTaskMetrics)
	mux.HandleFunc("GET /api/v1/cluster/{cluster_id}", handleCluster)
	mux.HandleFunc("GET /api/v1/job/{job_id}", handleJob)

	log.Printf("Listening on %s", *addr)
	if err := http.ListenAndServe(*addr, corsMiddleware(mux)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}
