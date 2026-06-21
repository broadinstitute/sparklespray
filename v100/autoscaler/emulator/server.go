package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/broadinstitute/sparklespray/v100/autoscaler"
	"github.com/google/uuid"
)

const emulatorRegion = "emulator"

var emulatorZones = []string{"emulator-zone-a", "emulator-zone-b", "emulator-zone-c"}

// ---- request/response types ----

type label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type createJobRequest struct {
	Region       string   `json:"region"`
	MachineType  string   `json:"machineType"`
	VMCount      int      `json:"vmCount"`
	Preemptible  bool     `json:"preemptible"`
	DockerImage  string   `json:"dockerImage"`
	Command      string   `json:"command"`
	EmptyVolumes []string `json:"emptyVolumes"`
	Labels       []label  `json:"labels"`
}

// ---- in-memory state ----

type emulatorVM struct {
	InstanceName string
	Zone         string
	done         bool
	exitCode     int
}

type emulatorJob struct {
	JobID       string
	Labels      []label
	DockerImage string
	Command     string
	VMCount     int
	Status      autoscaler.BatchJobStatus
	VMs         []*emulatorVM
	cancel      chan struct{}
	cancelOnce  sync.Once
}

func (j *emulatorJob) cancelJob() {
	j.cancelOnce.Do(func() { close(j.cancel) })
}

type server struct {
	mu        sync.Mutex
	jobs      map[string]*emulatorJob
	nextJobID int
	queueTime time.Duration
	wg        sync.WaitGroup // tracks all docker-wait goroutines for graceful shutdown
}

func newServer(queueTime time.Duration) *server {
	return &server{
		jobs:      make(map[string]*emulatorJob),
		queueTime: queueTime,
	}
}

// ---- HTTP routing ----

func (s *server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /jobs", s.handleCreateJob)
	mux.HandleFunc("GET /jobs/{jobID}/status", s.handleGetJobStatus)
	mux.HandleFunc("GET /jobs", s.handleListJobs)
	mux.HandleFunc("GET /region/{region}/zones", s.handleGetZones)
	mux.HandleFunc("GET /vms/{zone}", s.handleListVMs)
	mux.HandleFunc("DELETE /vms/{zone}/{instanceName}", s.handleTerminateVM)
	mux.HandleFunc("POST /jobs/{jobID}/cancel", s.handleTerminateJob)
	mux.HandleFunc("POST /control/reset", s.handleReset)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// ---- handlers ----

func (s *server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Region != emulatorRegion {
		http.Error(w, fmt.Sprintf("region must be %q, got %q", emulatorRegion, req.Region), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.nextJobID++
	jobID := fmt.Sprintf("emjob-%d-%s", s.nextJobID, uuid.New().String()[:8])
	log.Printf("emulator: CreateJob jobID=%s image=%s vmCount=%d preemptible=%v", jobID, req.DockerImage, req.VMCount, req.Preemptible)
	job := &emulatorJob{
		JobID:       jobID,
		Labels:      req.Labels,
		DockerImage: req.DockerImage,
		Command:     req.Command,
		VMCount:     req.VMCount,
		Status:      autoscaler.BatchJobStatusQueued,
		cancel:      make(chan struct{}),
	}
	for i := 0; i < req.VMCount; i++ {
		job.VMs = append(job.VMs, &emulatorVM{
			InstanceName: fmt.Sprintf("vm-%s-%d", jobID, i),
			Zone:         emulatorZones[i%len(emulatorZones)],
		})
	}
	s.jobs[jobID] = job
	s.mu.Unlock()

	go s.runJob(job)

	writeJSON(w, http.StatusOK, map[string]string{"jobID": jobID})
}

func (s *server) handleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	log.Printf("emulator: GetJobStatus jobID=%s", jobID)

	s.mu.Lock()
	job := s.jobs[jobID]
	var status autoscaler.BatchJobStatus
	if job != nil {
		status = job.Status
	}
	s.mu.Unlock()

	if job == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": string(status)})
}

func (s *server) handleGetZones(w http.ResponseWriter, r *http.Request) {
	region := r.PathValue("region")
	log.Printf("emulator: GetZones region=%s", region)
	if region != emulatorRegion {
		http.Error(w, fmt.Sprintf("unknown region %q", region), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"zones": emulatorZones})
}

func (s *server) handleListVMs(w http.ResponseWriter, r *http.Request) {
	zone := r.PathValue("zone")
	filterName := r.URL.Query().Get("filterLabelName")
	filterValue := r.URL.Query().Get("filterLabelValue")
	log.Printf("emulator: ListVMs zone=%s %s=%s", zone, filterName, filterValue)

	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]autoscaler.VMInfo)
	for _, job := range s.jobs {
		if job.Status != autoscaler.BatchJobStatusRunning {
			continue
		}
		if !jobMatchesLabel(job, filterName, filterValue) {
			continue
		}
		for _, vm := range job.VMs {
			if vm.Zone != zone || vm.done {
				continue
			}
			result[vm.InstanceName] = autoscaler.VMInfo{
				InstanceName: vm.InstanceName,
				Zone:         vm.Zone,
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]map[string]autoscaler.VMInfo{"vms": result})
}

func jobMatchesLabel(job *emulatorJob, name, value string) bool {
	if name == "" {
		return true
	}
	for _, l := range job.Labels {
		if l.Name == name && l.Value == value {
			return true
		}
	}
	return false
}

func (s *server) handleTerminateVM(w http.ResponseWriter, r *http.Request) {
	instanceName := r.PathValue("instanceName")
	log.Printf("emulator: TerminateVM instance=%s", instanceName)
	if err := exec.Command("docker", "stop", instanceName).Run(); err != nil {
		http.Error(w, fmt.Sprintf("docker stop %s: %v", instanceName, err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleTerminateJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	log.Printf("emulator: TerminateJob jobID=%s", jobID)

	s.mu.Lock()
	job := s.jobs[jobID]
	var containerNames []string
	if job != nil {
		for _, vm := range job.VMs {
			if !vm.done {
				containerNames = append(containerNames, vm.InstanceName)
			}
		}
		job.Status = autoscaler.BatchJobStatusFailed
	}
	s.mu.Unlock()

	if job == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	// Unblock queue-wait goroutine if still sleeping, then fire-and-forget stop for running containers.
	job.cancelJob()
	for _, name := range containerNames {
		go exec.Command("docker", "stop", name).Run()
	}
	w.WriteHeader(http.StatusNoContent)
}

type vmDebugInfo struct {
	InstanceName string `json:"instanceName"`
	Zone         string `json:"zone"`
	Done         bool   `json:"done"`
	ExitCode     int    `json:"exitCode"`
}

type jobDebugInfo struct {
	JobID  string        `json:"jobID"`
	Status string        `json:"status"`
	Labels []label       `json:"labels"`
	VMs    []vmDebugInfo `json:"vms"`
}

func (s *server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	log.Printf("emulator: ListJobs")
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs := make([]jobDebugInfo, 0, len(s.jobs))
	for _, job := range s.jobs {
		info := jobDebugInfo{
			JobID:  job.JobID,
			Status: string(job.Status),
			Labels: job.Labels,
			VMs:    make([]vmDebugInfo, 0, len(job.VMs)),
		}
		for _, vm := range job.VMs {
			info.VMs = append(info.VMs, vmDebugInfo{
				InstanceName: vm.InstanceName,
				Zone:         vm.Zone,
				Done:         vm.done,
				ExitCode:     vm.exitCode,
			})
		}
		jobs = append(jobs, info)
	}
	writeJSON(w, http.StatusOK, map[string][]jobDebugInfo{"jobs": jobs})
}

func (s *server) handleReset(w http.ResponseWriter, r *http.Request) {
	log.Printf("emulator: Reset")
	s.mu.Lock()
	var names []string
	for _, job := range s.jobs {
		job.cancelJob()
		for _, vm := range job.VMs {
			if !vm.done {
				names = append(names, vm.InstanceName)
			}
		}
	}
	s.jobs = make(map[string]*emulatorJob)
	s.nextJobID = 0
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		name := name
		go func() {
			defer wg.Done()
			exec.Command("docker", "stop", name).Run()
		}()
	}
	wg.Wait()
	w.WriteHeader(http.StatusNoContent)
}

// ---- job lifecycle ----

func (s *server) runJob(job *emulatorJob) {
	// Phase 1: wait in QUEUED state for queueTime, or abort on cancellation.
	select {
	case <-time.After(s.queueTime):
	case <-job.cancel:
		s.mu.Lock()
		job.Status = autoscaler.BatchJobStatusFailed
		s.mu.Unlock()
		return
	}

	// Phase 2: spawn containers sequentially.
	for _, vm := range job.VMs {
		args := []string{"run", "-d", "--name", vm.InstanceName}
		for _, l := range job.Labels {
			args = append(args, "--label", fmt.Sprintf("%s=%s", l.Name, l.Value))
		}
		args = append(args, job.DockerImage)
		if job.Command != "" {
			args = append(args, strings.Fields(job.Command)...)
		}
		cmd := exec.Command("docker", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Printf("emulator: docker run -d %s: %v", vm.InstanceName, err)
			vm.done = true
			vm.exitCode = 1
		}
	}

	// If every VM failed to start, mark the job Failed immediately without entering Running.
	allFailed := true
	for _, vm := range job.VMs {
		if !vm.done {
			allFailed = false
			break
		}
	}
	if allFailed {
		s.mu.Lock()
		job.Status = autoscaler.BatchJobStatusFailed
		s.mu.Unlock()
		log.Printf("emulator: all VMs failed to start for job %s", job.JobID)
		return
	}

	s.mu.Lock()
	job.Status = autoscaler.BatchJobStatusRunning
	s.mu.Unlock()

	// Phase 3: watch each successfully-started container.
	var watchWg sync.WaitGroup
	for _, vm := range job.VMs {
		if vm.done {
			continue // spawn failed; skip
		}
		vm := vm
		watchWg.Add(1)
		s.wg.Add(1)
		go func() {
			defer watchWg.Done()
			defer s.wg.Done()

			exitCode := waitContainer(vm.InstanceName)
			exec.Command("docker", "rm", vm.InstanceName).Run()

			s.mu.Lock()
			vm.done = true
			vm.exitCode = exitCode
			s.mu.Unlock()
		}()
	}

	// Transition job to final status once every VM has exited.
	go func() {
		watchWg.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		if job.Status != autoscaler.BatchJobStatusRunning {
			return
		}
		for _, vm := range job.VMs {
			if vm.exitCode != 0 {
				job.Status = autoscaler.BatchJobStatusFailed
				return
			}
		}
		job.Status = autoscaler.BatchJobStatusSucceeded
	}()
}

// waitContainer blocks until the named container exits and returns its exit code.
func waitContainer(name string) int {
	out, err := exec.Command("docker", "wait", name).Output()
	if err != nil {
		return 1
	}
	code, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 1
	}
	return code
}

// ---- graceful shutdown ----

func (s *server) stopAllContainers() {
	s.mu.Lock()
	var names []string
	for _, job := range s.jobs {
		job.cancelJob()
		if job.Status == autoscaler.BatchJobStatusRunning {
			for _, vm := range job.VMs {
				if !vm.done {
					names = append(names, vm.InstanceName)
				}
			}
		}
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		name := name
		go func() {
			defer wg.Done()
			log.Printf("emulator: stopping container %s", name)
			exec.Command("docker", "stop", name).Run()
		}()
	}
	wg.Wait()
}

// Run starts the batch API emulator HTTP server and blocks until SIGINT or SIGTERM.
// On shutdown, it stops all running Docker containers and waits for them to exit cleanly
// before returning.
func Run(addr string, queueTime time.Duration) error {
	s := newServer(queueTime)
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	srv := &http.Server{Addr: addr, Handler: mux}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	srvErrCh := make(chan error, 1)
	go func() {
		log.Printf("emulator: listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			srvErrCh <- err
		}
		close(srvErrCh)
	}()

	select {
	case err := <-srvErrCh:
		return err
	case sig := <-sigCh:
		log.Printf("emulator: received %s, shutting down", sig)
	}

	srv.Shutdown(context.Background())

	log.Println("emulator: stopping all containers...")
	s.stopAllContainers()

	log.Println("emulator: waiting for containers to exit...")
	s.wg.Wait()

	log.Println("emulator: shutdown complete")
	return nil
}
