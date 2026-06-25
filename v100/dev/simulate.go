package dev

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/google/uuid"
	"github.com/urfave/cli"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const simWorkpoolID = "simulate-pool"

type simConfig struct {
	workpoolID        string
	tasksPerJob       int
	maxJobs           int
	meanJobInterval   time.Duration
	meanLocalization  time.Duration
	meanExecution     time.Duration
	meanUpload        time.Duration
	meanPreempt       time.Duration
	workerCount       int
	timingVariancePct int
}

func runDevSimulate(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")

	cfg := simConfig{
		workpoolID:        simWorkpoolID,
		tasksPerJob:       c.Int("tasks-per-job"),
		maxJobs:           c.Int("max-jobs"),
		meanJobInterval:   c.Duration("mean-job-interval"),
		meanLocalization:  c.Duration("mean-localization"),
		meanExecution:     c.Duration("mean-execution"),
		meanUpload:        c.Duration("mean-upload"),
		meanPreempt:       c.Duration("mean-preempt"),
		workerCount:       c.Int("worker-count"),
		timingVariancePct: c.Int("timing-variance-pct"),
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer psClient.Close()

	for _, topicShortName := range []string{"sparkles-events", "sparkles-worker-in"} {
		topicName := fmt.Sprintf("projects/%s/topics/%s", project, topicShortName)
		if _, err := psClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName}); err != nil {
			if grpcstatus.Code(err) != codes.AlreadyExists {
				return fmt.Errorf("ensuring %s topic: %w", topicShortName, err)
			}
		}
	}

	ep := v100.NewEventPublisher(psClient.Publisher("sparkles-events"), fsClient)
	defer ep.Stop()

	queue := v100.NewFirestoreTaskQueue(fsClient, ep)

	workpool := v100.WorkPool{
		WorkpoolID:     cfg.workpoolID,
		MachineType:    "n2-standard-4",
		Region:         "us-central1",
		Zones:          []string{"us-central1-a"},
		Expiry:         time.Now().Add(7 * 24 * time.Hour),
		MaxWorkerCount: cfg.workerCount,
		Status:         "ok",
	}
	if _, err := fsClient.Collection(v100.WorkpoolCollection).Doc(cfg.workpoolID).Set(ctx, workpool); err != nil {
		return fmt.Errorf("writing workpool: %w", err)
	}
	log.Printf("simulate: workpool %s written", cfg.workpoolID)

	batchID := uuid.New().String()
	batchStore := monitor.NewFirestoreBatchRequestStore(fsClient)
	if err := batchStore.Create(ctx, &monitor.BatchAPIRequest{
		BatchID:         batchID,
		JobID:           "simulated",
		WorkpoolID:      cfg.workpoolID,
		ExpectedVMCount: cfg.workerCount,
		Preemptible:     true,
		SubmittedAt:     time.Now(),
		Status:          monitor.BatchStatusStarted,
	}); err != nil {
		return fmt.Errorf("writing BatchAPIRequest: %w", err)
	}
	log.Printf("simulate: batch %s written", batchID)

	jobSummaries := monitor.NewFirestoreJobSummaryStore(fsClient)
	taskStore := monitor.NewFirestoreTaskStore(fsClient)
	workerStore := monitor.NewFirestoreWorkerStore(fsClient)
	workPoolSummaryStore := monitor.NewFirestoreWorkPoolSummaryStore(fsClient)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		runSimJobSubmitter(ctx, cfg, fsClient, ep, jobSummaries)
	}()

	for range cfg.workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSimWorker(ctx, cfg, batchID, fsClient, ep, queue)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		runSimJobSummaryUpdater(ctx, cfg.workpoolID, jobSummaries, taskStore, ep)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runSimWorkPoolSummaryUpdater(ctx, cfg.workpoolID, fsClient, batchStore, taskStore, workerStore, workPoolSummaryStore)
	}()

	wg.Wait()
	return nil
}

func runSimJobSubmitter(ctx context.Context, cfg simConfig, fsClient *firestore.Client, ep *v100.EventPublisher, jobSummaries *monitor.FirestoreJobSummaryStore) {
	jobCounter := 0
	first := true
	for ctx.Err() == nil {
		if cfg.maxJobs > 0 && jobCounter >= cfg.maxJobs {
			return
		}
		if !first {
			if !simSleep(ctx, jitter(cfg.meanJobInterval, cfg.timingVariancePct)) {
				return
			}
		}
		first = false

		jobCounter++
		jobID := uuid.New().String()
		taskIDs := make([]string, cfg.tasksPerJob)
		for i := range taskIDs {
			taskIDs[i] = uuid.New().String()
		}

		now := time.Now()
		job := v100.Job{
			JobID:      jobID,
			Name:       fmt.Sprintf("sim-job-%d", jobCounter),
			WorkpoolID: cfg.workpoolID,
			CreatedAt:  now,
			TaskCount:  cfg.tasksPerJob,
			Resources:  []v100.ResourceEntry{{Name: "slots", Value: 1}},
		}

		err := fsClient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			if err := tx.Set(fsClient.Collection(v100.JobCollection).Doc(jobID), job); err != nil {
				return err
			}
			for i, taskID := range taskIDs {
				base := fmt.Sprintf("gs://sim-bucket/jobs/%s/tasks/%s", jobID, taskID)
				task := v100.Task{
					JobID:       jobID,
					TaskID:      taskID,
					TaskIndex:   i,
					WorkpoolID:  cfg.workpoolID,
					Status:      v100.StatusPending,
					Command:     []string{"echo", "simulated", fmt.Sprintf("task-%d", i)},
					DockerImage: "simulated",
					LogPath:     base + "/output.log",
					ResultPath:  base + "/result",
				}
				if err := tx.Set(fsClient.Collection(v100.TaskCollection).Doc(taskID), task); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			log.Printf("simulate: submitting job: %v", err)
			continue
		}

		summary := &monitor.JobSummary{
			JobID:      jobID,
			WorkpoolID: cfg.workpoolID,
			CreatedAt:  now,
			Expiry:     now.Add(7 * 24 * time.Hour),
			Status:     monitor.JobStatusPending,
			Tasks:      []monitor.TaskCount{{State: "pending", Count: cfg.tasksPerJob}},
		}
		if err := jobSummaries.Create(ctx, summary); err != nil {
			log.Printf("simulate: creating job summary for %s: %v", jobID, err)
		}

		if err := ep.PublishJobCreated(ctx, v100.JobCreatedEvent{
			Type:       "job_created",
			JobID:      jobID,
			WorkpoolID: cfg.workpoolID,
		}); err != nil {
			log.Printf("simulate: publishing job_created: %v", err)
		}

		log.Printf("simulate: submitted job %s (%d tasks)", jobID[:8], cfg.tasksPerJob)
	}
}

func runSimWorker(ctx context.Context, cfg simConfig, batchID string, fsClient *firestore.Client, ep *v100.EventPublisher, queue v100.TaskQueue) {
	const workerHeartbeatPeriod = time.Minute

	for ctx.Err() == nil {
		workerID := uuid.New().String()
		now := time.Now()

		workerDoc := fsClient.Collection("Workers").Doc(workerID)
		if _, err := workerDoc.Set(ctx, v100.WorkerRecord{
			WorkerID:        workerID,
			WorkpoolID:      cfg.workpoolID,
			BatchID:         batchID,
			InstanceName:    "sim-" + workerID[:8],
			Status:          "started",
			Expiry:          now.Add(7 * 24 * time.Hour),
			HeartbeatExpiry: now.Add(workerHeartbeatPeriod),
		}); err != nil {
			log.Printf("simulate: writing worker doc: %v", err)
			if !simSleep(ctx, 2*time.Second) {
				return
			}
			continue
		}
		if err := ep.PublishWorkerEvent(ctx, v100.WorkerEvent{
			Type: "worker_started", WorkerID: workerID, WorkpoolID: cfg.workpoolID,
		}); err != nil {
			log.Printf("simulate: publishing worker_started: %v", err)
		}
		log.Printf("simulate: worker %s started", workerID[:8])

		heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
		go func() {
			ticker := time.NewTicker(workerHeartbeatPeriod)
			defer ticker.Stop()
			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-ticker.C:
					if _, err := workerDoc.Update(heartbeatCtx, []firestore.Update{
						{Path: "heartbeat_expiry", Value: time.Now().Add(workerHeartbeatPeriod)},
					}); err != nil {
						log.Printf("simulate: heartbeat for %s: %v", workerID[:8], err)
					}
				}
			}
		}()

		preemptDuration := jitter(cfg.meanPreempt, cfg.timingVariancePct)
		preemptCtx, cancelPreempt := context.WithTimeout(ctx, preemptDuration)
		simWorkerLifetime(ctx, preemptCtx, cfg, workerID, queue)
		cancelPreempt()
		stopHeartbeat()

		stopTime := time.Now()
		if _, err := workerDoc.Update(ctx, []firestore.Update{
			{Path: "status", Value: "stopped"},
			{Path: "expiry", Value: stopTime},
			{Path: "heartbeat_expiry", Value: stopTime},
		}); err != nil {
			log.Printf("simulate: stopping worker %s: %v", workerID[:8], err)
		}
		if err := ep.PublishWorkerEvent(ctx, v100.WorkerEvent{
			Type: "worker_stopped", WorkerID: workerID, WorkpoolID: cfg.workpoolID,
		}); err != nil {
			log.Printf("simulate: publishing worker_stopped: %v", err)
		}
		log.Printf("simulate: worker %s stopped", workerID[:8])

		if !simSleep(ctx, 2*time.Second) {
			return
		}
	}
}

// simWorkerLifetime runs the task execution loop for one worker lifetime (until
// preemptCtx is cancelled). ctx is used for cleanup calls after preemption.
func simWorkerLifetime(ctx context.Context, preemptCtx context.Context, cfg simConfig, workerID string, queue v100.TaskQueue) {
	for preemptCtx.Err() == nil {
		task, err := queue.GetFirstPendingTask(preemptCtx, cfg.workpoolID)
		if err != nil || task == nil {
			if !simSleep(preemptCtx, time.Second) {
				return
			}
			continue
		}

		claimed, err := queue.ClaimTask(preemptCtx, task.JobID, workerID)
		if err != nil || claimed == nil {
			continue
		}

		currentState := v100.StatusClaimed
		taskStart := time.Now()

		if !simSleep(preemptCtx, jitter(cfg.meanLocalization, cfg.timingVariancePct)) {
			queue.RecordFailed(ctx, claimed.TaskID, "simulated preemption", currentState) //nolint:errcheck
			return
		}

		if err := queue.UpdateState(preemptCtx, claimed.TaskID, v100.StatusClaimed, v100.StatusRunning); err != nil {
			log.Printf("simulate: claimed→running: %v", err)
			continue
		}
		currentState = v100.StatusRunning

		if !simSleep(preemptCtx, jitter(cfg.meanExecution, cfg.timingVariancePct)) {
			queue.RecordFailed(ctx, claimed.TaskID, "simulated preemption", currentState) //nolint:errcheck
			return
		}

		if err := queue.UpdateState(preemptCtx, claimed.TaskID, v100.StatusRunning, v100.StatusWriting); err != nil {
			log.Printf("simulate: running→writing: %v", err)
			continue
		}
		currentState = v100.StatusWriting

		if !simSleep(preemptCtx, jitter(cfg.meanUpload, cfg.timingVariancePct)) {
			queue.RecordFailed(ctx, claimed.TaskID, "simulated preemption", currentState) //nolint:errcheck
			return
		}

		elapsed := time.Since(taskStart)
		exitCode := 0
		if rand.Float64() < 0.05 {
			exitCode = 1
		}

		if exitCode != 0 {
			if err := queue.RecordError(preemptCtx, claimed.TaskID, exitCode); err != nil {
				log.Printf("simulate: recording error: %v", err)
			}
		} else {
			if err := queue.UpdateState(preemptCtx, claimed.TaskID, v100.StatusWriting, v100.StatusSuccess); err != nil {
				log.Printf("simulate: writing→success: %v", err)
			}
		}

		if err := queue.RecordResourceUsage(preemptCtx, claimed.TaskID, &v100.ResourceUsage{
			StartTime:      taskStart,
			EndTime:        time.Now(),
			ElapsedSeconds: elapsed.Seconds(),
			ExitCode:       exitCode,
		}); err != nil {
			log.Printf("simulate: recording resource usage: %v", err)
		}
	}
}

func runSimJobSummaryUpdater(ctx context.Context, workpoolID string, jobSummaries *monitor.FirestoreJobSummaryStore, taskStore *monitor.FirestoreTaskStore, ep *v100.EventPublisher) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			summaries, err := jobSummaries.ListNonTerminal(ctx)
			if err != nil {
				log.Printf("simulate: listing non-terminal jobs: %v", err)
				continue
			}
			for _, summary := range summaries {
				if summary.WorkpoolID != workpoolID {
					continue
				}
				counts, err := taskStore.CountByJob(ctx, summary.JobID)
				if err != nil {
					log.Printf("simulate: counting tasks for %s: %v", summary.JobID, err)
					continue
				}
				newStatus := simComputeJobStatus(counts)
				newTasks := simTaskCountsFromMap(counts)
				summary.Status = newStatus
				summary.Tasks = newTasks
				if err := jobSummaries.Save(ctx, summary); err != nil {
					log.Printf("simulate: saving job summary for %s: %v", summary.JobID, err)
					continue
				}
				history := &monitor.JobSummaryHistory{
					JobID:      summary.JobID,
					WorkpoolID: summary.WorkpoolID,
					Timestamp:  time.Now(),
					Expiry:     summary.Expiry,
					Status:     newStatus,
					Tasks:      newTasks,
				}
				if err := jobSummaries.SaveHistory(ctx, history); err != nil {
					log.Printf("simulate: saving job summary history for %s: %v", summary.JobID, err)
				}
				if monitor.IsTerminalJobStatus(newStatus) {
					if err := ep.PublishJobTerminated(ctx, summary.JobID, summary.WorkpoolID); err != nil {
						log.Printf("simulate: publishing job_terminated for %s: %v", summary.JobID, err)
					}
				}
			}
		}
	}
}

func runSimWorkPoolSummaryUpdater(ctx context.Context, workpoolID string, fsClient *firestore.Client, batchStore *monitor.FirestoreBatchRequestStore, taskStore *monitor.FirestoreTaskStore, workerStore *monitor.FirestoreWorkerStore, workPoolSummaryStore *monitor.FirestoreWorkPoolSummaryStore) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			summary, err := simComputeWorkPoolSummary(ctx, workpoolID, batchStore, taskStore, workerStore)
			if err != nil {
				log.Printf("simulate: computing workpool summary: %v", err)
				continue
			}
			if err := workPoolSummaryStore.Save(ctx, summary); err != nil {
				log.Printf("simulate: saving workpool summary: %v", err)
				continue
			}
			history := &monitor.WorkPoolSummaryHistory{
				WorkpoolID:                    summary.WorkpoolID,
				Timestamp:                     time.Now(),
				Expiry:                        summary.Expiry,
				ExpectedPreemptibleWorkers:    summary.ExpectedPreemptibleWorkers,
				ExpectedNonpreemptibleWorkers: summary.ExpectedNonpreemptibleWorkers,
				UnhealthyBatchCount:           summary.UnhealthyBatchCount,
				BatchAPIRequestCounts:         summary.BatchAPIRequestCounts,
				PreemptibleWorkers:            summary.PreemptibleWorkers,
				NonpreemptibleWorkers:         summary.NonpreemptibleWorkers,
				Tasks:                         summary.Tasks,
			}
			if err := workPoolSummaryStore.SaveHistory(ctx, history); err != nil {
				log.Printf("simulate: saving workpool summary history: %v", err)
			}
			_, err = fsClient.Collection(v100.WorkpoolCollection).Doc(workpoolID).Update(ctx, []firestore.Update{
				{Path: "status", Value: "ok"},
				{Path: "expiry", Value: time.Now().Add(7 * 24 * time.Hour)},
			})
			if err != nil {
				log.Printf("simulate: updating workpool status: %v", err)
			}
		}
	}
}

func simComputeWorkPoolSummary(ctx context.Context, workpoolID string, batchStore *monitor.FirestoreBatchRequestStore, taskStore *monitor.FirestoreTaskStore, workerStore *monitor.FirestoreWorkerStore) (*monitor.WorkPoolSummary, error) {
	batches, err := batchStore.ListAllByWorkpool(ctx, workpoolID)
	if err != nil {
		return nil, fmt.Errorf("listing batches: %w", err)
	}
	var expectedPreemptible, expectedNonPreemptible, unhealthyCount int
	batchStatusMap := make(map[string]int)
	batchPreemptible := make(map[string]bool)
	for _, b := range batches {
		if b.Preemptible {
			expectedPreemptible += b.ExpectedVMCount
		} else {
			expectedNonPreemptible += b.ExpectedVMCount
		}
		if b.Unhealthy {
			unhealthyCount++
		}
		batchStatusMap[string(b.Status)]++
		batchPreemptible[b.BatchID] = b.Preemptible
	}

	taskCounts, err := taskStore.CountByWorkpool(ctx, workpoolID)
	if err != nil {
		return nil, fmt.Errorf("counting tasks: %w", err)
	}

	workers, err := workerStore.ListAllForWorkpool(ctx, workpoolID)
	if err != nil {
		return nil, fmt.Errorf("listing workers: %w", err)
	}
	preemptibleWorkerCounts := make(map[string]int)
	nonPreemptibleWorkerCounts := make(map[string]int)
	for _, w := range workers {
		if batchPreemptible[w.BatchID] {
			preemptibleWorkerCounts[w.Status]++
		} else {
			nonPreemptibleWorkerCounts[w.Status]++
		}
	}

	now := time.Now()
	return &monitor.WorkPoolSummary{
		WorkpoolID:                    workpoolID,
		Expiry:                        now.Add(7 * 24 * time.Hour),
		LastUpdated:                   now,
		ExpectedPreemptibleWorkers:    expectedPreemptible,
		ExpectedNonpreemptibleWorkers: expectedNonPreemptible,
		UnhealthyBatchCount:           unhealthyCount,
		BatchAPIRequestCounts:         simStatusCountsFromMap(batchStatusMap),
		PreemptibleWorkers:            simStatusCountsFromMap(preemptibleWorkerCounts),
		NonpreemptibleWorkers:         simStatusCountsFromMap(nonPreemptibleWorkerCounts),
		Tasks:                         simStatusCountsFromMap(taskCounts),
	}, nil
}

func simStatusCountsFromMap(m map[string]int) []monitor.StatusCount {
	var result []monitor.StatusCount
	for status, count := range m {
		if count > 0 {
			result = append(result, monitor.StatusCount{Status: status, Count: count})
		}
	}
	return result
}

// simComputeJobStatus mirrors the unexported monitor.computeJobStatus.
func simComputeJobStatus(counts map[string]int) monitor.JobStatus {
	pending := counts["pending"]
	active := counts["claimed"] + counts["running"] + counts["writing"]
	failed := counts["failed"]
	errored := counts["error"]

	if pending > 0 || active > 0 {
		if failed > 0 {
			return monitor.JobStatusInProgressWithFailure
		}
		if errored > 0 {
			return monitor.JobStatusInProgressWithError
		}
		if active > 0 {
			return monitor.JobStatusInProgress
		}
		return monitor.JobStatusPending
	}
	if counts["killed"] > 0 {
		return monitor.JobStatusKilled
	}
	if failed > 0 {
		return monitor.JobStatusFailed
	}
	if errored > 0 {
		return monitor.JobStatusError
	}
	return monitor.JobStatusSuccess
}

// simTaskCountsFromMap mirrors the unexported monitor.taskCountsFromMap.
func simTaskCountsFromMap(counts map[string]int) []monitor.TaskCount {
	var result []monitor.TaskCount
	for state, count := range counts {
		if count > 0 {
			result = append(result, monitor.TaskCount{State: state, Count: count})
		}
	}
	return result
}

// jitter returns a duration uniformly randomised within ±variancePct% of mean.
func jitter(mean time.Duration, variancePct int) time.Duration {
	if variancePct == 0 {
		return mean
	}
	delta := float64(mean) * float64(variancePct) / 100.0
	return mean + time.Duration((rand.Float64()*2-1)*delta)
}

// simSleep sleeps for d, returning true on completion and false if ctx is cancelled.
func simSleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
