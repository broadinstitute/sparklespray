package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"time"

	"github.com/redis/go-redis/v9"
)

// StartMockBatchAPI starts a background goroutine that simulates the GCP Batch API
// for local testing. It polls Redis for pending redisBatchJob entries, launches their
// WorkerCommandArgs as subprocesses (one per InstanceCount), and updates the job state
// to Running, then Complete or Failed based on the exit status.
func StartMockBatchAPI(ctx context.Context, client *redis.Client, pollInterval time.Duration) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
			if err := mockBatchAPIPoll(ctx, client); err != nil {
				log.Printf("mock batch API: poll error: %v", err)
			}
		}
	}()
}

// mockBatchAPIPoll scans all batch_job:* keys in Redis and starts any that are Pending.
func mockBatchAPIPoll(ctx context.Context, client *redis.Client) error {
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, "batch_job:*", 100).Result()
		if err != nil {
			return fmt.Errorf("scanning batch job keys: %w", err)
		}
		for _, key := range keys {
			data, err := client.Get(ctx, key).Bytes()
			if err != nil {
				continue
			}
			var job redisBatchJob
			if err := json.Unmarshal(data, &job); err != nil {
				continue
			}
			if job.State == Pending {
				if err := mockStartJob(ctx, client, &job); err != nil {
					log.Printf("mock batch API: failed to start job %s: %v", job.ID, err)
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

// mockStartJob marks the job Running and launches one subprocess per InstanceCount.
func mockStartJob(ctx context.Context, client *redis.Client, job *redisBatchJob) error {
	if len(job.WorkerCommandArgs) == 0 {
		return fmt.Errorf("job %s: WorkerCommandArgs is empty", job.ID)
	}

	job.State = Running
	if err := mockWriteJob(ctx, client, job); err != nil {
		return fmt.Errorf("marking job %s running: %w", job.ID, err)
	}

	instanceCount := job.InstanceCount
	if instanceCount <= 0 {
		instanceCount = 1
	}

	go mockWatchJobInstances(ctx, client, job.ID, job.WorkerCommandArgs, instanceCount)
	return nil
}

// mockWatchJobInstances runs instanceCount copies of the command concurrently and
// updates the job state to Complete (all succeeded) or Failed (any failed).
func mockWatchJobInstances(ctx context.Context, client *redis.Client, jobID string, args []string, instanceCount int) {
	type result struct{ err error }
	results := make(chan result, instanceCount)

	for i := 0; i < instanceCount; i++ {
		go func() {
			cmd := exec.CommandContext(ctx, args[0], args[1:]...)
			results <- result{err: cmd.Run()}
		}()
	}

	anyFailed := false
	for i := 0; i < instanceCount; i++ {
		if r := <-results; r.err != nil {
			anyFailed = true
		}
	}

	// Use a fresh context for the final write — the parent ctx may have been cancelled.
	writeCtx := context.Background()

	data, err := client.Get(writeCtx, "batch_job:"+jobID).Bytes()
	if err != nil {
		log.Printf("mock batch API: reading job %s for final update: %v", jobID, err)
		return
	}
	var job redisBatchJob
	if err := json.Unmarshal(data, &job); err != nil {
		log.Printf("mock batch API: decoding job %s for final update: %v", jobID, err)
		return
	}

	if anyFailed {
		job.State = Failed
	} else {
		job.State = Complete
	}

	if err := mockWriteJob(writeCtx, client, &job); err != nil {
		log.Printf("mock batch API: writing final state for job %s: %v", jobID, err)
	}
}

// mockWriteJob serializes a redisBatchJob and stores it back to Redis.
func mockWriteJob(ctx context.Context, client *redis.Client, job *redisBatchJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return client.Set(ctx, "batch_job:"+job.ID, data, 0).Err()
}
