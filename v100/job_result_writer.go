package v100

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
)

// JobResultWriter writes a job_summary.json file to Job.ResultPath once a job
// reaches a terminal state. Satisfies the monitor.JobResultWriter interface.
type JobResultWriter struct {
	queue     TaskQueue
	gcsClient *storage.Client
}

func NewJobResultWriter(queue TaskQueue, gcsClient *storage.Client) *JobResultWriter {
	return &JobResultWriter{queue: queue, gcsClient: gcsClient}
}

// WriteJobSummary writes a JobSummaryOutput (the job, its tasks, and its
// workpool) to <Job.ResultPath>/job_summary.json. If the job has no
// ResultPath set, this is a no-op.
func (w *JobResultWriter) WriteJobSummary(ctx context.Context, jobID string) error {
	job, err := w.queue.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("fetching job %s: %w", jobID, err)
	}
	if job.ResultPath == "" {
		return nil
	}

	tasks, err := w.queue.ListTasksForJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("listing tasks for job %s: %w", jobID, err)
	}

	var workpool *WorkPool
	if job.WorkpoolID != "" {
		workpool, err = w.queue.GetWorkPool(ctx, job.WorkpoolID)
		if err != nil {
			return fmt.Errorf("fetching workpool %s: %w", job.WorkpoolID, err)
		}
	}

	data, err := json.MarshalIndent(JobSummaryOutput{Job: *job, Tasks: tasks, Workpool: workpool}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling job summary: %w", err)
	}

	destPath := strings.TrimSuffix(job.ResultPath, "/") + "/job_summary.json"
	bucket, object, err := parseGCSPath(destPath)
	if err != nil {
		return err
	}
	gcsWriter := w.gcsClient.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err := gcsWriter.Write(data); err != nil {
		gcsWriter.Close()
		return fmt.Errorf("uploading job summary to %s: %w", destPath, err)
	}
	return gcsWriter.Close()
}
