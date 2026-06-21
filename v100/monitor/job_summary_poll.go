package monitor

import (
	"context"
	"log"
	"time"
)

// runJobSummaryPoll updates JobSummary for every job that is not yet in a
// terminal state. For each job it counts tasks by status, recomputes the job
// status, writes a history snapshot, and — on first transition to terminal —
// publishes a JobTerminatedEvent.
func (a *Monitor) runJobSummaryPoll(ctx context.Context) error {
	summaries, err := a.jobSummaries.ListNonTerminal(ctx)
	if err != nil {
		return err
	}

	for _, summary := range summaries {
		if err := a.updateJobSummary(ctx, summary); err != nil {
			log.Printf("job summary poll: job %s: %v", summary.JobID, err)
		}
	}
	return nil
}

func (a *Monitor) updateJobSummary(ctx context.Context, summary *JobSummary) error {
	counts, err := a.tasks.CountByJob(ctx, summary.JobID)
	if err != nil {
		return err
	}

	newStatus := computeJobStatus(counts)
	newTasks := taskCountsFromMap(counts)

	summary.Status = newStatus
	summary.Tasks = newTasks

	if err := a.jobSummaries.Save(ctx, summary); err != nil {
		return err
	}

	history := &JobSummaryHistory{
		JobID:      summary.JobID,
		WorkpoolID: summary.WorkpoolID,
		Timestamp:  time.Now(),
		Expiry:     summary.Expiry,
		Status:     newStatus,
		Tasks:      newTasks,
	}
	if err := a.jobSummaries.SaveHistory(ctx, history); err != nil {
		return err
	}

	if IsTerminalJobStatus(newStatus) && a.jobTerminated != nil {
		if err := a.jobTerminated.PublishJobTerminated(ctx, summary.JobID, summary.WorkpoolID); err != nil {
			log.Printf("job summary poll: publish job_terminated for job %s: %v", summary.JobID, err)
		}
	}
	return nil
}

// computeJobStatus derives a JobStatus from a map of task status → count.
func computeJobStatus(counts map[string]int) JobStatus {
	pending := counts["pending"]
	active := counts["claimed"] + counts["running"] + counts["writing"]
	failed := counts["failed"]
	errored := counts["error"]

	if pending > 0 || active > 0 {
		if failed > 0 {
			return JobStatusInProgressWithFailure
		}
		if errored > 0 {
			return JobStatusInProgressWithError
		}
		if active > 0 {
			return JobStatusInProgress
		}
		return JobStatusPending
	}

	// All tasks are in terminal states.
	if counts["killed"] > 0 {
		return JobStatusKilled
	}
	if failed > 0 {
		return JobStatusFailed
	}
	if errored > 0 {
		return JobStatusError
	}
	return JobStatusSuccess
}

// taskCountsFromMap converts a status→count map to the []TaskCount slice
// stored in JobSummary. Only statuses with non-zero counts are included.
func taskCountsFromMap(counts map[string]int) []TaskCount {
	var result []TaskCount
	for state, count := range counts {
		if count > 0 {
			result = append(result, TaskCount{State: state, Count: count})
		}
	}
	return result
}
