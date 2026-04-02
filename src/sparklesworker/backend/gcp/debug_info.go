package gcp

import (
	"context"
	"fmt"
	"log"
	"time"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/logging/logadmin"
	"github.com/broadinstitute/sparklesworker/backend"
	"google.golang.org/api/iterator"
)

type DebugInfo struct {
	ID                 string
	State              backend.BatchJobState
	RequestedInstances int
	Events             []*Event
	LogMessages        []string
}

type Event struct {
	Type        string
	Description string
	Timestamp   time.Time
	BatchError  string
	ExitCode    *int
}

func FetchDebugInfo(ctx context.Context, batchClient *batch.Client, projectID string, region string, clusterID string) ([]*DebugInfo, error) {
	req := &batchpb.ListJobsRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", projectID, region),
		Filter: fmt.Sprintf(`labels.sparkles-cluster = "%s"`, clusterID),
	}

	loggingClient, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating logging client: %w", err)
	}
	defer loggingClient.Close()

	var jobs []*DebugInfo
	it := batchClient.ListJobs(ctx, req)
	for {
		job, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing batch jobs: %w", err)
		}
		instanceCount := 0
		for _, tg := range job.GetTaskGroups() {
			instanceCount += int(tg.GetTaskCount())
		}
		events := mapToEvent(job.Status.GetStatusEvents())

		var logMessages []string
		if len(events) >= 1 {
			// get approximately the last 10 minutes of logs (just 10 minutes in case this has been running for hours, no need to fetch everything)
			lastEventTime := events[len(events)-1].Timestamp
			startTime := lastEventTime.Add(-10 * time.Minute)
			endTime := lastEventTime.Add(30 * time.Second)

			logMessages, err = fetchJobLogs(ctx, loggingClient, job.GetUid(), startTime, endTime)
			if err != nil {
				return nil, fmt.Errorf("fetching logs for job %s: %w", job.GetName(), err)
			}
		}

		jobs = append(jobs, &DebugInfo{
			ID:                 job.GetName(),
			State:              batchStateToBatchJobState(job),
			RequestedInstances: instanceCount,
			Events:             events,
			LogMessages:        logMessages,
		})
	}
	return jobs, nil
}

func fetchJobLogs(ctx context.Context, client *logadmin.Client, jobUID string, startTime, endTime time.Time) ([]string, error) {
	// jobName is "projects/.../locations/.../jobs/JOB_ID"; extract the short ID
	// parts := strings.Split(jobName, "/")
	// jobID := parts[len(parts)-1]

	filter := fmt.Sprintf(
		//		`resource.type="batch.googleapis.com/Job" AND resource.labels.job_id="%s" AND timestamp >= "%s" AND timestamp <= "%s"`,
		`labels.job_uid=%q AND timestamp >= "%s" AND timestamp <= "%s"`,
		jobUID,
		startTime.UTC().Format(time.RFC3339),
		endTime.UTC().Format(time.RFC3339),
	)

	var messages []string
	logIt := client.Entries(ctx, logadmin.Filter(filter))
	for {
		entry, err := logIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading log entries: %w", err)
		}
		switch p := entry.Payload.(type) {
		case string:
			messages = append(messages, p)
		case fmt.Stringer:
			messages = append(messages, p.String())
		}
	}
	log.Printf("Fetched %d log entries", len(messages))
	return messages, nil
}

func mapToEvent(statusEvents []*batchpb.StatusEvent) []*Event {
	events := make([]*Event, 0, len(statusEvents))
	for _, se := range statusEvents {
		e := &Event{
			Type:        se.GetType(),
			Description: se.GetDescription(),
		}
		if t := se.GetEventTime(); t != nil {
			e.Timestamp = t.AsTime()
		}
		if exec := se.GetTaskExecution(); exec != nil {
			exitCode := int(exec.GetExitCode())
			if exitCode == 50001 {
				e.BatchError = "VM Preemption"
			} else if exitCode == 50002 {
				e.BatchError = "Timeout (VM has stopped communicating with Batch Service)"
			} else if exitCode == 50003 {
				e.BatchError = "VM rebooted unexpectedly during execution"
			} else if exitCode == 50004 {
				e.BatchError = "A task reached the unresponsive time limit and cannot be cancelled"
			} else if exitCode == 50005 {
				e.BatchError = "A task exceeded the max runtime aforded to it"
			} else if exitCode == 50006 {
				e.BatchError = "VM unexpectedly recreated during run time"
			} else {
				e.ExitCode = &exitCode
			}
		}
		events = append(events, e)
	}
	return events
}

func PrintDebugInfos(infos []*DebugInfo) {
	for _, info := range infos {
		fmt.Printf("Job: %s  State: %v  Instances: %d\n", info.ID, info.State, info.RequestedInstances)
		for _, e := range info.Events {
			fmt.Printf("  Event [%s] %s: %s", e.Timestamp.Format(time.RFC3339), e.Type, e.Description)
			if e.BatchError != "" {
				fmt.Printf(" (batch error: %s)", e.BatchError)
			}
			if e.ExitCode != nil {
				fmt.Printf(" (exit code: %d)", *e.ExitCode)
			}
			fmt.Println()
		}
		for _, msg := range info.LogMessages {
			fmt.Printf("  Log: %s\n", msg)
		}
		fmt.Println()
	}
}
