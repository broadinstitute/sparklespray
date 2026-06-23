package dev

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/monitor"
	"github.com/google/uuid"
	"github.com/urfave/cli"
)

func runDevAddWorker(c *cli.Context) error {
	specFile := c.Args().Get(0)
	if specFile == "" {
		return fmt.Errorf("workpool spec json file path is required")
	}
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")
	vmCount := c.Int("vm-count")
	preemptible := c.Bool("preemptible")

	workpoolSpec, err := readJSON[WorkpoolSpec](specFile)
	if err != nil {
		return err
	}
	workpoolID, err := resolveWorkpoolID(workpoolSpec)
	if err != nil {
		return err
	}

	ctx := context.Background()

	batchClient, err := monitor.NewGCPBatchAPIClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating batch API client: %w", err)
	}

	batchID := uuid.New().String()
	jobID, err := batchClient.CreateJob(ctx, &monitor.WorkerJobSpec{
		WorkpoolID:            workpoolID,
		BatchID:               batchID,
		Region:                workpoolSpec.Region,
		MachineType:           workpoolSpec.MachineType,
		VMCount:               vmCount,
		Preemptible:           preemptible,
		RootDir:               workpoolSpec.RootDir,
		SparklesWorkerGCSPath: workpoolSpec.SparklesWorkerGCSPath,
		EmptyVolumes:          toMonitorEmptyVolumes(workpoolSpec.EmptyVolumes),
	})
	if err != nil {
		return fmt.Errorf("creating batch job: %w", err)
	}

	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	batchStore := monitor.NewFirestoreBatchRequestStore(fsClient)
	if err := batchStore.Create(ctx, &monitor.BatchAPIRequest{
		BatchID:         batchID,
		JobID:           jobID,
		WorkpoolID:      workpoolID,
		ExpectedVMCount: vmCount,
		Preemptible:     preemptible,
		SubmittedAt:     time.Now(),
		Status:          monitor.BatchStatusPending,
	}); err != nil {
		return fmt.Errorf("writing BatchAPIRequest to Firestore: %w", err)
	}

	fmt.Printf("created batch job: %s (batchID: %s)\n", jobID, batchID)
	return nil
}

// toMonitorEmptyVolumes converts v100.EmptyVolume to monitor.EmptyVolume.
// The two types have identical field names and Firestore tags by design.
func toMonitorEmptyVolumes(vs []v100.EmptyVolume) []monitor.EmptyVolume {
	out := make([]monitor.EmptyVolume, len(vs))
	for i, v := range vs {
		out[i] = monitor.EmptyVolume{
			MountPoint: v.MountPoint,
			Type:       v.Type,
			SizeInGB:   v.SizeInGB,
		}
	}
	return out
}
