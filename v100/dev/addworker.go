package dev

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/monitor"
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

	batchID := monitor.CreateBatchID()
	jobID, err := batchClient.CreateJob(ctx, &monitor.WorkerJobSpec{
		WorkpoolID:            workpoolID,
		BatchID:               batchID,
		ProjectID:             workpoolSpec.ProjectID,
		Region:                workpoolSpec.Region,
		MachineType:           workpoolSpec.MachineType,
		VMCount:               vmCount,
		Preemptible:           preemptible,
		RootDir:               workpoolSpec.RootDir,
		SparklesWorkerGCSPath: workpoolSpec.SparklesWorkerGCSPath,
		EmptyVolumes:          toMonitorEmptyVolumes(workpoolSpec.EmptyVolumes),
		GCSMounts:             toMonitorGCSMounts(workpoolSpec.GCSMounts),
		Resources:             toMonitorResources(workpoolSpec.Resources),
		ServiceAccount:        workpoolSpec.ServiceAccount,
		DBName:                db,
		LingerTime:            time.Duration(workpoolSpec.LingerTimeSec) * time.Second,
		BootDiskSizeGb:        workpoolSpec.BootDiskSizeGb,
		BootDiskType:          workpoolSpec.BootDiskType,
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
	now := time.Now()
	if err := batchStore.Create(ctx, &monitor.BatchAPIRequest{
		BatchID:         batchID,
		JobID:           jobID,
		ProjectID:       workpoolSpec.ProjectID,
		WorkpoolID:      workpoolID,
		ExpectedVMCount: vmCount,
		Preemptible:     preemptible,
		SubmittedAt:     now,
		Expiry:          now.Add(7 * 24 * time.Hour),
		Status:          monitor.BatchStatusPending,
	}); err != nil {
		return fmt.Errorf("writing BatchAPIRequest to Firestore: %w", err)
	}

	fmt.Printf("created batch job: %s (batchID: %s)\n", jobID, batchID)
	return nil
}

// toMonitorLabels converts v100.Label to monitor.Label.
// The two types have identical field names and Firestore tags by design.
func toMonitorLabels(ls []v100.Label) []monitor.Label {
	out := make([]monitor.Label, len(ls))
	for i, l := range ls {
		out[i] = monitor.Label{Name: l.Name, Value: l.Value}
	}
	return out
}

// toMonitorResources converts v100.ResourceEntry to monitor.ResourceEntry.
func toMonitorResources(rs []v100.ResourceEntry) []monitor.ResourceEntry {
	out := make([]monitor.ResourceEntry, len(rs))
	for i, r := range rs {
		out[i] = monitor.ResourceEntry{Name: r.Name, Value: r.Value}
	}
	return out
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

// toMonitorGCSMounts converts v100.GCSMount to monitor.GCSMount.
// The two types have identical field names and Firestore tags by design.
func toMonitorGCSMounts(ms []v100.GCSMount) []monitor.GCSMount {
	out := make([]monitor.GCSMount, len(ms))
	for i, m := range ms {
		out[i] = monitor.GCSMount{
			MountPath:    m.MountPath,
			GCSPath:      m.GCSPath,
			MountOptions: m.MountOptions,
		}
	}
	return out
}
