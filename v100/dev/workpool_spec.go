package dev

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	v100 "github.com/broadinstitute/sparklespray/v100"
)

// WorkpoolSpec is the JSON schema for the workpool spec file passed to "dev add-worker".
type WorkpoolSpec struct {
	ID string `json:"id"`
	// ProjectID, if set, is the GCP project the Batch jobs (and therefore the
	// worker VMs) are created in. Empty means the project the
	// dashboard-backend/monitor was started with. The control plane
	// (Firestore, Pub/Sub) always stays in the latter.
	ProjectID             string               `json:"projectID"`
	MachineType           string               `json:"machineType"`
	RootDir               string               `json:"rootDir"`
	SparklesWorkerGCSPath string               `json:"sparklesWorkerGCSPath"`
	Resources             []v100.ResourceEntry `json:"resources"`
	EmptyVolumes          []v100.EmptyVolume   `json:"emptyVolumes"`
	GCSMounts             []v100.GCSMount      `json:"gcsMounts"`
	Region                string               `json:"region"`
	Zones                 []string             `json:"zones"`
	ServiceAccount        string               `json:"serviceAccount"`
	Labels                []v100.Label         `json:"labels"`
	BootDiskSizeGb        int                  `json:"bootDiskSizeGb"`
	BootDiskType          string               `json:"bootDiskType"`

	MaxWorkerCount               int `json:"maxWorkerCount"`
	MaxPreemptibleWorkerAttempts int `json:"maxPreemptibleWorkerAttempts"`
	MaxWorkersPerRequest         int `json:"maxWorkersPerRequest"`

	VMShutdownGracePeriodSec    int `json:"vmShutdownGracePeriodSec"`
	MaxZombiesBeforeAbort       int `json:"maxZombiesBeforeAbort"`
	MaxConsecutiveFailedBatches int `json:"maxConsecutiveFailedBatches"`
	LingerTimeSec               int `json:"lingerTimeSec"`
}

func readJSON[T any](path string) (*T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return &v, nil
}

// computeWorkpoolSpecHash returns the hex-encoded sha256 of the canonical
// JSON of workpoolSpec, including Labels — used both to derive a
// deterministic workpool ID (resolveWorkpoolID) when one isn't given, and to
// populate WorkPool.WorkpoolSpecHash so the exact spec a workpool was
// created from can be identified later.
func computeWorkpoolSpecHash(workpoolSpec *WorkpoolSpec) (string, error) {
	canonical, err := json.Marshal(workpoolSpec)
	if err != nil {
		return "", fmt.Errorf("canonicalizing workpool spec: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func resolveWorkpoolID(workpoolSpec *WorkpoolSpec) (string, error) {
	if workpoolSpec.ID != "" {
		return workpoolSpec.ID, nil
	}
	hash, err := computeWorkpoolSpecHash(workpoolSpec)
	if err != nil {
		return "", err
	}
	return "wp-" + hash[:20], nil
}
