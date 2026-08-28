package monitor

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
