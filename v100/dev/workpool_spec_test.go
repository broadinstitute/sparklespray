package dev

import (
	"testing"

	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeWorkpoolSpecHash_Deterministic(t *testing.T) {
	spec := &WorkpoolSpec{MachineType: "n2-standard-2", Region: "us-central1"}
	h1, err := computeWorkpoolSpecHash(spec)
	require.NoError(t, err)
	h2, err := computeWorkpoolSpecHash(spec)
	require.NoError(t, err)
	assert.Equal(t, h1, h2)
	assert.NotEmpty(t, h1)
}

func TestComputeWorkpoolSpecHash_ChangesWithLabels(t *testing.T) {
	base := &WorkpoolSpec{MachineType: "n2-standard-2"}
	withLabel := &WorkpoolSpec{
		MachineType: "n2-standard-2",
		Labels:      []v100.Label{{Name: "team", Value: "alice"}},
	}

	baseHash, err := computeWorkpoolSpecHash(base)
	require.NoError(t, err)
	labelHash, err := computeWorkpoolSpecHash(withLabel)
	require.NoError(t, err)

	assert.NotEqual(t, baseHash, labelHash, "labels must be folded into the spec hash")

	// A different label value should also change the hash.
	otherLabel := &WorkpoolSpec{
		MachineType: "n2-standard-2",
		Labels:      []v100.Label{{Name: "team", Value: "bob"}},
	}
	otherHash, err := computeWorkpoolSpecHash(otherLabel)
	require.NoError(t, err)
	assert.NotEqual(t, labelHash, otherHash)
}

func TestResolveWorkpoolID_ExplicitIDWins(t *testing.T) {
	spec := &WorkpoolSpec{ID: "my-pool", MachineType: "n2-standard-2"}
	id, err := resolveWorkpoolID(spec)
	require.NoError(t, err)
	assert.Equal(t, "my-pool", id)
}

func TestResolveWorkpoolID_DerivedFromHashWhenIDOmitted(t *testing.T) {
	spec := &WorkpoolSpec{MachineType: "n2-standard-2"}
	id, err := resolveWorkpoolID(spec)
	require.NoError(t, err)
	assert.Regexp(t, `^wp-[0-9a-f]{20}$`, id)

	hash, err := computeWorkpoolSpecHash(spec)
	require.NoError(t, err)
	assert.Equal(t, "wp-"+hash[:20], id)
}

func TestResolveWorkpoolID_DiffersWithLabels(t *testing.T) {
	base := &WorkpoolSpec{MachineType: "n2-standard-2"}
	withLabel := &WorkpoolSpec{
		MachineType: "n2-standard-2",
		Labels:      []v100.Label{{Name: "team", Value: "alice"}},
	}

	baseID, err := resolveWorkpoolID(base)
	require.NoError(t, err)
	labelID, err := resolveWorkpoolID(withLabel)
	require.NoError(t, err)

	assert.NotEqual(t, baseID, labelID)
}
