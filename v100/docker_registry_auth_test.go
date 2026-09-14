package v100

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDockerRegistryAuth(run func(args ...string) ([]byte, error)) *dockerRegistryAuth {
	return &dockerRegistryAuth{
		hosts: make(map[string]struct{}),
		run:   run,
	}
}

func lastRegistriesArg(calls [][]string) string {
	last := calls[len(calls)-1]
	for _, a := range last {
		if rest, ok := strings.CutPrefix(a, "--registries="); ok {
			return rest
		}
	}
	return ""
}

func TestDockerRegistryAuth_FirstCallIncludesDefaultHost(t *testing.T) {
	var calls [][]string
	d := newTestDockerRegistryAuth(func(args ...string) ([]byte, error) {
		calls = append(calls, args)
		return nil, nil
	})

	require.NoError(t, d.ensure("us-west1-docker.pkg.dev"))
	require.Len(t, calls, 1)
	assert.Equal(t, "gcr.io,us-west1-docker.pkg.dev", lastRegistriesArg(calls))
}

func TestDockerRegistryAuth_SameHostIsNoOp(t *testing.T) {
	var calls [][]string
	d := newTestDockerRegistryAuth(func(args ...string) ([]byte, error) {
		calls = append(calls, args)
		return nil, nil
	})

	require.NoError(t, d.ensure("us-west1-docker.pkg.dev"))
	require.NoError(t, d.ensure("us-west1-docker.pkg.dev"))
	assert.Len(t, calls, 1)
}

func TestDockerRegistryAuth_NewHostGrowsList(t *testing.T) {
	var calls [][]string
	d := newTestDockerRegistryAuth(func(args ...string) ([]byte, error) {
		calls = append(calls, args)
		return nil, nil
	})

	require.NoError(t, d.ensure("us-west1-docker.pkg.dev"))
	require.NoError(t, d.ensure("asia-northeast1-docker.pkg.dev"))
	require.Len(t, calls, 2)
	assert.Equal(t, "asia-northeast1-docker.pkg.dev,gcr.io,us-west1-docker.pkg.dev", lastRegistriesArg(calls))
}

func TestDockerRegistryAuth_GCRAlreadyImplicitlyConfigured(t *testing.T) {
	var calls [][]string
	d := newTestDockerRegistryAuth(func(args ...string) ([]byte, error) {
		calls = append(calls, args)
		return nil, nil
	})

	require.NoError(t, d.ensure("us-west1-docker.pkg.dev"))
	require.NoError(t, d.ensure("gcr.io"))
	assert.Len(t, calls, 1)
}

func TestDockerRegistryAuth_IgnoresEmptyAndNonGoogleHosts(t *testing.T) {
	var calls [][]string
	d := newTestDockerRegistryAuth(func(args ...string) ([]byte, error) {
		calls = append(calls, args)
		return nil, nil
	})

	require.NoError(t, d.ensure(""))
	require.NoError(t, d.ensure("docker.io"))
	require.NoError(t, d.ensure("quay.io"))
	assert.Empty(t, calls)
}

func TestDockerRegistryAuth_FailureDoesNotCache(t *testing.T) {
	attempts := 0
	d := newTestDockerRegistryAuth(func(args ...string) ([]byte, error) {
		attempts++
		return []byte("boom output"), errors.New("boom")
	})

	err := d.ensure("gcr.io")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom output")

	// A later call for the same host retries rather than treating it as
	// already configured.
	err = d.ensure("gcr.io")
	require.Error(t, err)
	assert.Equal(t, 2, attempts)
}

func TestRegistryHostFromImage(t *testing.T) {
	cases := []struct {
		image string
		want  string
	}{
		{"ubuntu:latest", ""},
		{"library/ubuntu", ""},
		{"gcr.io/project/image:tag", "gcr.io"},
		{"us-west1-docker.pkg.dev/project/repo/image:tag", "us-west1-docker.pkg.dev"},
		{"localhost:5000/image", "localhost:5000"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, registryHostFromImage(c.image), "image=%s", c.image)
	}
}

func TestIsGoogleContainerRegistryHost(t *testing.T) {
	assert.True(t, isGoogleContainerRegistryHost("gcr.io"))
	assert.True(t, isGoogleContainerRegistryHost("us.gcr.io"))
	assert.True(t, isGoogleContainerRegistryHost("us-west1-docker.pkg.dev"))
	assert.False(t, isGoogleContainerRegistryHost("docker.io"))
	assert.False(t, isGoogleContainerRegistryHost("quay.io"))
	assert.False(t, isGoogleContainerRegistryHost("localhost:5000"))
}
