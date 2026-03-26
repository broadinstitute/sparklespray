package dev

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestDevSubmitHappyPath(t *testing.T) {
	mr := miniredis.RunT(t)

	tmp := t.TempDir()
	aetherRoot := filepath.Join(tmp, "aether")

	baseReq := DevSubmitRequest{
		RedisAddr:      mr.Addr(),
		AetherRoot:     aetherRoot,
		Dir:            filepath.Join(tmp, "worker"),
		ClusterID:      "local",
		Command:        "echo hello",
		RunLoopMaxWait: 100 * time.Millisecond,
	}

	// --- First submission ---
	req1 := baseReq
	req1.Name = "test-echo-job-1"
	req1.ExportLogTo = filepath.Join(tmp, "log-export-1")

	task1, err := ExecuteSubmit(&req1)
	require.NoError(t, err)
	require.Empty(t, task1.UsedCacheResultFromTaskID, "first task should not have a cache hit")

	stdout1, err := os.ReadFile(filepath.Join(req1.ExportLogTo, "stdout.txt"))
	require.NoError(t, err)
	require.True(t, strings.Contains(string(stdout1), "hello"),
		"stdout.txt should contain 'hello', got: %q", string(stdout1))

	// --- Second submission (same command — should be a cache hit) ---
	req2 := baseReq
	req2.Name = "test-echo-job-2"
	req2.ExportLogTo = filepath.Join(tmp, "log-export-2")

	task2, err := ExecuteSubmit(&req2)
	require.NoError(t, err)
	require.Equal(t, task1.TaskID, task2.UsedCacheResultFromTaskID,
		"second task should report a cache hit from the first task")

	stdout2, err := os.ReadFile(filepath.Join(req2.ExportLogTo, "stdout.txt"))
	require.NoError(t, err)
	require.True(t, strings.Contains(string(stdout2), "hello"),
		"stdout.txt should contain 'hello', got: %q", string(stdout2))
}
