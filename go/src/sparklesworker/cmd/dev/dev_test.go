package dev

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestDevSubmitHappyPath(t *testing.T) {
	mr := miniredis.RunT(t)

	tmp := t.TempDir()
	aetherRoot := filepath.Join(tmp, "aether")
	logExportDir := filepath.Join(tmp, "log-export")

	req := &DevSubmitRequest{
		RedisAddr:   mr.Addr(),
		AetherRoot:  aetherRoot,
		Dir:         filepath.Join(tmp, "worker"),
		Name:        "test-echo-job",
		ClusterID:   "local",
		Command:     "echo hello",
		ExportLogTo: logExportDir,
	}

	err := ExecuteSubmit(req)
	require.NoError(t, err)

	stdoutBytes, err := os.ReadFile(filepath.Join(logExportDir, "stdout.txt"))
	require.NoError(t, err)
	require.True(t, strings.Contains(string(stdoutBytes), "hello"),
		"stdout.txt should contain 'hello', got: %q", string(stdoutBytes))
}
