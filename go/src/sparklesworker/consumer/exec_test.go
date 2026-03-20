package consumer

import (
	"encoding/json"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func captureWriteResult() (func([]byte) error, *[]byte) {
	var captured []byte
	return func(data []byte) error {
		captured = data
		return nil
	}, &captured
}

func TestWriteResultFile(t *testing.T) {
	workdir := t.TempDir()

	startTime := time.Unix(1000, 0)
	endTime := time.Unix(1010, 0)
	dlStart := time.Unix(990, 0)
	dlEnd := time.Unix(995, 0)
	ulStart := time.Unix(1011, 0)
	ulEnd := time.Unix(1015, 0)

	execResult := &ExecResult{
		Rusage:    &syscall.Rusage{Maxrss: 4096, Inblock: 10, Oublock: 20},
		Status:    "0",
		StartTime: startTime,
		EndTime:   endTime,
		ExePath:   "/bin/sh",
		ExeArgs:   []string{"/bin/sh", "-c", "echo hello"},
	}

	dlStats := &TransferStats{Bytes: 1024, FileCount: 2, StartTime: dlStart, EndTime: dlEnd}
	ulStats := &TransferStats{Bytes: 512, FileCount: 1, StartTime: ulStart, EndTime: ulEnd}
	parameters := map[string]string{"key": "value"}
	manifestKey := "sha256:abc123"

	writeResult, captured := captureWriteResult()
	err := writeResultFile(writeResult, "0", execResult, workdir, manifestKey, parameters, dlStats, ulStats)
	require.NoError(t, err)

	resultBytes := *captured
	require.NotNil(t, resultBytes, "result file was not written")

	var result ResultStruct
	require.NoError(t, json.Unmarshal(resultBytes, &result))

	assert.Equal(t, "/bin/sh", result.ExePath)
	assert.Equal(t, []string{"/bin/sh", "-c", "echo hello"}, result.ExeArgs)
	assert.Equal(t, "0", result.ReturnCode)
	assert.Equal(t, parameters, result.Parameters)
	assert.Equal(t, manifestKey, result.ManifestKey)

	require.NotNil(t, result.Usage)
	assert.Equal(t, toUnixFloat(startTime), result.Usage.StartTime)
	assert.Equal(t, toUnixFloat(endTime), result.Usage.EndTime)
	assert.InDelta(t, 10.0, result.Usage.ElapsedTime, 0.001)
	assert.Equal(t, int64(4096), result.Usage.MaxMemorySize)
	assert.Equal(t, int64(10), result.Usage.BlockInputOps)
	assert.Equal(t, int64(20), result.Usage.BlockOutputOps)

	assert.Equal(t, int64(1024), result.Usage.DownloadBytes)
	assert.Equal(t, 2, result.Usage.DownloadFileCount)
	assert.Equal(t, toUnixFloat(dlStart), result.Usage.DownloadStartTime)
	assert.Equal(t, toUnixFloat(dlEnd), result.Usage.DownloadEndTime)
	assert.InDelta(t, 5.0, result.Usage.DownloadElapsed, 0.001)

	assert.Equal(t, int64(512), result.Usage.UploadBytes)
	assert.Equal(t, 1, result.Usage.UploadFileCount)
	assert.Equal(t, toUnixFloat(ulStart), result.Usage.UploadStartTime)
	assert.Equal(t, toUnixFloat(ulEnd), result.Usage.UploadEndTime)
	assert.InDelta(t, 4.0, result.Usage.UploadElapsed, 0.001)

	// Verify JSON field names match the expected contract
	var raw map[string]any
	require.NoError(t, json.Unmarshal(resultBytes, &raw))
	assert.Contains(t, raw, "exe_path")
	assert.Contains(t, raw, "exe_args")
	assert.Contains(t, raw, "return_code")
	assert.Contains(t, raw, "manifest_key")
	usage := raw["resource_usage"].(map[string]any)
	assert.Contains(t, usage, "download_bytes")
	assert.Contains(t, usage, "download_elapsed")
	assert.Contains(t, usage, "upload_bytes")
	assert.Contains(t, usage, "upload_elapsed")

	// Verify "command" is no longer present
	assert.NotContains(t, raw, "command")
}

func TestWriteResultFileUsesExePathFromExecResult(t *testing.T) {
	workdir := t.TempDir()

	execResult := &ExecResult{
		Rusage:    &syscall.Rusage{},
		Status:    "1",
		StartTime: time.Now(),
		EndTime:   time.Now(),
		ExePath:   "docker",
		ExeArgs:   []string{"docker", "run", "--rm", "ubuntu:22.04", "/bin/sh", "-c", "false"},
	}

	writeResult, captured := captureWriteResult()
	err := writeResultFile(writeResult, "1", execResult, workdir,
		"", nil, &TransferStats{}, &TransferStats{})
	require.NoError(t, err)

	var result ResultStruct
	require.NoError(t, json.Unmarshal(*captured, &result))

	assert.Equal(t, "docker", result.ExePath)
	assert.Equal(t, execResult.ExeArgs, result.ExeArgs)
	assert.Equal(t, "1", result.ReturnCode)
}

func TestWriteResultFileJsonFieldNames(t *testing.T) {
	workdir := t.TempDir()

	writeResult, captured := captureWriteResult()
	err := writeResultFile(writeResult, "0",
		&ExecResult{Rusage: &syscall.Rusage{}, StartTime: time.Now(), EndTime: time.Now()},
		workdir, "", nil,
		&TransferStats{}, &TransferStats{})
	require.NoError(t, err)

	// Unmarshal into a raw map to check field names without relying on struct tags
	var raw map[string]any
	require.NoError(t, json.Unmarshal(*captured, &raw))

	expectedTopLevel := []string{"exe_path", "exe_args", "return_code", "resource_usage"}
	for _, field := range expectedTopLevel {
		assert.Contains(t, raw, field, "missing top-level field %q", field)
	}

	usage := raw["resource_usage"].(map[string]any)
	expectedUsage := []string{
		"user_cpu_time", "system_cpu_time", "max_memory_size",
		"start_time", "end_time", "elapsed_time",
		"download_bytes", "download_file_count", "download_start_time", "download_end_time", "download_elapsed",
		"upload_bytes", "upload_file_count", "upload_start_time", "upload_end_time", "upload_elapsed",
	}
	for _, field := range expectedUsage {
		assert.Contains(t, usage, field, "missing resource_usage field %q", field)
	}
}
