package consumer

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/task_queue"
	aetherclient "github.com/pgm/aether/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- helpers ----------------------------------------------------------------

// stdoutFile creates a temp file suitable for use as a stdout capture target.
func stdoutFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdout-")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

// readFile returns the full contents of a file as a string.
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}

// emptyAetherRoot stages an empty filesystem into a local aether store and
// returns the manifest ref ("sha256:…") for use as AetherFSRoot.
func emptyAetherRoot(t *testing.T, aetherDir string) string {
	t.Helper()
	ctx := context.Background()
	result, err := aetherclient.MakeFilesystem(ctx, aetherclient.MakeFilesystemOptions{
		Root:  aetherDir,
		Files: nil,
	})
	require.NoError(t, err)
	return "sha256:" + result.ManifestKey
}

// memCache is a simple in-memory TaskCache for testing.
type memCache struct {
	mu      sync.Mutex
	entries map[string]*task_queue.CachedTaskEntry
}

func newMemCache() *memCache {
	return &memCache{entries: make(map[string]*task_queue.CachedTaskEntry)}
}

func (c *memCache) GetCachedEntry(_ context.Context, key string) (*task_queue.CachedTaskEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[key], nil
}

func (c *memCache) SetCachedEntry(_ context.Context, entry *task_queue.CachedTaskEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[entry.ID] = entry
	return nil
}

// ---- execCommand ------------------------------------------------------------

func TestExecCommand_CapturesStdout(t *testing.T) {
	tmp := t.TempDir()
	f := stdoutFile(t)

	result, err := execCommand(context.Background(), []string{"/bin/sh", "-c", "echo hello"}, tmp, tmp, f, "")
	require.NoError(t, err)

	f.Seek(0, io.SeekStart)
	assert.Equal(t, "0", result.Status)
	assert.Contains(t, readFile(t, f.Name()), "hello")
}

func TestExecCommand_NonZeroExitCode(t *testing.T) {
	tmp := t.TempDir()
	f := stdoutFile(t)

	result, err := execCommand(context.Background(), []string{"/bin/sh", "-c", "exit 42"}, tmp, tmp, f, "")
	require.NoError(t, err)
	assert.Equal(t, "42", result.Status)
}

func TestExecCommand_PathResolution(t *testing.T) {
	// "sh" is a bare name — exec.LookPath should resolve it via PATH.
	tmp := t.TempDir()
	f := stdoutFile(t)

	result, err := execCommand(context.Background(), []string{"sh", "-c", "echo ok"}, tmp, tmp, f, "")
	require.NoError(t, err)
	assert.Equal(t, "0", result.Status)
	assert.Contains(t, readFile(t, f.Name()), "ok")
}

func TestExecCommand_UnknownExecutable(t *testing.T) {
	tmp := t.TempDir()
	f := stdoutFile(t)

	_, err := execCommand(context.Background(), []string{"no-such-binary-xyz"}, tmp, tmp, f, "")
	require.Error(t, err)
}

func TestExecCommand_ResultFields(t *testing.T) {
	tmp := t.TempDir()
	f := stdoutFile(t)

	before := time.Now()
	result, err := execCommand(context.Background(), []string{"/bin/sh", "-c", "true"}, tmp, tmp, f, "")
	after := time.Now()

	require.NoError(t, err)
	assert.Equal(t, "0", result.Status)
	assert.NotEmpty(t, result.ExePath)
	assert.NotNil(t, result.Rusage)
	assert.True(t, !result.StartTime.Before(before))
	assert.True(t, !result.EndTime.After(after.Add(time.Second)))
}

// ---- ResolveUploads ---------------------------------------------------------

func TestResolveUploads_IncludePattern(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.log"), []byte("b"), 0644))

	files, err := ResolveUploads(dir, &task_queue.UploadSpec{IncludePatterns: []string{"*.txt"}})
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, filepath.Join(dir, "a.txt"), files[0])
}

func TestResolveUploads_ExcludePattern(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("k"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "drop.txt"), []byte("d"), 0644))

	files, err := ResolveUploads(dir, &task_queue.UploadSpec{
		IncludePatterns: []string{"*.txt"},
		ExcludePatterns: []string{"drop.txt"},
	})
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, filepath.Join(dir, "keep.txt"), files[0])
}

func TestResolveUploads_NilSpecDefaultsToAll(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f1"), []byte("1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f2"), []byte("2"), 0644))

	files, err := ResolveUploads(dir, nil)
	require.NoError(t, err)
	assert.Len(t, files, 2)
}

func TestResolveUploads_SkipsDirectories(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "subdir"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file.txt"), []byte("x"), 0644))

	files, err := ResolveUploads(dir, &task_queue.UploadSpec{IncludePatterns: []string{"*"}})
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, filepath.Join(dir, "file.txt"), files[0])
}

// ---- prepareTaskDirectories -------------------------------------------------

func TestPrepareTaskDirectories_CreatesStructure(t *testing.T) {
	tmp := t.TempDir()
	dirs, err := prepareTaskDirectories(tmp, path.Join(tmp, "tasks"), path.Join(tmp, "cache"), ".")
	require.NoError(t, err)

	for _, p := range []string{dirs.logDir, dirs.workDir, dirs.cwdDir} {
		fi, err := os.Stat(p)
		require.NoError(t, err, "expected directory %s to exist", p)
		assert.True(t, fi.IsDir())
	}
	assert.True(t, strings.HasSuffix(dirs.stdoutPath, "stdout.txt"))
	assert.True(t, strings.HasSuffix(dirs.resultPath, "result.json"))
}

func TestPrepareTaskDirectories_RejectsAbsoluteWorkingDir(t *testing.T) {
	tmp := t.TempDir()
	_, err := prepareTaskDirectories(tmp, path.Join(tmp, "tasks"), path.Join(tmp, "cache"), "/absolute/path")
	require.Error(t, err)
}

// ---- computeCacheKey --------------------------------------------------------

func TestComputeCacheKey_Deterministic(t *testing.T) {
	spec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", "echo hi"},
		DockerImage:  "",
		AetherFSRoot: "sha256:abc",
	}
	k1, err := computeCacheKey(spec)
	require.NoError(t, err)
	k2, err := computeCacheKey(spec)
	require.NoError(t, err)
	assert.Equal(t, k1, k2)
}

func TestComputeCacheKey_ChangesWithInputs(t *testing.T) {
	base := &task_queue.TaskSpec{Command: []string{"echo", "hi"}, AetherFSRoot: "sha256:aaa"}
	k1, _ := computeCacheKey(base)

	diffCmd := &task_queue.TaskSpec{Command: []string{"echo", "bye"}, AetherFSRoot: "sha256:aaa"}
	k2, _ := computeCacheKey(diffCmd)

	diffRoot := &task_queue.TaskSpec{Command: []string{"echo", "hi"}, AetherFSRoot: "sha256:bbb"}
	k3, _ := computeCacheKey(diffRoot)

	diffImage := &task_queue.TaskSpec{Command: []string{"echo", "hi"}, DockerImage: "ubuntu", AetherFSRoot: "sha256:aaa"}
	k4, _ := computeCacheKey(diffImage)

	assert.NotEqual(t, k1, k2, "different command should give different key")
	assert.NotEqual(t, k1, k3, "different aether root should give different key")
	assert.NotEqual(t, k1, k4, "different docker image should give different key")
}

// ---- ExecuteTask ------------------------------------------------------------

func TestExecuteTask_BasicRun(t *testing.T) {
	tmp := t.TempDir()
	aetherDir := filepath.Join(tmp, "aether")
	ctx := context.Background()

	spec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", "echo hello-task"},
		AetherFSRoot: emptyAetherRoot(t, aetherDir),
		Uploads:      &task_queue.UploadSpec{},
	}
	cfg := &backend.AetherConfig{Root: aetherDir}

	result, err := ExecuteTask(ctx, cfg, "task-1", "job-1", spec, tmp,
		filepath.Join(tmp, "cache"), filepath.Join(tmp, "tasks"), nil, nil, time.Time{}, &backend.NullEventPublisher{})
	require.NoError(t, err)
	assert.Equal(t, "0", result.RetCode)
	assert.NotEmpty(t, result.LogsKey)
	assert.NotEmpty(t, result.OutputsKey)

	// Export the logs and verify stdout.txt contains the expected output.
	logDir := filepath.Join(tmp, "log-export")
	_, err = aetherclient.Export(ctx, aetherclient.ExportOptions{
		Root:        aetherDir,
		ManifestRef: result.LogsKey,
		Dest:        logDir,
	})
	require.NoError(t, err)
	assert.Contains(t, readFile(t, filepath.Join(logDir, "stdout.txt")), "hello-task")
}

func TestExecuteTask_NonZeroExitCode(t *testing.T) {
	tmp := t.TempDir()
	aetherDir := filepath.Join(tmp, "aether")
	ctx := context.Background()

	spec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", "exit 7"},
		AetherFSRoot: emptyAetherRoot(t, aetherDir),
		Uploads:      &task_queue.UploadSpec{},
	}
	result, err := ExecuteTask(ctx, &backend.AetherConfig{Root: aetherDir}, "task-fail", "job-1", spec, tmp,
		filepath.Join(tmp, "cache"), filepath.Join(tmp, "tasks"), nil, nil, time.Time{}, &backend.NullEventPublisher{})
	require.NoError(t, err)
	assert.Equal(t, "7", result.RetCode)
}

func TestExecuteTask_UploadsOutputFiles(t *testing.T) {
	tmp := t.TempDir()
	aetherDir := filepath.Join(tmp, "aether")
	ctx := context.Background()

	// The command writes a file; the upload spec captures it.
	spec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", "echo output-content > result.txt"},
		AetherFSRoot: emptyAetherRoot(t, aetherDir),
		Uploads:      &task_queue.UploadSpec{IncludePatterns: []string{"*.txt"}},
	}
	result, err := ExecuteTask(ctx, &backend.AetherConfig{Root: aetherDir}, "task-upload", "job-1", spec, tmp,
		filepath.Join(tmp, "cache"), filepath.Join(tmp, "tasks"), nil, nil, time.Time{}, &backend.NullEventPublisher{})
	require.NoError(t, err)
	assert.Equal(t, "0", result.RetCode)

	outDir := filepath.Join(tmp, "out-export")
	_, err = aetherclient.Export(ctx, aetherclient.ExportOptions{
		Root:        aetherDir,
		ManifestRef: result.OutputsKey,
		Dest:        outDir,
	})
	require.NoError(t, err)
	assert.Contains(t, readFile(t, filepath.Join(outDir, "result.txt")), "output-content")
}

func TestExecuteTask_CacheMissThenHit(t *testing.T) {
	tmp := t.TempDir()
	aetherDir := filepath.Join(tmp, "aether")
	ctx := context.Background()
	cache := newMemCache()
	expiry := time.Now().Add(time.Hour)

	spec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", "echo cached"},
		AetherFSRoot: emptyAetherRoot(t, aetherDir),
		Uploads:      &task_queue.UploadSpec{},
	}
	cfg := &backend.AetherConfig{Root: aetherDir}

	// First call — cache miss, task executes.
	r1, err := ExecuteTask(ctx, cfg, "task-a", "job-1", spec, tmp,
		filepath.Join(tmp, "cache"), filepath.Join(tmp, "tasks"), nil, cache, expiry, &backend.NullEventPublisher{})
	require.NoError(t, err)
	assert.Equal(t, "0", r1.RetCode)
	assert.Empty(t, r1.UsedCacheResultFromTaskID)

	// Second call with identical spec — should be a cache hit.
	r2, err := ExecuteTask(ctx, cfg, "task-b", "job-1", spec, tmp,
		filepath.Join(tmp, "cache"), filepath.Join(tmp, "tasks"), nil, cache, expiry, &backend.NullEventPublisher{})
	require.NoError(t, err)
	assert.Equal(t, "task-a", r2.UsedCacheResultFromTaskID)
	assert.Equal(t, r1.LogsKey, r2.LogsKey)
	assert.Equal(t, r1.OutputsKey, r2.OutputsKey)
}

func TestExecuteTask_CacheNotStoredOnFailure(t *testing.T) {
	tmp := t.TempDir()
	aetherDir := filepath.Join(tmp, "aether")
	ctx := context.Background()
	cache := newMemCache()
	expiry := time.Now().Add(time.Hour)

	spec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", "exit 1"},
		AetherFSRoot: emptyAetherRoot(t, aetherDir),
		Uploads:      &task_queue.UploadSpec{},
	}
	cfg := &backend.AetherConfig{Root: aetherDir}

	r1, err := ExecuteTask(ctx, cfg, "task-fail", "job-1", spec, tmp,
		filepath.Join(tmp, "cache"), filepath.Join(tmp, "tasks"), nil, cache, expiry, &backend.NullEventPublisher{})
	require.NoError(t, err)
	assert.Equal(t, "1", r1.RetCode)

	// Second call must not get a cache hit — failure should not be cached.
	r2, err := ExecuteTask(ctx, cfg, "task-fail-2", "job-1", spec, tmp,
		filepath.Join(tmp, "cache"), filepath.Join(tmp, "tasks"), nil, cache, expiry, &backend.NullEventPublisher{})
	require.NoError(t, err)
	assert.Empty(t, r2.UsedCacheResultFromTaskID)
}

func TestExecuteTask_WritesResultFile(t *testing.T) {
	tmp := t.TempDir()
	aetherDir := filepath.Join(tmp, "aether")
	ctx := context.Background()

	spec := &task_queue.TaskSpec{
		Command:      []string{"/bin/sh", "-c", "echo hi"},
		AetherFSRoot: emptyAetherRoot(t, aetherDir),
		Uploads:      &task_queue.UploadSpec{},
		Parameters:   map[string]string{"env": "test"},
	}
	result, err := ExecuteTask(ctx, &backend.AetherConfig{Root: aetherDir}, "task-result", "job-1", spec, tmp,
		filepath.Join(tmp, "cache"), filepath.Join(tmp, "tasks"), nil, nil, time.Time{}, &backend.NullEventPublisher{})
	require.NoError(t, err)

	// Export logs and parse result.json.
	logDir := filepath.Join(tmp, "log-export")
	_, err = aetherclient.Export(ctx, aetherclient.ExportOptions{
		Root:        aetherDir,
		ManifestRef: result.LogsKey,
		Dest:        logDir,
	})
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(logDir, "result.json"))
	require.NoError(t, err)
	var rs ResultStruct
	require.NoError(t, json.Unmarshal(data, &rs))
	assert.Equal(t, "0", rs.ReturnCode)
	assert.Equal(t, map[string]string{"env": "test"}, rs.Parameters)
}

// ---- writeResultFile (existing test kept, extended) -------------------------

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

	resultPath := path.Join(workdir, "results.json")
	err := writeResultFile(resultPath, "0", execResult, workdir, manifestKey, parameters, dlStats, ulStats)
	require.NoError(t, err)

	resultsFile, err := os.Open(resultPath)
	require.NoError(t, err)
	resultBytes, err := io.ReadAll(resultsFile)

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
