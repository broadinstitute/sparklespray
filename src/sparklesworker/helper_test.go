package sparklesworker

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/broadinstitute/sparklesworker/backend"
	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/task_queue"
	aetherclient "github.com/pgm/aether/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
test
func executeTaskInDir(workdir string, spec *consumer.TaskSpec) error {
local_to_url_mapping = consumer.ResolveUploads(workdir, spec.uploads, downloaded)

gcp ops:

	make files, and upload via:
	consumer.UploadMapped(...)
	download files to different dir:
	func downloadAll(workdir string, downloads []*consumer.TaskDownload) (error, consumer.Stringset) {
*/
func check(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func TestResolveUploads(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "testTmp")
	check(err)
	workdir := path.Join(tempDir, "work")
	err = os.Mkdir(workdir, 0700)
	check(err)

	file1 := path.Join(workdir, "file1.txt")
	err = ioutil.WriteFile(file1, make([]byte, 1), 0644)
	check(err)

	file2 := path.Join(workdir, "file2.txt")
	err = ioutil.WriteFile(file2, make([]byte, 1), 0644)
	check(err)

	log.Printf("file1=%v file2=%v\n", file1, file2)

	uploadSpec := &task_queue.UploadSpec{IncludePatterns: []string{"*"}}
	filesToUpload, err := consumer.ResolveUploads(workdir, uploadSpec)
	assert.Nil(t, err)

	assert.Len(t, filesToUpload, 2)
}

func TestExecute(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "testTmp")
	require.NoError(t, err)

	tasksDir := path.Join(tmpdir, "tasks")
	cacheDir := path.Join(tmpdir, "cache")
	rootDir := tmpdir

	ctx := context.Background()
	assert.Nil(t, err)

	aetherConfig := &backend.AetherConfig{Root: path.Join(tmpdir, "aether")}

	mkfsResult, err := aetherclient.MakeFilesystem(ctx, aetherclient.MakeFilesystemOptions{
		Root:            aetherConfig.Root,
		Files:           []aetherclient.FileInput{},
		MaxSizeToBundle: aetherConfig.MaxSizeToBundle,
		MaxBundleSize:   aetherConfig.MaxBundleSize,
		Workers:         aetherConfig.Workers,
	})
	require.NoError(t, err)

	spec := &task_queue.TaskSpec{
		WorkingDir:    ".",
		PreExecScript: "ls",
		Command:       []string{"/bin/sh", "-c", "echo hello"},
		Uploads:       &task_queue.UploadSpec{},
		AetherFSRoot:  "sha256:" + mkfsResult.ManifestKey}
	execResult, err := consumer.ExecuteTask(ctx, aetherConfig, "test-task", "job-1", spec, rootDir, cacheDir, tasksDir, nil, nil, time.Time{}, &backend.NullEventPublisher{})
	assert.Nil(t, err)
	assert.Equal(t, "0", execResult.RetCode)
}
