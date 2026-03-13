package sparklesworker

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/broadinstitute/sparklesworker/consumer"
	"github.com/broadinstitute/sparklesworker/task_queue"
	"github.com/stretchr/testify/assert"
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

type MockIOClient struct {
	uploaded   []string
	downloaded []string
}

func NewMockIOClient() *MockIOClient {
	return &MockIOClient{}
}

func (ioc *MockIOClient) Upload(src string, destURL string) error {
	ioc.uploaded = append(ioc.uploaded, src+" -> "+destURL)
	return nil
}

func (ioc *MockIOClient) UploadBytes(src string, data []byte) error {
	return nil
}

func (ioc *MockIOClient) Download(srcUrl string, destPath string) error {
	ioc.downloaded = append(ioc.downloaded, destPath+" <- "+srcUrl)
	return nil
}

func (ioc *MockIOClient) DownloadAsBytes(srcUrl string) ([]byte, error) {
	return make([]byte, 0), nil
}

func (ioc *MockIOClient) IsExists(url string) (bool, error) {
	return true, nil
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
	log.Printf("Testcommpla\n")
}

func TestIOClient(t *testing.T) {
	// TODO: FIX
	destURL := "gs://broad-achilles-kubeque/test/TestIOClient"

	ctx := context.Background()
	ioc, err := NewIOClient(ctx, http.DefaultClient)
	assert.Nil(t, err)

	sourceFile, _ := ioutil.TempFile("", "sample")
	destFile := sourceFile.Name() + ".dl"
	log.Printf("destFile=%v\n", destFile)
	sourceContents := []byte("test")
	assert.Nil(t, ioutil.WriteFile(sourceFile.Name(), sourceContents, 0700))

	assert.Nil(t, ioc.Upload(sourceFile.Name(), destURL))
	assert.Nil(t, ioc.Download(destURL, destFile))

	destContents, _ := ioutil.ReadFile(destFile)

	assert.Equal(t, destContents, sourceContents)
}

func TestExecute(t *testing.T) {
	urlprefix := "gs://broad-achilles-kubeque/test/testExecute"

	tmpdir, err := ioutil.TempDir("", "testTmp")
	assert.Nil(t, err)

	tasksDir := path.Join(tmpdir, "tasks")
	cacheDir := path.Join(tmpdir, "cache")
	rootDir := tmpdir

	ctx := context.Background()
	ioc, err := NewIOClient(ctx, http.DefaultClient)
	assert.Nil(t, err)

	spec := &task_queue.TaskSpec{
		WorkingDir:       ".",
		PreExecScript:    "ls",
		Command:          "bash -c 'echo hello'",
		CommandResultURL: urlprefix + "result.json",
		StdoutURL:        urlprefix + "stdout.txt",
		Uploads:          &task_queue.UploadSpec{},
	}

	retcode, err := consumer.ExecuteTask(ctx, ioc, consumer.AetherConfig{}, "test-task", spec, rootDir, cacheDir, tasksDir, nil)
	assert.Nil(t, err)
	assert.Equal(t, "0", retcode)
}
