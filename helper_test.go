package kubequeconsume

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

/*
test
func executeTaskInDir(workdir string, spec *TaskSpec) error {
local_to_url_mapping = resolveUploads(workdir, spec.uploads, downloaded)

gcp ops:
	make files, and upload via:
	uploadMapped(...)
	download files to different dir:
	func downloadAll(workdir string, downloads []*TaskDownload) (error, stringset) {

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

func TestResolveUploads(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "testTmp")
	check(err)
	workdir := path.Join(tempDir, "work")
	err = os.Mkdir(workdir, 0700)
	check(err)

	// make a few files
	downloadedFile := path.Join(workdir, "downloadedFile.txt")
	err = ioutil.WriteFile(downloadedFile, make([]byte, 1), 0644)
	check(err)

	// one where we pretend it was downloaded
	downloaded := make(stringset)
	downloaded[downloadedFile] = true

	// and one which is new
	toUploadFile := path.Join(workdir, "needUpload.txt")
	err = ioutil.WriteFile(toUploadFile, make([]byte, 1), 0644)
	check(err)

	log.Printf("toUploadFile=%v\n", toUploadFile)

	uploadSpecs := []*TaskUpload{&TaskUpload{DstURL: "gs://fake/dest", SrcWildcard: "*"}}
	filesToUpload, err := resolveUploads(workdir, uploadSpecs, downloaded)
	assert.Nil(t, err)

	assert.Len(t, filesToUpload, 1)

	ioc := NewMockIOClient()
	uploadMapped(ioc, filesToUpload)

	assert.Len(t, ioc.uploaded, 1)
	assert.Equal(t, toUploadFile+" -> gs://fake/dest/needUpload.txt", ioc.uploaded[0])
	log.Printf("Testcommpla\n")
}

func TestIOClient(t *testing.T) {
	// TODO: FIX
	destURL := "gs://broad-achilles-kubeque/test/TestIOClient"

	ctx := context.Background()
	ioc, err := NewIOClient(ctx)
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

	workdir := path.Join(tmpdir, "work")
	assert.Nil(t, os.Mkdir(workdir, 0700))
	cachedir := path.Join(tmpdir, "cache")

	ctx := context.Background()
	ioc, err := NewIOClient(ctx)
	assert.Nil(t, err)

	spec := &TaskSpec{
		WorkingDir:       ".",
		PreExecScript:    "ls",
		Command:          "bash -c 'echo hello'",
		CommandResultURL: urlprefix + "result.json",
		StdoutURL:        urlprefix + "stdout.txt"}

	assert.Nil(t, executeTaskInDir(ioc, workdir, spec, cachedir))
}
