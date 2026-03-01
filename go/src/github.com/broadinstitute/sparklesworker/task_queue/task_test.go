package task_queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskSaveLoadRoundTrip(t *testing.T) {
	original := &Task{
		TaskID:    "job1.0",
		TaskIndex: 0,
		JobID:     "job1",
		Status:    StatusPending,
		TaskSpec: &TaskSpec{
			Command: "echo hello",
			Parameters: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			Downloads: []*TaskDownload{
				{SrcURL: "gs://bucket/file.txt", Dst: "file.txt", IsCASKey: true, Executable: true},
			},
			Uploads: &UploadSpec{
				DstURL:          "gs://bucket/output",
				IncludePatterns: []string{"*.txt"},
				ExcludePatterns: []string{"*.tmp"},
			},
			DockerImage:      "ubuntu:22.04",
			StdoutURL:        "gs://bucket/stdout.txt",
			CommandResultURL: "gs://bucket/result.json",
		},
		History: []*TaskHistory{
			{
				Timestamp:       1234567890.0,
				Status:          StatusPending,
				OwnedByWorkerID: "worker-1",
			},
		},
		Version:     3,
		ExitCode:    "0",
		Cluster:     "my-cluster",
		LastUpdated: 1234567890.0,
	}

	props, err := original.Save()
	require.NoError(t, err)

	loaded := &Task{}
	require.NoError(t, loaded.Load(props))

	require.NotNil(t, loaded.TaskSpec)
	assert.Equal(t, original.TaskSpec.Command, loaded.TaskSpec.Command)
	assert.Equal(t, original.TaskSpec.Parameters, loaded.TaskSpec.Parameters)
	assert.Equal(t, original.TaskSpec.DockerImage, loaded.TaskSpec.DockerImage)
	assert.Equal(t, original.TaskSpec.StdoutURL, loaded.TaskSpec.StdoutURL)
	assert.Equal(t, original.TaskSpec.CommandResultURL, loaded.TaskSpec.CommandResultURL)

	require.Len(t, loaded.TaskSpec.Downloads, 1)
	assert.Equal(t, original.TaskSpec.Downloads[0].SrcURL, loaded.TaskSpec.Downloads[0].SrcURL)
	assert.Equal(t, original.TaskSpec.Downloads[0].Dst, loaded.TaskSpec.Downloads[0].Dst)
	assert.Equal(t, original.TaskSpec.Downloads[0].IsCASKey, loaded.TaskSpec.Downloads[0].IsCASKey)
	assert.Equal(t, original.TaskSpec.Downloads[0].Executable, loaded.TaskSpec.Downloads[0].Executable)

	require.NotNil(t, loaded.TaskSpec.Uploads)
	assert.Equal(t, original.TaskSpec.Uploads.DstURL, loaded.TaskSpec.Uploads.DstURL)
	assert.Equal(t, original.TaskSpec.Uploads.IncludePatterns, loaded.TaskSpec.Uploads.IncludePatterns)
	assert.Equal(t, original.TaskSpec.Uploads.ExcludePatterns, loaded.TaskSpec.Uploads.ExcludePatterns)

	assert.Equal(t, original.Status, loaded.Status)
	assert.Equal(t, original.Version, loaded.Version)
	assert.Equal(t, original.Cluster, loaded.Cluster)
	assert.Equal(t, original.ExitCode, loaded.ExitCode)
	assert.Equal(t, original.LastUpdated, loaded.LastUpdated)
	assert.Equal(t, original.JobID, loaded.JobID)

	require.Len(t, loaded.History, 1)
	assert.Equal(t, original.History[0].Status, loaded.History[0].Status)
	assert.Equal(t, original.History[0].Timestamp, loaded.History[0].Timestamp)
	assert.Equal(t, original.History[0].OwnedByWorkerID, loaded.History[0].OwnedByWorkerID)
}

func TestTaskSaveLoadNilTaskSpec(t *testing.T) {
	original := &Task{
		TaskID:   "job1.0",
		JobID:    "job1",
		Status:   StatusPending,
		TaskSpec: nil,
	}

	props, err := original.Save()
	require.NoError(t, err)

	loaded := &Task{}
	require.NoError(t, loaded.Load(props))

	assert.Nil(t, loaded.TaskSpec)
}
