package v100

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTempCSV(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "params.csv")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))
	return path
}

func TestLoadTemplateParameters(t *testing.T) {
	path := writeTempCSV(t, "a,b\n1,2\n3,4\n")
	rows, err := loadTemplateParameters(path)
	require.NoError(t, err)
	assert.Equal(t, []map[string]string{
		{"a": "1", "b": "2"},
		{"a": "3", "b": "4"},
	}, rows)
}

func TestApplyTaskTemplate_ExpandsIntoTasks(t *testing.T) {
	doc := map[string]any{
		"workpool_id": "pool-1",
		"task_template": map[string]any{
			"image":   "image-{{a}}",
			"command": []any{"cmd", "{{b}}"},
		},
	}
	params := []map[string]string{
		{"a": "1", "b": "2"},
		{"a": "3", "b": "4"},
	}

	err := applyTaskTemplate(doc, params, true)
	require.NoError(t, err)

	_, hasTemplate := doc["task_template"]
	assert.False(t, hasTemplate, "task_template should be removed")

	tasks, ok := doc["tasks"].([]any)
	require.True(t, ok)
	require.Len(t, tasks, 2)
	assert.Equal(t, map[string]any{"image": "image-1", "command": []any{"cmd", "2"}}, tasks[0])
	assert.Equal(t, map[string]any{"image": "image-3", "command": []any{"cmd", "4"}}, tasks[1])
}

func TestApplyTaskTemplate_DefaultParamsProduceOneTask(t *testing.T) {
	doc := map[string]any{
		"task_template": map[string]any{"image": "fixed-image"},
	}
	// Default TemplateParameters when --params isn't given.
	err := applyTaskTemplate(doc, []map[string]string{nil}, false)
	require.NoError(t, err)

	tasks, ok := doc["tasks"].([]any)
	require.True(t, ok)
	require.Len(t, tasks, 1)
	assert.Equal(t, map[string]any{"image": "fixed-image"}, tasks[0])
}

func TestApplyTaskTemplate_BothTemplateAndTasksSet_Errors(t *testing.T) {
	doc := map[string]any{
		"task_template": map[string]any{"image": "x"},
		"tasks":         []any{map[string]any{"image": "y"}},
	}
	err := applyTaskTemplate(doc, []map[string]string{nil}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "task_template")
	assert.Contains(t, err.Error(), "tasks")
}

func TestApplyTaskTemplate_ParamsWithoutTemplate_Errors(t *testing.T) {
	doc := map[string]any{"tasks": []any{map[string]any{"image": "y"}}}
	err := applyTaskTemplate(doc, []map[string]string{{"a": "1"}}, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--params")
}

func TestApplyTaskTemplate_NoTemplateNoParams_Noop(t *testing.T) {
	doc := map[string]any{"tasks": []any{map[string]any{"image": "y"}}}
	err := applyTaskTemplate(doc, []map[string]string{nil}, false)
	require.NoError(t, err)
	assert.Equal(t, []any{map[string]any{"image": "y"}}, doc["tasks"])
}

func TestApplyTaskTemplate_MissingParamColumn_Errors(t *testing.T) {
	doc := map[string]any{
		"task_template": map[string]any{"image": "image-{{c}}"},
	}
	err := applyTaskTemplate(doc, []map[string]string{{"a": "1", "b": "2"}}, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "c")
}

func TestSetJobNameIfMissing(t *testing.T) {
	doc := map[string]any{}
	setJobNameIfMissing(doc, "/some/path/my-job.json")
	assert.Equal(t, "my-job", doc["name"])

	doc2 := map[string]any{"name": "explicit"}
	setJobNameIfMissing(doc2, "/some/path/my-job.json")
	assert.Equal(t, "explicit", doc2["name"])
}

// fakeStage returns a stageFunc that rewrites source into
// "gs://staged/<prefix>/<source>" and records every call it receives.
func fakeStage(calls *[]string) stageFunc {
	return func(ctx context.Context, prefix, localPath string) (string, error) {
		*calls = append(*calls, fmt.Sprintf("%s|%s", prefix, localPath))
		return fmt.Sprintf("gs://staged/%s/%s", prefix, localPath), nil
	}
}

func TestStageLocalFiles_JobLevelOnly(t *testing.T) {
	doc := map[string]any{
		"gcs_staging_prefix": "gs://bucket/staging",
		"filesToLocalize": []any{
			map[string]any{"source": "local.py", "destination": "local.py"},
			map[string]any{"source": "gs://already/there", "destination": "there"},
		},
	}

	var calls []string
	err := stageLocalFiles(context.Background(), doc, fakeStage(&calls))
	require.NoError(t, err)

	files := doc["filesToLocalize"].([]any)
	assert.Equal(t, "gs://staged/gs://bucket/staging/local.py", files[0].(map[string]any)["source"])
	assert.Equal(t, "gs://already/there", files[1].(map[string]any)["source"])
	assert.Equal(t, []string{"gs://bucket/staging|local.py"}, calls)

	_, hasPrefix := doc["gcs_staging_prefix"]
	assert.False(t, hasPrefix, "gcs_staging_prefix must be stripped from doc")
}

func TestStageLocalFiles_TaskLevelOnly(t *testing.T) {
	doc := map[string]any{
		"gcs_staging_prefix": "gs://bucket/staging",
		"tasks": []any{
			map[string]any{
				"filesToLocalize": []any{
					map[string]any{"source": "task0.py", "destination": "task0.py"},
				},
			},
			map[string]any{
				"filesToLocalize": []any{
					map[string]any{"source": "task1.py", "destination": "task1.py"},
				},
			},
		},
	}

	var calls []string
	err := stageLocalFiles(context.Background(), doc, fakeStage(&calls))
	require.NoError(t, err)

	tasks := doc["tasks"].([]any)
	f0 := tasks[0].(map[string]any)["filesToLocalize"].([]any)[0].(map[string]any)
	f1 := tasks[1].(map[string]any)["filesToLocalize"].([]any)[0].(map[string]any)
	assert.Equal(t, "gs://staged/gs://bucket/staging/task0.py", f0["source"])
	assert.Equal(t, "gs://staged/gs://bucket/staging/task1.py", f1["source"])
}

func TestStageLocalFiles_JobAndTaskLevel(t *testing.T) {
	doc := map[string]any{
		"gcs_staging_prefix": "gs://bucket/staging",
		"filesToLocalize": []any{
			map[string]any{"source": "job.py", "destination": "job.py"},
		},
		"tasks": []any{
			map[string]any{
				"filesToLocalize": []any{
					map[string]any{"source": "task0.py", "destination": "task0.py"},
				},
			},
		},
	}

	var calls []string
	err := stageLocalFiles(context.Background(), doc, fakeStage(&calls))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"gs://bucket/staging|job.py",
		"gs://bucket/staging|task0.py",
	}, calls)
}

func TestStageLocalFiles_MissingPrefixWithLocalSource_Errors(t *testing.T) {
	doc := map[string]any{
		"filesToLocalize": []any{
			map[string]any{"source": "local.py", "destination": "local.py"},
		},
	}

	var calls []string
	err := stageLocalFiles(context.Background(), doc, fakeStage(&calls))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gcs_staging_prefix")
	assert.Empty(t, calls)
}

func TestStageLocalFiles_GCSSourcesUntouched_NoPrefixNeeded(t *testing.T) {
	doc := map[string]any{
		"filesToLocalize": []any{
			map[string]any{"source": "gs://already/there", "destination": "there"},
		},
	}

	var calls []string
	err := stageLocalFiles(context.Background(), doc, fakeStage(&calls))
	require.NoError(t, err)
	assert.Empty(t, calls)
}

func TestStageLocalFiles_PrefixAlwaysStrippedEvenIfUnused(t *testing.T) {
	doc := map[string]any{
		"gcs_staging_prefix": "gs://bucket/staging",
		"filesToLocalize": []any{
			map[string]any{"source": "gs://already/there", "destination": "there"},
		},
	}

	var calls []string
	err := stageLocalFiles(context.Background(), doc, fakeStage(&calls))
	require.NoError(t, err)

	_, hasPrefix := doc["gcs_staging_prefix"]
	assert.False(t, hasPrefix)
}
