package v100

import (
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
