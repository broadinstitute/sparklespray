package v100

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cbroglie/mustache"
	"github.com/urfave/cli"
)

// submitJobResponse mirrors openapi.yaml's SubmitJobResponse schema.
type submitJobResponse struct {
	ID string `json:"id"`
}

// submitAPIError mirrors openapi.yaml's Error schema.
type submitAPIError struct {
	Code  string `json:"code"`
	Error string `json:"error"`
}

// setJobNameIfMissing sets doc["name"] to the basename of jobFile (extension
// dropped) if the "name" property is absent or empty.
func setJobNameIfMissing(doc map[string]any, jobFile string) {
	if name, ok := doc["name"].(string); !ok || name == "" {
		base := filepath.Base(jobFile)
		base = strings.TrimSuffix(base, filepath.Ext(base))
		doc["name"] = base
	}
}

// loadTemplateParameters reads a CSV file into a slice of row maps, keyed by
// the column names in the header row.
func loadTemplateParameters(path string) ([]map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("reading header row: %w", err)
	}

	var rows []map[string]string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		row := make(map[string]string, len(header))
		for i, h := range header {
			if i < len(record) {
				row[h] = record[i]
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// renderTemplateString evaluates s as a Mustache template against params.
// Missing variables are treated as errors rather than rendered as empty
// strings, since a missing param column is almost always a typo.
func renderTemplateString(s string, params map[string]string) (string, error) {
	mustache.AllowMissingVariables = false
	ctx := params
	if ctx == nil {
		ctx = map[string]string{}
	}
	out, err := mustache.Render(s, ctx)
	if err != nil {
		return "", fmt.Errorf("rendering template %q: %w", s, err)
	}
	return out, nil
}

// renderTemplateValue recursively walks a decoded-JSON value (as produced by
// json.Unmarshal into `any`: maps, slices, strings, and other scalars),
// rendering every string it finds as a Mustache template against params.
func renderTemplateValue(v any, params map[string]string) (any, error) {
	switch val := v.(type) {
	case string:
		return renderTemplateString(val, params)
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, vv := range val {
			rv, err := renderTemplateValue(vv, params)
			if err != nil {
				return nil, err
			}
			out[k] = rv
		}
		return out, nil
	case []any:
		out := make([]any, len(val))
		for i, vv := range val {
			rv, err := renderTemplateValue(vv, params)
			if err != nil {
				return nil, err
			}
			out[i] = rv
		}
		return out, nil
	default:
		return val, nil
	}
}

// expandTaskTemplate renders taskTemplate once per entry in templateParams,
// producing the "tasks" array to submit.
func expandTaskTemplate(taskTemplate any, templateParams []map[string]string) ([]any, error) {
	tasks := make([]any, 0, len(templateParams))
	for i, params := range templateParams {
		task, err := renderTemplateValue(taskTemplate, params)
		if err != nil {
			return nil, fmt.Errorf("expanding task_template for params row %d: %w", i, err)
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// applyTaskTemplate looks for a "task_template" property in doc and, if
// present, expands it into doc["tasks"] using templateParams (one task per
// row), removing "task_template" from doc. A job JSON may specify either
// "task_template" or "tasks", but not both.
func applyTaskTemplate(doc map[string]any, templateParams []map[string]string, paramsFileGiven bool) error {
	taskTemplate, hasTemplate := doc["task_template"]
	_, hasTasks := doc["tasks"]

	if hasTemplate && hasTasks {
		return fmt.Errorf(`job JSON cannot set both "task_template" and "tasks"`)
	}
	if paramsFileGiven && !hasTemplate {
		return fmt.Errorf("--params was given but the job JSON has no \"task_template\" to apply it to")
	}
	if !hasTemplate {
		return nil
	}

	tasks, err := expandTaskTemplate(taskTemplate, templateParams)
	if err != nil {
		return err
	}
	doc["tasks"] = tasks
	delete(doc, "task_template")
	return nil
}

func runSubmit(c *cli.Context) error {
	url := c.String("url")
	if url == "" {
		return fmt.Errorf("--url is required")
	}
	apiKey := os.Getenv("SPARKLES_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("SPARKLES_API_KEY environment variable is required")
	}
	jobFile := c.Args().Get(0)
	if jobFile == "" {
		return fmt.Errorf("job json file path is required")
	}

	// Defaults to a single row of no parameters, so a task_template with no
	// placeholders still expands into exactly one task.
	templateParams := []map[string]string{nil}
	paramsFile := c.String("params")
	if paramsFile != "" {
		rows, err := loadTemplateParameters(paramsFile)
		if err != nil {
			return fmt.Errorf("reading %s: %w", paramsFile, err)
		}
		templateParams = rows
	}

	body, err := os.ReadFile(jobFile)
	if err != nil {
		return fmt.Errorf("reading %s: %w", jobFile, err)
	}

	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("parsing %s: %w", jobFile, err)
	}

	if err := applyTaskTemplate(doc, templateParams, paramsFile != ""); err != nil {
		return err
	}

	setJobNameIfMissing(doc, jobFile)

	body, err = json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshalling job: %w", err)
	}

	endpoint := strings.TrimRight(url, "/") + "/api/v1/job"
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building request to %s: %w", endpoint, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("posting job to %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response from %s: %w", endpoint, err)
	}

	if resp.StatusCode == http.StatusOK {
		var result submitJobResponse
		if err := json.Unmarshal(respBody, &result); err != nil {
			return fmt.Errorf("job submitted, but could not parse response: %w (body: %s)", err, respBody)
		}
		fmt.Printf("Job submitted successfully: id=%s\n", result.ID)
		return nil
	}

	var apiErr submitAPIError
	if err := json.Unmarshal(respBody, &apiErr); err == nil && (apiErr.Code != "" || apiErr.Error != "") {
		fmt.Printf("Job submission failed: [%s] %s\n", apiErr.Code, apiErr.Error)
	} else {
		fmt.Printf("Job submission failed: HTTP %d: %s\n", resp.StatusCode, respBody)
	}
	return fmt.Errorf("job submission failed with status %d", resp.StatusCode)
}
