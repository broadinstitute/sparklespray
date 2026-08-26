package v100

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

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

func runSubmit(c *cli.Context) error {
	url := c.String("url")
	if url == "" {
		return fmt.Errorf("--url is required")
	}
	jobFile := c.Args().Get(0)
	if jobFile == "" {
		return fmt.Errorf("job json file path is required")
	}

	// Read the file as-is; the server is responsible for parsing/validating it.
	body, err := os.ReadFile(jobFile)
	if err != nil {
		return fmt.Errorf("reading %s: %w", jobFile, err)
	}

	endpoint := strings.TrimRight(url, "/") + "/api/v1/job"
	resp, err := http.Post(endpoint, "application/json", bytes.NewReader(body))
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
