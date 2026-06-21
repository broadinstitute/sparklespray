package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

// RemoteBatchAPIClient implements BatchAPIClient by proxying calls to a batch API emulator over HTTP.
type RemoteBatchAPIClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewRemoteBatchAPIClient(baseURL string) *RemoteBatchAPIClient {
	return &RemoteBatchAPIClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{},
	}
}

func (c *RemoteBatchAPIClient) do(ctx context.Context, method, path string, body, result any) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

type remoteLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type remoteCreateJobBody struct {
	Region       string        `json:"region"`
	MachineType  string        `json:"machineType"`
	VMCount      int           `json:"vmCount"`
	Preemptible  bool          `json:"preemptible"`
	DockerImage  string        `json:"dockerImage"`
	Command      string        `json:"command"`
	EmptyVolumes []string      `json:"emptyVolumes"`
	Labels       []remoteLabel `json:"labels"`
}

func (c *RemoteBatchAPIClient) CreateJob(ctx context.Context, spec *WorkerJobSpec) (string, error) {
	body := remoteCreateJobBody{
		Region:       spec.Region,
		MachineType:  spec.MachineType,
		VMCount:      spec.VMCount,
		Preemptible:  spec.Preemptible,
		DockerImage:  spec.DockerImage,
		Command:      spec.Command,
		EmptyVolumes: spec.EmptyVolumes,
		Labels: []remoteLabel{
			{Name: labelBatch, Value: spec.BatchID},
			{Name: labelWorkpool, Value: spec.WorkpoolID},
		},
	}
	var resp struct {
		JobID string `json:"jobID"`
	}
	if err := c.do(ctx, http.MethodPost, "/jobs", body, &resp); err != nil {
		return "", fmt.Errorf("remote CreateJob: %w", err)
	}
	return resp.JobID, nil
}

func (c *RemoteBatchAPIClient) GetJobStatus(ctx context.Context, jobID string) (BatchJobStatus, error) {
	var resp struct {
		Status BatchJobStatus `json:"status"`
	}
	path := "/jobs/" + url.PathEscape(jobID) + "/status"
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return BatchJobStatusFailed, fmt.Errorf("remote GetJobStatus: %w", err)
	}
	return resp.Status, nil
}

// ListRunningVMs fans out one GET /vms/{zone} request per zone in parallel, then merges
// the results, matching the per-zone pagination required by the real GCP Compute API.
func (c *RemoteBatchAPIClient) ListRunningVMs(ctx context.Context, filterLabelName, filterLabelValue string, zones []string) (map[string]VMInfo, error) {
	type zoneResult struct {
		vms map[string]VMInfo
		err error
	}
	results := make([]zoneResult, len(zones))

	var wg sync.WaitGroup
	for i, zone := range zones {
		i, zone := i, zone
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("/vms/%s?filterLabelName=%s&filterLabelValue=%s",
				url.PathEscape(zone),
				url.QueryEscape(filterLabelName),
				url.QueryEscape(filterLabelValue))
			var resp struct {
				VMs map[string]VMInfo `json:"vms"`
			}
			err := c.do(ctx, http.MethodGet, path, nil, &resp)
			results[i] = zoneResult{vms: resp.VMs, err: err}
		}()
	}
	wg.Wait()

	merged := make(map[string]VMInfo)
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for k, v := range r.vms {
			merged[k] = v
		}
	}
	return merged, nil
}

func (c *RemoteBatchAPIClient) TerminateVM(ctx context.Context, zone, instanceName string) error {
	path := fmt.Sprintf("/vms/%s/%s", url.PathEscape(zone), url.PathEscape(instanceName))
	if err := c.do(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("remote TerminateVM: %w", err)
	}
	return nil
}

func (c *RemoteBatchAPIClient) TerminateJob(ctx context.Context, jobID string) error {
	path := "/jobs/" + url.PathEscape(jobID) + "/cancel"
	if err := c.do(ctx, http.MethodPost, path, nil, nil); err != nil {
		return fmt.Errorf("remote TerminateJob: %w", err)
	}
	return nil
}
