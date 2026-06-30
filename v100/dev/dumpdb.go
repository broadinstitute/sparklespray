package dev

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/firestore"
	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/urfave/cli"
)

type dumpJobInfo struct {
	JobID  string `json:"jobID"`
	Status string `json:"status"`
	Labels []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"labels"`
	VMs []struct {
		InstanceName string `json:"instanceName"`
		Zone         string `json:"zone"`
		Done         bool   `json:"done"`
		ExitCode     int    `json:"exitCode"`
	} `json:"vms"`
}

type dumpVMInfo struct {
	InstanceName string `json:"instanceName"`
	Zone         string `json:"zone"`
}

func runDevDumpDB(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")

	ctx := context.Background()
	log.Printf("Connecting to project %s, db %s", project, db)
	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	fmt.Println("=== Tasks ===")
	taskDocs, err := fsClient.Collection(v100.TaskCollection).Documents(ctx).GetAll()
	if err != nil {
		return fmt.Errorf("listing tasks: %w", err)
	}
	for _, doc := range taskDocs {
		var t v100.Task
		if err := doc.DataTo(&t); err != nil {
			fmt.Printf("  %s  (error reading fields: %v)\n", doc.Ref.ID, err)
			continue
		}
		fmt.Printf("  %s  job=%s  workpool=%s  status=%s\n", t.TaskID, t.JobID, t.WorkpoolID, t.Status)
	}
	fmt.Printf("  (%d total)\n\n", len(taskDocs))

	fmt.Println("=== WorkPools ===")
	poolDocs, err := fsClient.Collection(v100.WorkpoolCollection).Documents(ctx).GetAll()
	if err != nil {
		return fmt.Errorf("listing workpools: %w", err)
	}
	for _, doc := range poolDocs {
		var p v100.WorkPool
		if err := doc.DataTo(&p); err != nil {
			fmt.Printf("  %s  (error reading fields: %v)\n", doc.Ref.ID, err)
			continue
		}
		fmt.Printf("  %s  machine=%s  region=%s  state=%s\n", p.WorkpoolID, p.MachineType, p.Region, p.State)
	}
	fmt.Printf("  (%d total)\n", len(poolDocs))

	if emulatorURL := os.Getenv("SPARKLES_BATCH_API_EMULATOR"); emulatorURL != "" {
		fmt.Println()
		if err := dumpBatchAPI(ctx, emulatorURL); err != nil {
			fmt.Printf("batch API dump error: %v\n", err)
		}
	}

	return nil
}

func batchAPIGet(ctx context.Context, url string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func dumpBatchAPI(ctx context.Context, baseURL string) error {
	fmt.Println("=== Batch API Jobs ===")
	var jobsResp struct {
		Jobs []dumpJobInfo `json:"jobs"`
	}
	if err := batchAPIGet(ctx, baseURL+"/jobs", &jobsResp); err != nil {
		return fmt.Errorf("GET /jobs: %w", err)
	}
	for _, job := range jobsResp.Jobs {
		labels := ""
		for _, l := range job.Labels {
			labels += fmt.Sprintf(" %s=%s", l.Name, l.Value)
		}
		fmt.Printf("  %s  status=%s  vms=%d%s\n", job.JobID, job.Status, len(job.VMs), labels)
		for _, vm := range job.VMs {
			doneStr := ""
			if vm.Done {
				doneStr = fmt.Sprintf(" done(exit=%d)", vm.ExitCode)
			}
			fmt.Printf("    vm=%s  zone=%s%s\n", vm.InstanceName, vm.Zone, doneStr)
		}
	}
	fmt.Printf("  (%d total)\n\n", len(jobsResp.Jobs))

	fmt.Println("=== Batch API VMs (by zone) ===")
	var zonesResp struct {
		Zones []string `json:"zones"`
	}
	if err := batchAPIGet(ctx, baseURL+"/region/emulator/zones", &zonesResp); err != nil {
		return fmt.Errorf("GET /region/emulator/zones: %w", err)
	}
	totalVMs := 0
	for _, zone := range zonesResp.Zones {
		var vmsResp struct {
			VMs map[string]dumpVMInfo `json:"vms"`
		}
		if err := batchAPIGet(ctx, baseURL+"/vms/"+zone, &vmsResp); err != nil {
			fmt.Printf("  zone=%s  error: %v\n", zone, err)
			continue
		}
		fmt.Printf("  zone=%s  vms=%d\n", zone, len(vmsResp.VMs))
		for _, vm := range vmsResp.VMs {
			fmt.Printf("    %s\n", vm.InstanceName)
		}
		totalVMs += len(vmsResp.VMs)
	}
	fmt.Printf("  (%d total across %d zones)\n", totalVMs, len(zonesResp.Zones))

	return nil
}
