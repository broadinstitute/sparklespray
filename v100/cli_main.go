package v100

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/urfave/cli"
)

func Main() error {
	app := cli.NewApp()
	app.Name = "sparkles"
	app.Version = "1.0.0"
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{
			Name:  "Philip Montgomery",
			Email: "pmontgom@broadinstitute.org",
		},
	}

	app.Commands = []cli.Command{
		{
			Name: "worker",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "project"},
				cli.StringFlag{Name: "db"},
				cli.StringFlag{Name: "workpool"},
				cli.StringFlag{Name: "resources"},
			},
			Action: runWorker,
		},
		{
			Name: "dev",
			Subcommands: []cli.Command{
				{
					Name:      "submit",
					ArgsUsage: "<job-spec-json> <workpool-spec-json>",
					Flags: []cli.Flag{
						cli.StringFlag{Name: "project"},
					},
					Action: runDevSubmit,
				},
			},
		},
	}

	return app.Run(os.Args)
}

func runDevSubmit(c *cli.Context) error {
	args := c.Args()
	jobSpecFile := args.Get(0)
	if jobSpecFile == "" {
		return fmt.Errorf("job spec json file path is required")
	}
	workpoolSpecFile := args.Get(1)
	if workpoolSpecFile == "" {
		return fmt.Errorf("workpool spec json file path is required")
	}
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	return devSubmit(jobSpecFile, workpoolSpecFile, project)
}

type JobSpec struct {
	Name            string           `json:"name"`
	Resources       []ResourceEntry  `json:"resources"`
	FilesToLocalize []FileToLocalize `json:"filesToLocalize"`
	Tasks           []JobSpecTask    `json:"tasks"`
}

type JobSpecTask struct {
	Command     []string `json:"command"`
	DockerImage string   `json:"dockerImage"`
}

type WorkpoolSpec struct {
	// if specified, will be used as the id, if not, we'll compute a hash of this spec to determine the ID
	ID           string          `json:"id"`
	MachineType  string          `json:"machineType"`
	RootDir      string          `json:"rootDir"`
	Resources    []ResourceEntry `json:"resources"`
	EmptyVolumes []EmptyVolume   `json:"emptyVolumes"`
}

func readJSON[T any](path string) (*T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return &v, nil
}

func resolveWorkpoolID(workpoolSpec *WorkpoolSpec) (string, error) {
	if workpoolSpec.ID != "" {
		return workpoolSpec.ID, nil
	}
	// Canonicalize by re-serializing, then hash.
	canonical, err := json.Marshal(workpoolSpec)
	if err != nil {
		return "", fmt.Errorf("canonicalizing workpool spec: %w", err)
	}
	sum := sha256.Sum256(canonical)
	workpoolID := hex.EncodeToString(sum[:])
	return workpoolID, nil
}

func devSubmit(jobSpecFile, workpoolSpecFile, project string) error {
	jobSpec, err := readJSON[JobSpec](jobSpecFile)
	if err != nil {
		return err
	}
	_ = jobSpec

	workpoolSpec, err := readJSON[WorkpoolSpec](workpoolSpecFile)
	if err != nil {
		return err
	}

	workpoolID, err := resolveWorkpoolID(workpoolSpec)
	workpool := WorkPool{
		WorkpoolID:   workpoolID,
		MachineType:  workpoolSpec.MachineType,
		RootDir:      workpoolSpec.RootDir,
		Resources:    workpoolSpec.Resources,
		EmptyVolumes: workpoolSpec.EmptyVolumes,
		Expiry:       time.Now().Add(7 * 24 * time.Hour),
	}

	ctx := context.Background()
	fsClient, err := firestore.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	_, err = fsClient.Collection(workpoolCollection).Doc(workpoolID).Set(ctx, workpool)
	if err != nil {
		return fmt.Errorf("writing workpool to firestore: %w", err)
	}

	fmt.Printf("workpool %s written\n", workpoolID)

	jobID := uuid.New().String()
	job := Job{
		JobID:      jobID,
		Name:       jobSpec.Name,
		WorkpoolID: workpoolID,
		Resources:  jobSpec.Resources,
	}

	// Pre-generate task IDs outside the transaction so retries are idempotent.
	taskIDs := make([]string, len(jobSpec.Tasks))
	for i := range taskIDs {
		taskIDs[i] = uuid.New().String()
	}

	err = fsClient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := tx.Set(fsClient.Collection(jobCollection).Doc(jobID), job); err != nil {
			return err
		}
		for i, t := range jobSpec.Tasks {
			task := Task{
				JobID:           jobID,
				TaskID:          taskIDs[i],
				TaskIndex:       i,
				WorkpoolID:      workpoolID,
				Status:          StatusPending,
				Command:         t.Command,
				DockerImage:     t.DockerImage,
				FilesToLocalize: jobSpec.FilesToLocalize,
			}
			if err := tx.Set(fsClient.Collection(taskCollection).Doc(taskIDs[i]), task); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("writing job and tasks to firestore: %w", err)
	}

	fmt.Printf("job %s written with %d tasks\n", jobID, len(jobSpec.Tasks))
	return nil
}
