package v100

import (
	"os"
	"time"

	"github.com/urfave/cli"
)

const defaultDB = "sparkles"

// NewApp creates and configures the sparkles CLI app with the end-user
// commands (worker, submit, kill). The "serve" and "dev" commands are
// registered by cmd/sparkles/main.go, since they live in v100/dev and this
// package can't import it without a cycle.
func NewApp() *cli.App {
	app := cli.NewApp()
	app.Name = "sparkles"
	app.Version = "dev"
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
				cli.StringFlag{Name: "db", Value: defaultDB},
				cli.StringFlag{Name: "workpool"},
				cli.StringFlag{Name: "resources"},
				cli.StringFlag{Name: "batch", Usage: "batch ID for this worker"},
				cli.IntFlag{Name: "linger", Usage: "seconds to keep polling after the queue is empty (leader worker only)"},
				cli.BoolFlag{Name: "stream", Usage: "stream task output to Firestore immediately when a task starts"},
				cli.BoolFlag{Name: "no-gcp", Usage: "local development mode: skip GCP metadata server"},
				cli.BoolFlag{Name: "no-docker", Usage: "run task commands directly without Docker (ignores image name)"},
				cli.StringSliceFlag{Name: "bind-mount", Usage: "additional Docker bind mounts (host:container), may be repeated"},
				cli.StringFlag{Name: "work-dir", Usage: "parent directory for task working directories (default: OS temp dir)"},
			},
			Action: runWorker,
		},
		{
			Name:      "submit",
			ArgsUsage: "<job-json>",
			Usage:     "Submit a job to a running dashboard-backend (POST /api/v1/job). Requires the SPARKLES_API_KEY environment variable.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "url", Usage: "base URL of the dashboard-backend (required)"},
				cli.StringFlag{Name: "params", Usage: "CSV file of template parameters, one row per task, used to expand a \"task_template\" in the job JSON into \"tasks\""},
			},
			Action: runSubmit,
		},
		{
			Name:      "kill",
			ArgsUsage: "<job-id>",
			Usage:     "Kill all pending and running tasks for a job",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "project"},
				cli.StringFlag{Name: "db", Value: defaultDB},
				cli.BoolFlag{Name: "no-wait", Usage: "exit immediately after sending kill signal without polling for completion"},
			},
			Action: runKill,
		},
	}

	return app
}

// Main runs the complete sparkles CLI. Provided for backward compatibility;
// callers that need dev commands should use NewApp() and append them instead.
func Main() error {
	return NewApp().Run(os.Args)
}
