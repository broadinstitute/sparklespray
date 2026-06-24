package dev

import (
	"time"

	"github.com/urfave/cli"
)

const defaultDB = "sparkles"

// Command returns the top-level "dev" cli.Command with all dev subcommands.
func Command() cli.Command {
	return cli.Command{
		Name: "dev",
		Subcommands: []cli.Command{
			{
				Name:      "submit",
				ArgsUsage: "<job-spec-json> <workpool-spec-json>",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
				},
				Action: runDevSubmit,
			},
			{
				Name:  "dumpdb",
				Usage: "Print all tasks and workpools from Firestore",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
				},
				Action: runDevDumpDB,
			},
			{
				Name:      "add-worker",
				ArgsUsage: "<workpool-spec-json>",
				Usage:     "Create a GCP Batch job for a workpool and record it in Firestore",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
					cli.IntFlag{Name: "vm-count", Value: 1, Usage: "number of VMs to provision"},
					cli.BoolFlag{Name: "preemptible", Usage: "use SPOT/preemptible VMs"},
				},
				Action: runDevAddWorker,
			},
			{
				Name:  "simulate",
				Usage: "Simulate job submission and task execution, writing to Firestore and PubSub",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
					cli.IntFlag{Name: "tasks-per-job", Value: 10, Usage: "number of tasks per submitted job"},
					cli.DurationFlag{Name: "mean-job-interval", Value: 30 * time.Second, Usage: "mean time between job submissions"},
					cli.DurationFlag{Name: "mean-localization", Value: 5 * time.Second, Usage: "mean time in claimed state (staging)"},
					cli.DurationFlag{Name: "mean-execution", Value: 60 * time.Second, Usage: "mean time in running state"},
					cli.DurationFlag{Name: "mean-upload", Value: 5 * time.Second, Usage: "mean time in writing state (result upload)"},
					cli.DurationFlag{Name: "mean-preempt", Value: 5 * time.Minute, Usage: "mean worker lifetime before preemption"},
					cli.IntFlag{Name: "worker-count", Value: 3, Usage: "number of simulated workers"},
					cli.IntFlag{Name: "timing-variance-pct", Value: 20, Usage: "timing variance as percentage of mean (±%)"},
				},
				Action: runDevSimulate,
			},
			{
				Name:  "dashboard-backend",
				Usage: "Start an HTTP dashboard backend implementing the API described in dashboard-api.md",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
					cli.StringFlag{Name: "addr", Value: ":8080", Usage: "address to listen on"},
					cli.StringFlag{Name: "subscriber-sa", Value: "", Usage: "service account email used to generate Pub/Sub tokens for subscription endpoint"},
				},
				Action: runDevDashboardBackend,
			},
			{
				Name:  "batchapi-emulator",
				Usage: "Run a local batch API emulator for testing",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "addr", Value: ":8742", Usage: "address to listen on"},
					cli.DurationFlag{Name: "queueTime", Value: 0, Usage: "how long jobs sit in QUEUED state before containers are started"},
					cli.BoolFlag{Name: "no-docker", Usage: "run commands directly in batch-api-procs/<instance> instead of Docker"},
				},
				Action: runBatchAPIEmulator,
			},
		},
	}
}
