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
				Name:      "set-config",
				ArgsUsage: "<config-json>",
				Usage:     "Load a SparklesConfig JSON file into Firestore (SparklesConfig/default), used by dashboard-backend",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
				},
				Action: runSetConfig,
			},
			{
				Name:      "add-api-key",
				ArgsUsage: "<user>",
				Usage:     "Generate an API key for a user and store it in Firestore (APIKeys collection)",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
				},
				Action: runAddAPIKey,
			},
			{
				Name:      "export",
				ArgsUsage: "<collection>",
				Usage:     "Dump Firestore collection contents as JSON (one doc per line)",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
					cli.StringSliceFlag{Name: "filter", Usage: "field=value filter (repeatable)"},
				},
				Action: runDevExport,
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
					cli.IntFlag{Name: "max-jobs", Value: 5, Usage: "stop submitting after this many jobs (0 = unlimited)"},
					cli.DurationFlag{Name: "mean-job-interval", Value: 240 * time.Second, Usage: "mean time between job submissions"},
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
				Usage: "Start only the HTTP dashboard backend (see \"sparkles serve\" to run it together with the monitor)",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
					cli.StringFlag{Name: "addr", Value: ":8080", Usage: "address to listen on"},
				},
				Action: runDevDashboardBackend,
			},
			{
				Name:  "monitor",
				Usage: "Start only the monitor (see \"sparkles serve\" to run it together with the dashboard backend)",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project", Usage: "GCP project ID (required)"},
					cli.StringFlag{Name: "db", Value: defaultDB, Usage: "Firestore database"},
					cli.BoolFlag{Name: "verbose, v", Usage: "log a message at the start of every poll"},
					cli.IntFlag{Name: "linger", Value: 0, Usage: "exit after this many minutes with no active tasks (0 = run forever)"},
				},
				Action: runDevMonitor,
			},
			{
				Name:  "create-topics",
				Usage: "Create all Pub/Sub topics and subscriptions required by sparklespray",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
				},
				Action: runDevCreateTopics,
			},
			{
				Name:  "clean-expired",
				Usage: "Delete all documents whose expiry timestamp has passed",
				Flags: []cli.Flag{
					cli.StringFlag{Name: "project"},
					cli.StringFlag{Name: "db", Value: defaultDB},
					cli.BoolFlag{Name: "expire-all", Usage: "treat every document as expired by using (now + 10 years) as the reference time"},
				},
				Action: runDevCleanExpired,
			},
			{
				Name:      "test-profile-command",
				ArgsUsage: "<docker-image> <command...>",
				Usage:     "Run a command in a docker image with the worker's periodic metric collection, printing each sample as JSON to stdout (use -- before the image if <command> has its own flags)",
				Flags: []cli.Flag{
					cli.DurationFlag{Name: "interval", Usage: "metric sampling interval (default: same as production, 1m)"},
				},
				Action: runDevTestProfileCommand,
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
