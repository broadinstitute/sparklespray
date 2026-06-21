package dev

import "github.com/urfave/cli"

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
