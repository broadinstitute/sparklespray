package monitor

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli"
)

const MonitorVersion = "0.1.0"

func Main() error {
	app := cli.NewApp()
	app.Name = "sparkles-monitor"
	app.Version = MonitorVersion
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{
			Name:  "Philip Montgomery",
			Email: "pmontgom@broadinstitute.org",
		},
	}
	app.Usage = "Monitor and manage Sparklespray clusters"

	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "project", Usage: "Google Cloud project ID"},
		cli.StringFlag{Name: "cluster", Usage: "Cluster ID to monitor"},
	}

	app.Action = run

	return app.Run(os.Args)
}

func run(c *cli.Context) error {
	project := c.String("project")
	cluster := c.String("cluster")

	fmt.Printf("sparkles-monitor\n")
	fmt.Printf("  project: %s\n", project)
	fmt.Printf("  cluster: %s\n", cluster)

	return nil
}
