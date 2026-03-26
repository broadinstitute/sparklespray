package sparklesworker

import (
	"os"
	"time"

	"github.com/broadinstitute/sparklesworker/cmd/autoscaler"
	"github.com/broadinstitute/sparklesworker/cmd/consume"
	"github.com/broadinstitute/sparklesworker/cmd/exec_task"
	"github.com/urfave/cli"
)

const WorkerVersion = "5.8.2"

func Main() error {
	consume.WorkerVersion = WorkerVersion

	app := cli.NewApp()
	app.Name = "sparklesworker"
	app.Version = WorkerVersion
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{Name: "Philip Montgomery", Email: "pmontgom@broadinstitute.org"},
	}
	app.Commands = []cli.Command{
		consume.ConsumeCmd,
		exec_task.ExecCmd,
		autoscaler.AutoscalerCmd,
	}
	return app.Run(os.Args)
}
