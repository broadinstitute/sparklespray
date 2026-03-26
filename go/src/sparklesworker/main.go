package sparklesworker

import (
	"os"
	"time"

	"github.com/broadinstitute/sparklesworker/cmd/autoscaler"
	"github.com/broadinstitute/sparklesworker/cmd/consume"
	"github.com/broadinstitute/sparklesworker/cmd/copyexe"
	"github.com/broadinstitute/sparklesworker/cmd/dev"
	"github.com/broadinstitute/sparklesworker/cmd/exec_task"
	"github.com/broadinstitute/sparklesworker/cmd/fetch"
	"github.com/broadinstitute/sparklesworker/cmd/submit"
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
		submit.SubmitCmd,
		copyexe.CopyexeCmd,
		exec_task.ExecCmd,
		fetch.FetchCmd,
		autoscaler.AutoscalerCmd,
		dev.DevCmd,
	}
	return app.Run(os.Args)
}
