package main

import (
	"log"
	"os"
	"time"

	"github.com/broadinstitute/sparklesworker/cmd/dev"
	"github.com/broadinstitute/sparklesworker/cmd/submit"
	"github.com/urfave/cli"
)

const version = "5.8.2"

func main() {
	app := cli.NewApp()
	app.Name = "sparkles"
	app.Version = version
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{Name: "Philip Montgomery", Email: "pmontgom@broadinstitute.org"},
	}
	app.Commands = []cli.Command{
		submit.SubmitCmd,
		dev.DevCmd,
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Error: %s", err)
	}
}
