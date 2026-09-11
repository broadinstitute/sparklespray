package main

import (
	"fmt"
	"log"
	"os"

	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/dev"
	"github.com/urfave/cli"
)

func main() {
	app := v100.NewApp()
	app.Version = dev.Version
	app.Commands = append(app.Commands, dev.ServeCommand(), dev.Command(), cli.Command{
		Name:  "version",
		Usage: "Print the sparkles binary version",
		Action: func(c *cli.Context) error {
			fmt.Println(dev.Version)
			return nil
		},
	})
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
