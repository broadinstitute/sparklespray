package main

import (
	"fmt"
	"log"
	"os"

	"github.com/broadinstitute/sprinkles"
	"github.com/broadinstitute/sprinkles/dev"
	"github.com/urfave/cli"
)

func main() {
	app := sprinkles.NewApp()
	app.Version = dev.Version
	app.Commands = append(app.Commands, dev.ServeCommand(), dev.Command(), cli.Command{
		Name:  "version",
		Usage: "Print the sprinkles binary version",
		Action: func(c *cli.Context) error {
			fmt.Println(dev.Version)
			return nil
		},
	})
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
