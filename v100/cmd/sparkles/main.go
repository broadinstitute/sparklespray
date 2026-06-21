package main

import (
	"log"
	"os"

	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/dev"
)

func main() {
	app := v100.NewApp()
	app.Commands = append(app.Commands, dev.Command())
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
