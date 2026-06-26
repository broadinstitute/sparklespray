package main

import (
	"log"
	"os"

	v100 "github.com/broadinstitute/sparklespray/v100"
	"github.com/broadinstitute/sparklespray/v100/dev"
)

// Version is set at build time via -ldflags "-X main.Version=x.y.z".
var Version = "dev"

func main() {
	app := v100.NewApp()
	app.Version = Version
	app.Commands = append(app.Commands, dev.Command())
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
