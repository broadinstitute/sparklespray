package main

import (
	"log"
	"os"

	v100 "github.com/broadinstitute/sparklespray/v100"
)

func main() {
	if err := v100.Main(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
