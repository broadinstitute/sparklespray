package main

import (
	"log"

	"github.com/broadinstitute/sparklespray/monitor"
)

func main() {
	err := monitor.Main()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}
