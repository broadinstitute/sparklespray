package main

import (
	"log"

	"github.com/broadinstitute/sparklesworker"
)

func main() {
	if err := sparklesworker.Main(); err != nil {
		log.Fatalf("Error: %s", err)
	}
}
