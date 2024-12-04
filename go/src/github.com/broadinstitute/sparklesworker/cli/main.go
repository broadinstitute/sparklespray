package main

import (
	"log"

	"github.com/broadinstitute/sparklesworker"
)

func main() {
	err := sparklesworker.Main()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}
