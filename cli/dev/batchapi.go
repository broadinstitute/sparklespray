package dev

import (
	"github.com/broadinstitute/sprinkles/monitor/emulator"
	"github.com/urfave/cli"
)

func runBatchAPIEmulator(c *cli.Context) error {
	return emulator.Run(c.String("addr"), c.Duration("queueTime"), c.Bool("no-docker"))
}
