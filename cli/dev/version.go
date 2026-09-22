package dev

// Version is set at build time via
// -ldflags "-X github.com/broadinstitute/sprinkles/dev.Version=x.y.z"
// (see build.sh). It's the single source of truth for the binary's version:
// both the "sprinkles version" CLI command and the dashboard's
// GET /api/v1/version endpoint read it directly.
var Version = "dev"
