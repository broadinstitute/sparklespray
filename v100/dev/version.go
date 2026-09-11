package dev

// Version is set at build time via
// -ldflags "-X github.com/broadinstitute/sparklespray/v100/dev.Version=x.y.z"
// (see build.sh). It's the single source of truth for the binary's version:
// both the "sparkles version" CLI command and the dashboard's
// GET /api/v1/version endpoint read it directly.
var Version = "dev"
