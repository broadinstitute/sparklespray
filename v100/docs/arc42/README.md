# Sparkles (v100) — Architecture Documentation

This is [arc42](https://arc42.org)-based architecture documentation for **v100**,
the Go rewrite of the `sparkles` CLI/control-plane for running ad-hoc batch jobs
on Google Compute Engine via GCP Batch.

It documents the code under [`v100/`](..), not the legacy Python implementation
at the repository root (which remains the production implementation at the time
of writing; v100 is the actively developed successor).

## Sections

1. [Introduction and Goals](01-introduction-goals.md)
2. [Constraints](02-constraints.md)
3. [Context and Scope](03-context-scope.md)
4. [Solution Strategy](04-solution-strategy.md)
5. [Building Block View](05-building-block-view.md)
6. [Runtime View](06-runtime-view.md)
7. [Deployment View](07-deployment-view.md)
8. [Cross-cutting Concepts](08-crosscutting-concepts.md)
9. [Architectural Decisions](09-architectural-decisions.md)
10. [Quality Requirements](10-quality-requirements.md)
11. [Risks and Technical Debt](11-risks-technical-debt.md)
12. [Glossary](12-glossary.md)

## Sources

This documentation was derived from the code and design docs in `v100/` as of
2026-08-31 (commit `6fd277d`), specifically: `datamodel.md`, `cluster-health.md`,
`autoscaler.md`, `autoscaler_testing.md`, `func-test-plan.md`, `new-command-plan.md`,
`go.mod`, `cli_main.go`, `cmd/sparkles/main.go`, `worker.go`, `task_queue.go`,
`events.go`, `submit_cmd.go`, `kill.go`, `resources.go`, `transfer_client.go`,
`scheduler/`, `dev/`, and `monitor/`. Where the design docs and the implementation
disagree (e.g. `autoscale-imp.md` referring to a since-renamed `autoscaler` package),
the implementation is treated as authoritative and the discrepancy is called out.
