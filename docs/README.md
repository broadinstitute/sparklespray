# Sparklespray Documentation

All current documentation for this repository lives here. `v100/` is the
only active implementation (a Go rewrite of an earlier Python CLI, which has
been deleted from the repo along with its docs).

- **[arc42/](arc42/)** — arc42-style architecture documentation: goals,
  constraints, context, solution strategy, building blocks, runtime and
  deployment views, cross-cutting concepts, architectural decisions, quality
  requirements, risks/technical debt, and a glossary. Start with
  [arc42/README.md](arc42/README.md).
- **[design/](design/)** — narrower, code-level design docs: the
  autoscaler/monitor's provisioning and watchdog logic, cluster-health state
  machines, the Firestore/Pub-Sub data model, the dashboard REST API, the
  Batch API emulator, and testing plans. These are closer to the
  implementation than the arc42 set and are the better starting point when
  working on a specific package.
- **[deploying-dashboard-backend.md](deploying-dashboard-backend.md)** — how
  to build and deploy the dashboard-backend (with the embedded dashboard UI)
  to a remote server.
- **[openapi.yaml](openapi.yaml)** — OpenAPI spec fragment for the dashboard
  REST API (partial; see `design/dashboard-api.md` for the fuller, current
  endpoint-by-endpoint reference).
