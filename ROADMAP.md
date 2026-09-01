# streamt roadmap

This roadmap is the project-level source of truth for sequencing work. Detailed
requirements live in `docs/specs/`, and implementation checklists live in
`docs/plans/`.

## Product direction

streamt is a streaming change-safety and contract compiler for existing Kafka
and Flink estates. It lets teams import a subset of their estate, define SQL,
contracts, ownership, tests, and policy, review the complete impact of a change,
and hand deterministic artifacts to an appropriate deployment backend.

The project does not aim to replace Terraform, Kubernetes operators, managed
cloud control planes, observability systems, or data catalogs. It should
integrate with them.

## Release gates

No release may be called production-ready until all of the following hold:

- Unknown configuration fields fail validation instead of being ignored.
- A partial project can never delete resources it does not explicitly own.
- A saved plan identifies exactly what `apply` will change.
- Stateful Flink changes are either savepoint-safe or explicitly blocked.
- Every advertised CLI command has an end-to-end smoke test.
- Unit, scenario, packaging, documentation, lint, and the enforced type-check
  baseline pass in CI.
- Installation instructions point to a distribution that actually exists.

## Phase 0: restore trust

Target: the CLI is truthful and safe enough to evaluate against a shared
development cluster.

- [x] Disable automatic deletion of resources merely absent from a manifest.
- [ ] Introduce explicit `external`, `managed`, and `adopted` ownership modes.
- [ ] Persist the last applied state and delete only previously managed
      resources through an explicit destructive workflow.
- [x] Fix `apply --target` and `apply --select` so selection happens using
      artifact ownership metadata.
- [x] Default destructive operations to disabled.
- [x] Reject unknown YAML keys at every configuration level.
- [x] Add a versioned configuration envelope and migration policy.
- [x] Make all documentation examples parse through the same strict parser as
      real projects.
- [x] Fix or temporarily remove non-working CLI paths and unsupported claims.
- [x] Add CLI smoke tests, packaging checks, scenario tests, and a type-check
      baseline to CI.
- [ ] Publish an installable alpha release.

Exit criterion: on a cluster with 40 unrelated topics, a project managing two
topics can plan and apply without proposing or performing changes to the other
38.

## Phase 1: the change-impact workflow

Target: streamt provides immediate value in pull requests before it is allowed
to deploy anything.

- [ ] Add `streamt import` and `streamt adopt` for incremental adoption.
- [ ] Resolve Schema Registry subjects, versions, references, and compatibility
      rules during validation.
- [ ] Compare the desired project with both the last applied manifest and live
      infrastructure.
- [ ] Classify changes as safe, risky, destructive, schema-breaking, or
      state-migration-requiring.
- [ ] Include downstream models, exposures, owners, and live consumer groups in
      impact analysis.
- [ ] Emit a deterministic plan file with a checksum and require `apply` to use
      the reviewed plan.
- [ ] Ship a first-party GitHub Action with PR summaries and machine-readable
      annotations.

Exit criterion: a breaking schema or stateful SQL change produces a reviewable
report naming every known downstream consumer and blocks apply by policy.

## Phase 2: standards and deployment backends

Target: streamt becomes a portable authoring layer instead of a closed control
plane.

- [ ] Generate validated AsyncAPI 3.x documents.
- [ ] Generate Open Data Contract Standard documents.
- [ ] Emit OpenLineage-compatible compile and runtime events.
- [ ] Publish catalog metadata to Conduktor Console and support portable exports
      for Backstage/DataHub-style catalogs.
- [ ] Add a Terraform/OpenTofu backend for Confluent Cloud resources.
- [ ] Add GitOps output for Strimzi resources.
- [ ] Add a Flink Kubernetes Operator backend where its lifecycle semantics fit.
- [ ] Add a real Confluent Cloud Flink Statements backend.

Exit criterion: the same reviewed streamt plan can produce portable metadata
and deploy through at least one stateful external backend without changing the
project model.

## Phase 3: production operations

Target: close the loop between declared intent and runtime evidence.

- [ ] Plan savepoint, last-state, and stateless Flink upgrades explicitly.
- [ ] Integrate Prometheus/OpenTelemetry signals and expose policy evaluation
      over those signals.
- [ ] Support generic Alertmanager/webhook failure actions.
- [ ] Add remote state, locking, state migration, and recovery documentation for
      installations that retain direct apply.
- [ ] Add curated, validated connector profiles after the connector contract is
      stable.

## Deferred until the gates above are met

- Additional execution runtimes such as KStreams, RisingWave, and Materialize.
- Additional transports such as Pulsar and Kinesis.
- A VS Code extension, hosted SaaS, and high-level intent generation.
- New ML syntax that is not backed by an executable target integration.
