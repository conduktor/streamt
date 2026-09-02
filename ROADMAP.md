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
- Unit, scenario, packaging, documentation, lint, and a clean zero-error mypy
  check pass in CI.
- Installation instructions point to a distribution that actually exists.

## Phase 0: restore trust

Target: the CLI is truthful and safe enough to evaluate against a shared
development cluster.

- [x] Disable automatic deletion of resources merely absent from a manifest.
- [x] Introduce explicit `external`, `managed`, and `adopted` ownership modes.
- [x] Persist environment-scoped local last-applied ownership state for direct
      development applies.
- [ ] Delete only previously managed resources through an explicit destructive
      workflow backed by remote state and locking.
- [x] Fix `apply --target` and `apply --select` so selection happens using
      artifact ownership metadata.
- [x] Default destructive operations to disabled.
- [x] Reject unknown YAML keys at every configuration level.
- [x] Add a versioned configuration envelope and migration policy.
- [x] Make all documentation examples parse through the same strict parser as
      real projects.
- [x] Fix or temporarily remove non-working CLI paths and unsupported claims.
- [x] Add CLI smoke tests, packaging checks, scenario tests, and a zero-error
      mypy gate to CI.
- [ ] Publish an installable alpha release.

Exit criterion: on a cluster with 40 unrelated topics, a project managing two
topics can plan and apply without proposing or performing changes to the other
38.

## Phase 1: the change-impact workflow

Target: streamt provides immediate value in pull requests before it is allowed
to deploy anything.

- [x] Add no-clobber `streamt import` for external Kafka source declarations.
- [x] Add explicit, fail-closed adoption for one existing Kafka topic at a time.
- [x] Extend fail-closed adoption to one Schema Registry subject at a time.
- [ ] Extend adoption to jobs, connectors, and Gateway rules after their exact
      management surfaces and compound identities are specified.
- [x] Resolve Schema Registry subjects, versions, references, and compatibility
      rules during validation.
- [x] Compare the desired project with both the last applied manifest and live
      infrastructure.
- [x] Classify changes as safe, risky, destructive, schema-breaking, or
      state-migration-requiring, while marking unproven schema/downstream and
      operator-state evidence explicitly unknown.
- [x] Include downstream models, exposures, owners, and live consumer groups in
      impact analysis.
- [x] Emit a deterministic plan file with an integrity checksum and reject it
      when project, environment, ownership state, or live actions drift.
- [x] Require protected/shared-environment `apply` workflows to use a reviewed
      plan.
- [x] Ship a first-party GitHub Action with job summaries and machine-readable
      annotations.
- [x] Serialize deterministic blockers for Kafka partition reductions,
      incompatible schemas, and Flink updates, and reject them before apply.

Exit criterion: a breaking schema or stateful SQL change produces a reviewable
report naming every known downstream consumer and blocks apply by policy.

## Phase 2: standards and deployment backends

Target: streamt becomes a portable authoring layer instead of a closed control
plane.

- [x] Generate validated AsyncAPI 3.x documents (3.1 documents validated
      offline against the pinned official schema plus semantic reference
      checks).
- [x] Generate Open Data Contract Standard 3.1.0 project-wide schema documents
      with pinned offline validation and explicit contract identity, status,
      and version metadata.
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
- [x] Extract a typed provider-neutral state boundary while preserving the
      local version 1 JSON format and operation-wide same-host locking.
- [x] Add a strict local operation-control sidecar with intent-before-mutation,
      ordered progress, recovery blocking, and read-only plan status.
- [ ] Add remote state, locking, state migration, and recovery documentation for
      installations that retain direct apply.
- [ ] Add curated, validated connector profiles after the connector contract is
      stable.

## Deferred until the gates above are met

- Additional execution runtimes such as KStreams, RisingWave, and Materialize.
- Additional transports such as Pulsar and Kinesis.
- A VS Code extension, hosted SaaS, and high-level intent generation.
- New ML syntax that is not backed by an executable target integration.
