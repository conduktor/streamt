# streamt roadmap

This roadmap is the project-level source of truth for sequencing work. Detailed
requirements live in `docs/specs/`, and implementation checklists live in
`docs/plans/`.

## Product direction

streamt is a framework for developing, testing, deploying, and evolving
streaming applications as versioned projects. SQL is the first authoring path;
custom applications and their dependencies belong in the same project model.
Users can import existing resources as external declarations or add resources
whose lifecycle streamt manages. Review, contracts, lineage, and exports support
that development workflow.

The project does not aim to replace Terraform, Kubernetes operators, managed
cloud control planes, observability systems, or data catalogs. It should
integrate with them.

## Current execution order: 2026-09-05

The owner approved bounded Kafka Streams productization with Docker execution.
The [topology/runtime plan](docs/plans/2026-09-05-topology-runtime-execution.md)
now governs work: import existing or create new topology, validate contracts,
deploy managed processing, and prove coherent changes on stable identities.
The earlier prototype gate below is satisfied as an investment decision, not
as evidence that production lifecycle guarantees have already been implemented.

The [product direction](docs/specs/product-direction.md) and
[developer workflow specification](docs/specs/developer-workflow.md) define the
product scope. The previous M0-M2 authorization is retained in the
[prior execution plan](docs/plans/2026-09-04-developer-experience-execution.md),
not as a second active queue. Custom artifact deployment and stateful upgrades
remain deferred.

1. Preserve external/managed coexistence and enforce custom-consumer contracts.
2. Ship the explicit Kafka Streams compiler and fixed, locally built runner.
3. Prove import or fresh init through validation, managed creation, actual output,
   and no-op repeat using an installed package; run the same journey in CI.
4. Add predicate-only replacement with stable application identity, explicit
   progress evidence, durable checkpoints and interrupted-operation recovery.
5. Complete declared Git base/head comparison and dependency impact reporting.

Implemented: import provenance and declaration-only external planning, complete
custom-application edges, declared/inferred contract compatibility, strict Kafka
Streams compilation, maintained Java build assets, and managed create/no-op
integration. Disposable real-Kafka acceptance passes from source and an installed
wheel, including import, the managed starter, exact output/offset checks, and
negative cases that preserve provider resources and ownership state. The same
installed journey now passes its two-SDK CI gate. All 26 jobs passed for
`991a035`, including real DataHub and Strimzi. Typed replacement evidence,
reviewed format-6 binding and an internal journaled driver are implemented.
Real source and installed tests prove continuation after a lost Docker-create
response, with the same application and offsets. Local and PostgreSQL backends
now support durable same-operation resume authorization with retained incident
history. Public replacement and recovery remain blocked until CLI integration
and the complete user-facing interruption journey pass.

The immutable `0.1.0a1` [release procedure](docs/plans/2026-09-03-first-alpha-release.md)
remains open and unchanged. External publication is not the next implementation
task or a prerequisite for local development. Do not create tags, releases, or
package uploads as part of this cycle without separate release authority.

### Existing integration boundaries

The deterministic managed-topic-only Strimzi 1.2.0 `KafkaTopic` GitOps export
is complete under the
[Strimzi export specification](docs/specs/strimzi-kafkatopic-export.md). Its
support stops at the offline artifact; direct Kubernetes and Strimzi deployment
remain unsupported. No Strimzi expansion is scheduled in this cycle.

PostgreSQL-v2-only explicit Kafka Connect removal is complete under its
[Connector removal specification](docs/specs/connector-explicit-removal.md).
Its narrow full-online-reviewed boundary does not enable topic or schema
deletion, Flink cancellation, local-state deletion, or non-default Connect
cluster routing.

Topic and schema deletion, production catalog publication, and direct Kubernetes
apply remain outside this cycle. Any processor stop/replacement needed for M4
requires its own exact lifecycle contract; generic cancellation is not enabled.

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

## Capability record and backlog

The phases below retain completed work and outstanding technical contracts from
the stabilization program. They are not a second execution queue. An unchecked
item enters the active cycle only through M0-M5 above; safety gates still apply.

### Phase 0: restore trust

Target: the CLI is truthful and safe enough to evaluate against a shared
development cluster.

- [x] Disable automatic deletion of resources merely absent from a manifest.
- [x] Introduce explicit `external`, `managed`, and `adopted` ownership modes.
- [x] Persist environment-scoped local last-applied ownership state for direct
      development applies.
- [ ] Delete only previously managed resources through explicit destructive
      workflows backed by remote state and locking:
  - [x] Gateway rule removal uses an exact lifecycle tombstone, reviewed plan,
        locked state projection, recovery, and real Gateway gate. Its existing
        local-state development mode does not satisfy the broader remote-only
        completion criterion.
  - [x] Kafka Connect removal is PostgreSQL-schema-v2-only and requires an exact
        lifecycle tombstone, a fresh full online reviewed plan, managed state,
        strict live evidence, destructive authorization, and reviewed recovery.
        Source, sdist/wheel build, isolated installed-wheel, PostgreSQL 14/18,
        independent-process, secrecy, and pinned real Connect 7.5.0 gates
        pass. Manifest/model absence remains inert. See the
        [Connector removal implementation plan](docs/plans/2026-09-03-connector-explicit-removal.md).
  - [ ] Kafka topic, Schema Registry subject, and Flink job removal remain
        blocked on their separate data-loss, reference, identity, and stateful
        lifecycle contracts.
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
- [ ] Publish the installable `0.1.0a1` alpha. Package identity and release
      automation are prepared; the tag and uploads wait for exact-SHA CI,
      protected `testpypi`/`pypi` environments, trusted publishers, rehearsal,
      and independent production installation verification.

Exit criterion: on a cluster with 40 unrelated topics, a project managing two
topics can plan and apply without proposing or performing changes to the other
38.

### Phase 1: the change-impact workflow

Target: review makes application changes understandable before deployment.
Read-only evaluation remains possible within the full development workflow.

- [x] Add no-clobber `streamt import` for external Kafka source declarations.
- [x] Add explicit, fail-closed adoption for one existing Kafka topic at a time.
- [x] Extend fail-closed adoption to one Schema Registry subject at a time.
- [x] Extend fail-closed adoption beyond topics and schemas in the scoped order
      defined by the
      [extended resource adoption plan](docs/plans/2026-09-02-extended-resource-adoption.md):
  - [x] Make Connector planning secret-neutral, bind the canonical artifact to
        an exact Connect cluster locator, and add a strict one-request observer.
  - [x] Use that Connector observer for reviewed recovery and enable
        single-Connector state-only adoption.
  - [x] Normalize the complete scoped Gateway alias/interceptor aggregate under
        the frozen
        [Gateway implementation specification](docs/plans/2026-09-02-gateway-normalized-aggregate.md),
        then enable only exact alias-only rules with zero interceptors. The
        strict parser, binding, desired aggregate, immutable two-list snapshot,
        normalized planning/change model, collision gates, state projection,
        secret-neutral reviewed-plan presentation, shared-snapshot status, and
        exact managed mutation routing are complete. Versioned, secret-neutral
        rule-name plus current/desired evidence is now persisted on each durable
        pre-mutation Gateway action, with exact legacy compatibility. Reviewed
        recovery validates all action identities before provider access, derives
        desired and explicitly removed targets from one bounded two-list
        snapshot, and resolves exact create, update, and delete current/desired
        outcomes. Local and PostgreSQL reviewed-command E2E, installed-wheel
        recovery, and the focused real Gateway 3.15 observer gate now pass. The
        explicit lifecycle-removal workflow is implemented end to end in source:
        strict tombstones, provider-free state preflight, one-snapshot planning,
        reviewed-plan v4, destructive authorization, exact deletion, state
        projection, and recovery reuse. Its local, PostgreSQL 14/18, isolated
        installed-wheel, and real Gateway 3.15 exact-deletion gates pass, so the
        normalized Package 6 boundary is complete. Package 7 now adds exact
        alias-only, state-only Gateway adoption with two complete observations,
        zero mutation, exact reviewed recovery, local and PostgreSQL v2 state,
        isolated-wheel execution, and real Gateway 3.15 coverage. Adoption of
        rules with interceptors remains deliberately unsupported under the
        [Gateway specification](docs/plans/2026-09-02-gateway-normalized-aggregate.md).
  - [x] Keep Flink adoption deferred until stable per-job identity, strict
        cluster routing, provider-visible artifact evidence, unambiguous
        discovery, and evidence-gated state advancement exist.
- [x] Resolve Schema Registry subjects, versions, references, and compatibility
      rules during validation.
- [x] Compare the desired project with both the last applied manifest and live
      infrastructure.
- [x] Classify changes as safe, risky, destructive, schema-breaking, or
      state-migration-requiring, while reporting unproven schema/downstream and
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

### Phase 2: standards and deployment backends

Target: streamt becomes a portable authoring layer instead of a closed control
plane.

- [x] Generate validated AsyncAPI 3.x documents (3.1 documents validated
      offline against the pinned official schema plus semantic reference
      checks).
- [x] Generate Open Data Contract Standard 3.1.0 project-wide schema documents
      with pinned offline validation and explicit contract identity, status,
      and version metadata.
- [x] Export offline-validated OpenLineage 1.53.0 static `DatasetEvent` and
      `JobEvent` design metadata as deterministic JSONL.
- [x] Emit opt-in OpenLineage `RunEvent` telemetry for finite `streamt test`
      invocations without presenting it as deployed Flink, Gateway, or Connect
      runtime telemetry.
- [x] Emit opt-in OpenLineage `RunEvent` telemetry across the durable
      `streamt apply` operation boundary. START reuses the durable operation
      UUID and timestamp; terminal truth follows commit/recovery state, including
      COMPLETE for verified post-commit `E426` release failures. Local,
      isolated-wheel, source-distribution, and real PostgreSQL 14/18 executable
      gates pass in the full Python 3.10–3.14 unit release workflow; the
      provider-specific package gates retain their separately pinned lanes.
- [x] Export deterministic Backstage v1.54.2 core catalog entities from one
      offline compile, with pinned validation, canonical YAML, installed-wheel
      coverage, and independent `@backstage/catalog-model@1.10.0` parity.
- [x] Export deterministic DataHub v1.7.0 simplified Metadata Change
      Proposals from the neutral catalog model, with dependency-free
      validation, canonical JSON, isolated-wheel coverage, and an independent
      SDK/metadata-file oracle. A pinned test-only quickstart gate now proves
      both identity variants across replay, exact aspect read-back, and direct
      graph relationships. Production publication, synchronization, state, and
      deletion remain separate work.
- [ ] Publish metadata to Conduktor Console only after verifying and specifying
      an official supported API, authentication, idempotency, review, deletion,
      and recovery contract.
- [ ] Add a Terraform/OpenTofu backend for Confluent Cloud resources.
- [x] Add deterministic, offline-validated Strimzi 1.2.0 `KafkaTopic` GitOps
      output for managed compiled topics. Installed-package parity and the
      pinned real reconciliation/replay/read-back gate pass. This does not add
      direct Kubernetes apply, controller management, credentials, or deletion.
- [ ] Add a Flink Kubernetes Operator backend where its lifecycle semantics fit.
- [ ] Add a real Confluent Cloud Flink Statements backend.

Exit criterion: the same reviewed streamt plan can produce portable metadata
and deploy through at least one stateful external backend without changing the
project model.

### Phase 3: production operations

Target: close the loop between declared intent and runtime evidence.

- [ ] Plan savepoint, last-state, and stateless Flink upgrades explicitly.
- [ ] Integrate Prometheus/OpenTelemetry signals and expose policy evaluation
      over those signals.
- [ ] Support generic Alertmanager/webhook failure actions.
- [x] Extract a typed provider-neutral state boundary while preserving the
      local version 1 JSON format and operation-wide same-host locking.
- [x] Add a strict local operation-control sidecar with intent-before-mutation,
      ordered progress, recovery blocking, and read-only plan status.
- [x] Add strict local/PostgreSQL deployment-state configuration, whole-block
      environment selection, sanitized no-fallback preflight, and an opt-in
      remote-state safety policy.
- [x] Add optional, bounded, read-only PostgreSQL `state status` inspection
      with strict version-1/version-2 catalog verification and secret-neutral
      failures.
- [x] Add explicit, confirmation-gated PostgreSQL `state init` with atomic
      version-1 catalog creation, restrictive ACL validation, idempotent address
      registration, fresh read-back verification, and PostgreSQL 14/18
      process-concurrency gates. New stores remain version 1 until explicitly
      migrated.
- [x] Add non-reserving PostgreSQL `state lock-status` diagnostics with exact
      catalog validation, primary-only endpoint checks, and explicit proof that
      every successful probe releases its transaction-scoped lock before
      returning. The instantaneous result does not reserve future work.
- [x] Add a provider-neutral operation snapshot that reads ownership and
      control at one locked workflow boundary and compares both at begin;
      require remote providers to atomically commit ownership, control
      clearing, and history while preserving local compatibility ordering.
- [x] Route local direct/reviewed apply and adoption through canonical planned
      actions, final state/control rereads, post-confirm adoption observation,
      and release-before-success before implementing private PostgreSQL
      operations.
- [x] Implement PostgreSQL ordinary reads, session-affine locking, and atomic
      mutation transitions; version-1 owner credentials remain isolated test
      scaffolding only.
- [x] Ship an explicit, confirmation-gated PostgreSQL schema-version-2
      administrative migration and validate the exact least-privilege writer
      role. Status and diagnostics understand exact v2.
- [x] Pass ordinary PostgreSQL plan/apply/adopt command E2E, failure-injection,
      process-concurrency, recovery, and installed-wheel gates on PostgreSQL 14
      and 18 for the supported direct standalone-primary topology.
- [x] Enable the ordinary PostgreSQL factory only in the final implementation
      commit after every backend, command, recovery, role, and release gate
      passes; require the exact v2 writer and never fall back to owner, local,
      or empty state.
- [x] Complete remote-state activation, monitoring, backup/restore, recovery,
      rollback, and topology documentation for installations that retain
      direct apply. Poolers and every HA/failover topology remain unsupported.
- [ ] Add curated, validated connector profiles after the connector contract is
      stable.

## Outside the active cycle

- Kafka Streams processing beyond the fixed typed projection/filter contract.
  A general SQL engine requires an explicit product decision.
- Additional runtimes such as RisingWave and Materialize are not scheduled.
- Additional transports such as Pulsar and Kinesis.
- A VS Code extension, hosted SaaS, and high-level intent generation.
- New ML syntax that is not backed by an executable target integration.
- Further Strimzi work, Kubernetes operators, Terraform/OpenTofu backends, and
  new catalog publishers. Keep existing tested exports working.
- Custom-application builds or deployment until D2 selects their exact scope.
