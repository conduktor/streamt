# OpenLineage durable apply telemetry execution plan

## Objective

Complete slice 5 of the frozen
[`OpenLineage integration specification`](../specs/openlineage-integration.md)
by making an explicitly enabled `streamt apply` operation observable without
changing deployment, state, recovery, or failure semantics.

The OpenLineage run observes the existing durable operation. It does not create
a second operation identity, describe deployed Flink or connector lifecycles,
or treat infrastructure resources as datasets.

## Status — 2026-09-02

Implemented in `5b4ab25`, with the isolated-wheel, source-distribution, and real
PostgreSQL 14/18 executable release gates added in `a9386d6`. The event model,
namespace resolver, offline validator, File/HTTP transports, and finite
`streamt test` lifecycle were reused without a dependency, event schema, state
schema, or transport change.

The local acceptance matrix and the full Python 3.10–3.12 workflow pass,
including isolated-wheel, source-distribution, and real PostgreSQL 14/18 gates.

The work was split into three independently reviewable logical chunks:

1. command lifecycle wiring and exhaustive local acceptance tests;
2. installed-wheel and real PostgreSQL composition gates;
3. public documentation and roadmap closure after every executable gate passes.

## Frozen command contract

`streamt apply` adds the same four explicit options as `streamt test`:

```text
--emit-openlineage
--openlineage-job-namespace <NAMESPACE>
--openlineage-kafka-namespace <KAFKA-URI>
--openlineage-gateway-namespace <KAFKA-URI>
```

Merely setting OpenLineage environment variables does not enable emission.
When emission is not requested, OpenLineage configuration is not read and the
ordinary apply path is unchanged.

When enabled, namespace resolution, event construction, offline validation,
transport configuration, and transport creation occur only after parse,
validation, review, confirmation, safety, planning, dry-run, and final state
drift gates. They occur before `begin_operation`, so a preflight failure is
fatal `E506_OPENLINEAGE_INVALID` and cannot leave a durable marker or mutate a
provider.

The run has:

- job name `streamt/{encoded-project}/commands/apply`;
- job type `BATCH`, integration `STREAMT`, job type `COMMAND`;
- run ID equal to `OperationIntent.operation_id`;
- START time equal to `OperationIntent.started_at`;
- no inputs or outputs;
- no actions, plan data, artifacts, SQL, provider identities, state locations,
  runtime configuration, or secrets.

Explicit Kafka and Gateway namespace options are still validated for a
consistent public CLI contract, but they are not emitted as datasets.

## Exact durable ordering

The successful ordering is:

```text
ordinary gates and final drift check
OpenLineage preflight
begin_operation
START delivery attempt
progress and provider mutations
state compare-and-swap plus durable marker clear
COMPLETE delivery attempt
state authority release
transport close
formatter flush
```

START delivery failure is a fixed, bounded
`W112_OPENLINEAGE_EMIT_FAILED` warning and never blocks mutation. COMPLETE is
truthful as soon as the state compare-and-swap succeeds and the durable marker
is cleared. Therefore a verified post-commit authority-release failure still
has a COMPLETE event while the command retains its normal
`E426_STATE_RELEASE_FAILED_AFTER_COMMIT` result with `committed: true`.

Runtime failure, unknown commit, lost authority before confirmed commit, or any
recovery-required outcome produces FAIL after the command has made its existing
best effort to persist the conservative recovery truth. `KeyboardInterrupt`
after START produces ABORT. No START means no terminal event.

Every terminal event reuses the START run and job identities and has a fresh
canonical UTC transition time. FAIL contains only the fixed generic error
message `streamt apply command did not complete successfully`.

Terminal delivery and transport-close failures add only the fixed W112 warning.
They cannot change deployment results, rollback, recovery markers, exit status,
or the original exception. The transport is closed before the final formatter
flush so warnings remain visible in structured output.

Zero-action durable applies emit a normal START/COMPLETE pair. Repeated applies
use fresh durable operation/run UUIDs.

## Chunk 1: command lifecycle and local acceptance

Status: complete in `5b4ab25`.

### Source

- Update `src/streamt/cli/commands/apply.py` only.
- Reuse the existing OpenLineage construction, validation, namespace, and
  transport primitives.
- Keep lifecycle delivery state local to the apply command; do not refactor the
  already stable test-command integration in this chunk.
- Preserve direct and reviewed apply convergence before the durable intent.

### Required local tests

Add `tests/unit/test_openlineage_apply_command.py` and prove:

- no opt-in means no configuration, transport, or event calls even with invalid
  OpenLineage environment values;
- direct, reviewed, zero-action, and repeated applies emit exact pairs;
- the persisted operation UUID and timestamp exactly match START;
- START follows durable begin and precedes progress/provider mutation;
- COMPLETE follows state commit and marker clearance;
- direct and reviewed runtime failures emit FAIL after recovery truth is durable;
- interruption emits ABORT and preserves exit status 130;
- parse, validation, review, confirmation, safety, existing-recovery, planning,
  drift, and dry-run exits emit nothing;
- namespace, event, and transport preflight failures return E506 before durable
  begin or mutation;
- unknown commit, lock loss, and release-after-commit have the frozen terminal
  classification;
- START, terminal, and close delivery failures preserve all business and state
  outcomes while adding at most the bounded W112 warning;
- text and structured JSON output remain valid and secret-neutral;
- events contain no datasets, infrastructure identities, plans, actions, or
  deployed-runtime claims.

### Chunk 1 gate

- focused OpenLineage apply and existing apply/state/recovery tests;
- existing OpenLineage test-command, event, validation, and transport tests;
- Ruff and the repository's zero-error mypy baseline;
- full unit/scenario suite;
- `git diff --check`.

## Chunk 2: distribution and PostgreSQL composition

Status: complete. Executable gates landed in `a9386d6`, were stabilized in
`62a74de` and `2ca58e9`, and pass in the final combined CI workflow.

### Installed wheel

Add `tests/package/openlineage_apply_wheel_smoke.py` and run it from the clean
wheel environment with repository imports removed. It must perform a real local
durable apply through the File transport, validate the START/COMPLETE pair, and
prove the run ID is the durable operation ID.

Also close the existing distribution-resource gap by verifying the vendored
OpenLineage schemas in both the built wheel and source distribution.

### PostgreSQL v2

Add `tests/postgres/test_postgres_openlineage_apply_real.py` and include it in
the checkout and installed-wheel PostgreSQL 14/18 jobs. It must use the real v2
backend to prove:

- direct and reviewed success emit START/COMPLETE;
- emitted run identity equals the persisted operation-history identity;
- runtime failure emits START/FAIL only after a real recovery marker exists;
- no transport failure changes state or recovery truth.

No external OpenLineage service is required. The File transport composes the
command boundary; the existing HTTP transport suite remains authoritative for
bounded HTTP delivery.

### Chunk 2 gate

- isolated wheel smoke;
- wheel and source-distribution resource inspection;
- PostgreSQL 14 and 18 checkout tests;
- PostgreSQL 14 and 18 installed-wheel tests;
- full package build and install checks;
- full CI workflow.

## Chunk 3: public contract and roadmap closure

Status: complete. Implementation truth is closed in the normative
specification, implementation plans, public reference documentation, support
matrix, release notes, and roadmap. Strict documentation and the combined CI
workflow pass.

After chunks 1 and 2 have executable gates, update:

- `docs/specs/openlineage-integration.md` with implementation status and the
  explicit verified post-commit release classification;
- `docs/plans/2026-09-01-openlineage-integration.md` with slice 5 completion;
- `docs/reference/openlineage.md` with the durable apply lifecycle, examples,
  failure policy, and limitations;
- `docs/reference/cli.md` with all four options and removal of the stale claim
  that ordinary apply telemetry is unsupported;
- `docs/reference/support-matrix.md`;
- `docs/reference/release-notes.md`;
- `ROADMAP.md`.

The documentation gate is strict MkDocs plus executable examples and the full
documentation test suite. Documentation must say that the events describe only
the finite streamt control-plane command, never the lifecycle of a submitted
streaming workload.

## Non-goals

- required or transactional OpenLineage delivery;
- a durable telemetry outbox;
- OpenLineage client dependency adoption;
- implicit emission from environment variables;
- provider resources represented as datasets;
- deployed Flink, Connect, Kafka, or Gateway runtime lifecycle monitoring;
- action-by-action events, plan facets, state facets, or provider error text;
- changing apply, rollback, recovery, state, or reviewed-plan semantics.
