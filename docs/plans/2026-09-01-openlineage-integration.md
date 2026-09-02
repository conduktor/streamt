# OpenLineage integration implementation plan

## Objective

Implement the normative contract in
[`docs/specs/openlineage-integration.md`](../specs/openlineage-integration.md)
without overstating runtime visibility. Land each slice as an independently
reviewable commit after its focused and repository-wide gates pass.

## Status — 2026-09-01

Slice 1 is complete in commits `5491f0d` and `665dd4d`. The repository now has
the pinned OpenLineage schemas, pure typed event/identity helpers, fail-closed
offline validation, and an installed-wheel resource smoke test. The normal CI
unit matrix runs that coverage on Python 3.10 through 3.12.

Slice 2 is complete. Shared resolved-model SQL and dependencies landed in
`c2cc695`; the secret-safe compiled-model projection landed in `40dc830`; the
pure namespace and static event mapper landed in `925eb40`; and the public
`streamt docs openlineage` command and acceptance suite landed in `537252e`.
Static `DatasetEvent` and `JobEvent` export is supported. No transport or
runtime-emission command is implemented yet.

The target is OpenLineage 1.53.0 at signed release commit
`8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe`. The core schema retains its
`2-0-2` identifier. The implementation must use the artifact sizes, checksums,
schema URLs, mappings, and omissions in the normative specification.

## Work-order principles

1. Static design metadata lands before any network transport or command hook.
2. Offline validation lands before event generation is user-visible.
3. Runtime transport is opt-in and bounded before commands can call it.
4. Test telemetry lands before apply telemetry because it has no deployment
   state transaction to preserve.
5. Apply hooks reuse the existing durable operation UUID and boundaries; they
   do not create a parallel operation model.
6. Field lineage waits for a correctness upgrade and the pinned OpenLineage
   1.53 explicit lineage facet.

No slice may edit deployment state, plan, apply, or adoption behavior except
the explicit apply event hooks in slice 5. Unrelated user workspace changes are
never included in an OpenLineage commit.

## Slice 1: pinned schemas, event model, and offline validation

Progress: complete in `5491f0d`, with the clean installed-wheel build and
validation gate made independent of `uv` in `665dd4d`. This is an internal
construction and validation foundation only; it did not add a public export or
runtime emission surface.

### Scope

- Add the first-party OpenLineage integration package:
  - `src/streamt/integrations/openlineage/__init__.py`
  - `src/streamt/integrations/openlineage/events.py`
  - `src/streamt/integrations/openlineage/validation.py`
- Vendor the core and eight used facet schemas beneath
  `src/streamt/docs/schemas/` using the established gzip/base64 resource form.
- Extend `src/streamt/docs/schemas/README.md` with the release commit, exact
  upstream paths, sizes, checksums, and Apache-2.0 attribution.
- Implement immutable event/facet URL constants, typed internal records, stable
  job-segment encoding, dataset namespace validation, and semantic validation.
- Add the planned `E506_OPENLINEAGE_INVALID`,
  `W110_OPENLINEAGE_SCHEMA_INCOMPLETE`, and
  `W111_OPENLINEAGE_SINK_OUTPUT_OMITTED` identifiers without using them from a
  public command yet.

### Required tests

- `tests/unit/test_openlineage_validation.py`
- `tests/unit/test_openlineage_events.py`
- A wheel/source-distribution resource smoke test, either in
  `tests/unit/test_release_workflow.py` or a dedicated packaging test.

Tests must cover resource integrity, invalid compressed/base64 content,
official schema self-validation, closed-reference behavior, `FormatChecker`
enforcement, event-kind allowlists, facet URL/key matching, identity/field
collisions, percent encoding, and finite lifecycle pairing.

An offline test must make every unexpected network attempt fail and still
validate representative DatasetEvent, JobEvent, and RunEvent records.

### Exit gate

- Every vendored resource is importable from an installed wheel.
- Every representative event validates with the network unavailable.
- Corrupt resources and semantically invalid but core-schema-permitted events
  fail with stable safe locations.
- No CLI behavior changes.

## Slice 2: deterministic static export

Progress: complete in `c2cc695`, `40dc830`, `925eb40`, and `537252e`. The
implementation uses compiler-resolved macro dependencies, an immutable
secret-safe primary-artifact projection, strict namespace resolution, a pure
validated mapper, and the public offline CLI. User-facing reference and
support-matrix updates landed only after the executable command gate passed.

### Scope

- Add `streamt docs openlineage` in
  `src/streamt/cli/commands/docs.py` with the exact CLI, environment precedence,
  JSONL, structured JSON, warning, and atomic-file contract.
- Build DatasetEvent and JobEvent records from a validated project plus dry-run
  compiled artifacts.
- Reuse physical topic and virtual-topic names from compiled artifacts.
- Expose or extract one shared resolved-model-SQL/dependency helper in
  `src/streamt/compiler/compiler.py` and/or `src/streamt/core/dag.py` so macro
  references are not lost. Do not create an OpenLineage-only regex parser.
- Preserve strict parsing and existing docs command behavior.
- Add `docs/reference/openlineage.md`; update `docs/reference/cli.md`,
  `docs/reference/support-matrix.md`, `mkdocs.yml`, and `ROADMAP.md` only after
  the static acceptance gate passes and concurrent roadmap work is settled.

### Required tests

- `tests/unit/test_docs_openlineage.py`
- Focused compiler/DAG tests for rendered macro dependencies if a shared helper
  changes those modules.
- Strict documentation example validation.

The CLI tests must include:

- sources with and without columns;
- model contract precedence, including an explicitly empty contract;
- `topic` with and without SQL, effective `flink`, Gateway virtual topic, and
  sink materializations;
- physical topic overrides and Gateway proxy namespace separation;
- direct rather than transitive inputs;
- a rendered macro containing both `source` and `ref` dependencies;
- human ownership without an invented role;
- single-bootstrap derivation and multi-broker failure;
- source/model dataset identity collisions, job collisions, duplicate fields,
  and input/output collisions;
- canonical event and JSON-key ordering with a frozen timestamp;
- raw JSONL stdout, a normal global structured envelope, atomic replacement,
  cleanup after write failure, and no partial output;
- structured warnings for incomplete schema and omitted sink output;
- fixtures containing SQL literals, passwords, connector credentials, and
  reviewed-plan-like data that never appear in events or errors.

### Exit gate

- The command is deterministic for fixed time and inputs.
- All emitted events pass the slice 1 validator offline.
- No broker, registry, Flink, Connect, Gateway admin, state, plan, or network
  read occurs.
- Text and structured output remain machine-consumable.
- Only after this gate may documentation call static OpenLineage export
  supported. Runtime telemetry remains planned.

## Slice 3: bounded File and HTTP transport

Progress: Slice 3A, the pure transport-configuration boundary, is complete. It
loads only an explicit bounded duplicate-free YAML mapping, applies an exact
modern environment overlay, rejects implicit and legacy configuration, and
returns frozen secret-safe File or HTTP settings. It performs no file append,
HTTP request, event emission, or command hook.

The File/HTTP boundary does not use `openlineage-python`. streamt already owns
the validated event dictionaries and has `requests` as a core dependency. A
direct synchronous adapter can enforce the narrower timeout, retry, TLS,
redirect, environment-inheritance, and close contract without accepting client
factory defaults. No dependency or lockfile change belongs to this slice.

### Scope

- Add the pure configuration foundation in
  `src/streamt/integrations/openlineage/transport.py` with frozen secret-safe
  records, bounded duplicate/alias-safe YAML loading, exact environment
  precedence, strict File/HTTP allowlists, and safe located errors.
- Require explicit modern official transport configuration and an explicit
  command enable flag; never discover an implicit configuration or console
  output path.
- Parse only the transport boundary. Reject facet, tag, filter, normalization,
  legacy alias, arbitrary nested environment, and custom transport
  configuration.
- In the next Slice 3B commit, add a local append-only File adapter and a
  synchronous direct HTTP adapter without changing a CLI command.
- Enforce URL credential rejection, TLS verification, timeout and retry bounds,
  per-event validation before delivery, explicit close behavior, and redacted
  diagnostics.
- Add `W112_OPENLINEAGE_EMIT_FAILED`.

### Required tests

- `tests/unit/test_openlineage_transport.py`
- An import/package smoke test proving the transport boundary has no
  OpenLineage runtime dependency.

Slice 3A tests prove:

- missing configuration and `OPENLINEAGE_DISABLED=true` fail before command
  execution;
- console, Kafka, composite, async, remote-file, and custom transports are
  rejected;
- HTTP TLS-disable, credentials in URLs, excessive timeout, and excessive retry
  settings are rejected;
- authorization and configuration values never appear in record reprs or
  exception text;
- an explicit config file is UTF-8, regular, size/nesting/token bounded, and
  duplicate/alias-free;
- exact recognized nested environment fields override by presence, including
  an empty value that must fail rather than fall back.

Slice 3B tests use a fake transport or local mock HTTP server and must prove:

- events are validated before the first transport call;
- local file events are appended and flushed in order;
- HTTP never inherits proxy or `.netrc` state, never follows redirects, keeps
  TLS verification enabled, and attempts at most one retry;
- transport close is bounded and deterministic;
- delivery failures become the planned warning rather than an unhandled error
  after execution starts.

### Exit gate

- No configuration produces implicit console output.
- HTTP behavior is bounded by the normative limits.
- No transport test requires an external service.
- Static export and transport configuration remain usable without an
  OpenLineage runtime dependency.

## Slice 4: finite test-command RunEvents

### Scope

- Add the shared OpenLineage runtime options to
  `src/streamt/cli/commands/test.py`.
- Precompute and validate one aggregate command job and run before invoking
  `TestRunner`.
- Resolve only selected sample-test targets to input datasets.
- Emit START immediately before execution and exactly one COMPLETE, FAIL, or
  ABORT terminal attempt.
- Preserve the current exit status and structured test result when delivery
  fails.
- Do not change the existing truth that schema tests are structural, continuous
  tests observe status, and `--deploy` is unimplemented.

### Required tests

- Extend `tests/unit/test_cli.py`, `tests/unit/test_cli_json.py`,
  `tests/unit/test_sample_runner.py`, or add
  `tests/unit/test_openlineage_test_command.py` when clearer.

Tests must prove:

- UUIDv4 is new per non-empty invocation and stable across the pair;
- START and terminal job/run identity are identical;
- only sample-test topics appear as inputs;
- schema, continuous, coverage, empty selection, and unimplemented deployment
  paths do not claim dataset reads or executions;
- all pass maps to COMPLETE, assertion/infrastructure failures map to FAIL, and
  interruption maps to ABORT;
- the bounded redacted ErrorMessage facet appears only on FAIL;
- transport failure does not turn a passing test into failure or hide a real
  test failure;
- global JSON output is not corrupted by event delivery.

### Exit gate

- One finite test invocation has at most one START and one terminal attempt.
- The event never claims that a continuous Flink test job ran.
- Existing test command and runner suites remain unchanged when emission is not
  explicitly enabled.

## Slice 5: durable apply-command RunEvents

### Scope

- Add the shared OpenLineage runtime options and narrowly scoped hooks to
  `src/streamt/cli/commands/apply.py`.
- Construct and validate the command job before runtime mutation.
- Reuse `OperationIntent.operation_id` and `started_at`; do not create a second
  operation or run identifier.
- Attempt START after durable `begin_operation` and before the first planner
  action.
- Attempt COMPLETE only after state compare-and-swap and durable operation
  clear.
- Attempt FAIL on execution/recovery-required failures and ABORT on
  `KeyboardInterrupt` after START.
- Keep event delivery outside the state lock where the lifecycle boundary
  permits; any call made while locked remains bounded by slice 3.
- Preserve existing recovery, lock, result, and exit semantics exactly.

### Required tests

- Extend `tests/unit/test_operation_control.py` and
  `tests/unit/test_deployment_safety.py`, or add
  `tests/unit/test_openlineage_apply_command.py` for the event matrix.

Tests must prove:

- parse, validation, confirmation, review, safety-block, dry-run, and preflight
  exits before `begin_operation` emit no RunEvent;
- the START run UUID is the exact durable operation UUID;
- no planner mutation can occur before the START boundary is attempted;
- COMPLETE cannot precede successful state commit and operation clear;
- planner error, state commit error, recovery marking, and interruption choose
  the correct terminal type without altering recovery state;
- apply events contain no Kafka datasets or infrastructure resources;
- a transport failure before or after mutation cannot trigger rollback, clear a
  needed recovery marker, change an apply success to failure, or replace the
  original apply error;
- retries or repeated commands never reuse an old operation UUID;
- no event claims Flink job completion or running status.

### Exit gate

- All existing operation-control and deployment-safety tests pass unchanged
  when emission is disabled.
- OpenLineage hooks observe the durable operation; they do not become part of
  its source of truth.
- Only after slices 3 through 5 pass may documentation call streamt command-run
  telemetry supported.

## Slice 6: proven field lineage with the 1.53 explicit lineage facet

### Scope

- Treat this as a separate correctness feature, not cleanup attached to static
  export.
- Replace or substantially harden the current regex-oriented
  `ColumnLineageBuilder` using parsed and macro-resolved SQL.
- Define supported SQL constructs and fail/omit at unsupported boundaries.
- Vendor and integrity-check the pinned `LineageFacet.json` artifact specified
  in the normative contract.
- Add a `lineage` facet to output DatasetEvent records only when every emitted
  field relationship has a resolved physical dataset and column identity.
- Use OpenLineage 1.53 `LineageDatasetFacet`; do not add the legacy
  `ColumnLineageDatasetFacet` in parallel.
- Do not serialize SQL expressions in transformation descriptions until a
  separate secret-redaction and disclosure contract exists.

### Required tests

- Dedicated parsed-SQL field-lineage tests for identity projection, aliases,
  joins, filters, grouping, windows, conditional expressions, masks, ambiguous
  bare columns, nested queries, macro SQL, and unsupported constructs.
- Physical namespace/topic resolution tests across Kafka and Gateway datasets.
- Official `LineageFacet.json` validation, duplicate-field/collision checks,
  deterministic ordering, and no-network tests.
- Regression tests proving table-level export is unchanged when field lineage
  is absent or unsupported.

### Exit gate

- No field relationship is based solely on a regex guess.
- Every emitted input field resolves to one exact physical dataset identity.
- Unsupported or ambiguous expressions never produce plausible but incorrect
  lineage.
- The static exporter remains valid when this optional detail is omitted.

## Explicitly deferred after the six slices

The following require their own specifications and are not hidden acceptance
criteria for this plan:

- a durable OpenLineage delivery outbox and required-delivery mode;
- deployed Flink integration or correlation with Flink's own OpenLineage
  listener;
- periodic RUNNING snapshots and runtime metrics;
- parent/root run context propagation;
- Kafka or asynchronous transports;
- connector-specific external dataset identity plugins;
- OpenLineage ingestion or catalog synchronization;
- persistent OpenLineage fields in the strict streamt project DSL.

## Verification commands per slice

Each implementation commit runs the focused tests named above, followed by:

```text
uv run ruff check src tests
uv run mypy src
uv run pytest tests/unit
uv run pytest tests/scenarios
uv run mkdocs build --strict
uv build
```

The built wheel must be installed in a clean environment and smoke-tested for
schema resource availability and `streamt docs openlineage --help`. Runtime
slices also run the existing apply/test focused suites most closely related to
their hooks.

Before every commit:

- inspect `git diff --check`;
- inspect `git status --short`;
- exclude unrelated and user-owned files;
- update support documentation and roadmap checkboxes only for behavior whose
  full acceptance gate passed.

## Commit strategy

Use one commit for each completed slice. A slice may be divided further when a
schema/validator foundation is independently useful, but later-slice behavior
must not be pulled into an earlier claim. In particular:

- do not combine static export with network transport;
- do not combine test and apply lifecycle hooks;
- do not combine field-lineage parser work with table-level export;
- do not mark deployed Flink telemetry complete anywhere in this plan.
