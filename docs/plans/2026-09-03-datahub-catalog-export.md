# DataHub catalog export implementation plan

## Objective and status

Implement the dependency-free, deterministic offline DataHub MCP export in
[`DataHub catalog export`](../specs/datahub-catalog-export.md).

Status on 2026-09-03: the dependency-free mapper, CLI, distribution and
compatibility gates have landed. CI run `33775922977` passed the complete
Python 3.10-3.12 release workflow. Public documentation and roadmap
reconciliation are complete; publication and real-GMS behavior remain outside
this plan.

| Slice | Status | Exit evidence |
| --- | --- | --- |
| 0 — v1.7 oracle | Complete | `24aefdb`; frozen URN/aspect vectors and offline reader result |
| 1 — identity and validator | Complete | `ffe1107`; dependency-free focused tests |
| 2 — mapper | Complete | `9b20848`; exact mapping/topology/warning tests |
| 3 — CLI/output | Complete | `76cb008`; canonical raw/JSON/file and atomic-failure tests |
| 4 — package/oracle gates | Complete | `0976fb2`; isolated wheels plus SDK, reader, and Python 3.11 file-source CLI oracle |
| 5 — public docs | Complete | public guide example gate, strict site build, CLI/support/release references, roadmap, and status reconciliation after green CI run `33775922977` |

The specification owns the wire contract. Conflicts must be resolved there
before code lands; tests must not establish a third contract.

## Frozen boundary

- `streamt docs datahub --catalog-id --fabric` with optional one Kafka instance
  and conditionally required Gateway platform ID/instance;
- Project to DataFlow, datasets to Dataset, actual processes to DataJob;
- exact catalog ID and process logical name as flow/job IDs, within official
  200-encoded-UTF-16-unit bounds;
- official-native Dataset URNs, never hashes;
- direct-only `DataJobInputOutput`, including all four legacy/preferred arrays;
- contract state only in `streamt.contract.status`;
- owner/tag mapping fully deferred with one warning per declaration;
- no ownership, tags, status, subtypes, publisher, state, or deletion;
- no runtime/optional DataHub package; and
- exact `acryl-datahub==1.7.0` only in isolated release-oracle tests.

## Slice 0 — official v1.7 oracle

### Ownership

- `tests/fixtures/datahub/v1.7.0/`
- an isolated package-oracle script under `tests/package/`

Do not edit production, CLI, public docs, or roadmap.

### Work

1. Install only exact `acryl-datahub==1.7.0` in a temporary environment.
   Before importing it, remove inherited `DATAHUB_*` variables, explicitly set
   `DATAHUB_DATASET_URN_TO_LOWER=false` and
   `DATAHUB_TELEMETRY_ENABLED=false`, and assert case preservation.
2. Freeze DataFlow/DataJob/Dataset/DataPlatformInstance URNs from official
   constructors, including every PDL maximum and reserved character. Treat a
   maximum the generated SDK does not enforce as a local PDL compatibility
   gate, not an SDK rejection claim.
3. Cover Kafka with and without an instance and an explicit custom Gateway
   platform/instance. Prove slash, space, literal percent, comma, parentheses,
   U+241F, rejected U+001F, case, Unicode, and post-encoding collisions. Include
   no-instance Kafka `main.orders` colliding with Gateway platform `kafka`,
   instance `main`, physical `orders`.
4. Construct every allowed generated aspect, validate
   `MetadataChangeProposalWrapper`, compare simplified objects, and round-trip
   official metadata files.
5. Confirm `DataJobInfo.type` serializes exactly as `{"string": kind}` and
   required empty info defaults remain stable.
6. Confirm `DataJobInputOutput` with empty `inputDatasets`/`outputDatasets` and
   minimal preferred edge arrays validates.
7. For no-instance Kafka, prove exact Dataset-URN parity while intentionally
   omitting the platform-only `dataPlatformInstance` emitted by the high-level
   helper; validate that omission with the wrapper and file source.
8. Verify the exact official file-source CLI dry-run command is free of network
   and remote mutation. Confine its expected empty sink-file write to a fresh
   temporary directory; never treat `datahub ingest mcps` as offline
   validation.
9. Reconstruct from a deep copy because v1.7.0 `from_obj` mutates its input.

If the offline wrapper or file source rejects preferred edges, amend the
specification. Do not silently duplicate relationships into deprecated arrays.

### Gate

```text
python tests/package/datahub_catalog_v170_oracle.py --self-test
git diff --check -- tests/fixtures/datahub/v1.7.0 tests/package/datahub_catalog_v170_oracle.py
```

## Slice 1 — dependency-free identity and validator

### Ownership

- `src/streamt/integrations/catalog/datahub.py` for value records, URN helpers,
  and closed validation only
- `tests/unit/test_datahub_validation.py`
- focused additions to the existing wheel/sdist inspection test

Do not implement mapping/CLI yet or edit the neutral model, Backstage, docs, CI,
or packaging dependencies.

### Work

1. Parse exact catalog ID, FabricType, platform ID, and instance inputs.
2. Hand-render only the specified DataFlow/DataJob/Dataset/platform-instance
   URNs with pinned character escaping and exact upstream length limits.
3. Keep Kafka platform fixed, its instance optional, and Gateway
   platform/instance explicit.
4. Preserve official literal-percent behavior and reject post-encoding identity
   collisions within the complete export. Test and document the specified
   cross-export non-injectivity instead of claiming every raw change changes a
   URN.
5. Implement a closed validator for exact MCP root keys, entity/aspect pairs,
   aspect fields/types/defaults, FabricType, cardinality, cross-references,
   edges, and collisions.
6. Add immutable private configuration/result records.
7. Prove wheel/sdist have no `acryl-datahub` requirement, `streamt[datahub]`
   extra, SDK imports, or vendored SDK payload.

### Acceptance

- all FabricType values and invalid casing/whitespace;
- exact encoded-UTF-16 flow/job bounds and official platform, instance,
  Dataset-name, and complete-URN bounds;
- exact official bytes for all Slice 0 vectors;
- invalid Unicode, overlong values, post-encoding alias/collision, malformed
  URNs, extra/missing fields, wrong pairs, and dangling edges fail;
- no generic URL quoting or Dataset hashing; and
- errors never contain raw forbidden input or exception text.

### Gate

```text
pytest -q tests/unit/test_datahub_validation.py <distribution-inspection-test>
ruff check src/streamt/integrations/catalog/datahub.py tests/unit/test_datahub_validation.py
mypy src/streamt/integrations/catalog/datahub.py
python -m build
```

Do not update or rely on the user-owned `uv.lock`.

## Slice 2 — mapper

### Ownership

- `src/streamt/integrations/catalog/datahub_export.py` for the mapper and
  immutable export result; identity construction and closed validation remain
  in `src/streamt/integrations/catalog/datahub.py`
- `tests/unit/test_datahub_export.py`

Slice 1 lands first because the mapper depends on its identity records and
closed validator. Do not edit the neutral model, CLI, Backstage adapter,
packaging, or docs.

### Work

1. Emit one DataFlow/dataFlowInfo from exact catalog ID, fabric, project name,
   and optional description.
2. Emit every Dataset with native identity, exact datasetProperties, and a
   dataPlatformInstance only when an instance exists.
3. Emit one DataJob/dataJobInfo per actual process using exact logical job ID
   and union-object process type.
4. Resolve exact direct neutral inputs and the process's existing output into
   sorted minimal destination edges. Emit all four input/output arrays.
5. Preserve empty sink output and omit DataJob for process-free topics.
6. Emit only the contract-state custom property.
7. Emit W115-W118 once per declared fact; never echo owner/tag/exposure/sink
   content.
8. Emit no ownership, tags, status, subtypes, schema, column, or placeholder
   entity.
9. Validate the complete immutable proposal tuple before returning.

### Acceptance

- empty, source-only, process-free topic, Flink, Gateway, and Connect shapes;
- direct inputs/exact outputs/empty sink outputs, with no transitive edges;
- Kafka instance/no-instance and mandatory Gateway identity options;
- contract absent/declared/enforced;
- one owner/tag warning per original declaration and duplicate exposure
  warnings retained;
- every duplicate/collision/dangling target fails; and
- neutral/compiler sentinel secrets never reach results or errors.

### Gate

```text
pytest -q tests/unit/test_datahub_export.py tests/unit/test_datahub_validation.py
ruff check src/streamt/integrations/catalog/datahub.py src/streamt/integrations/catalog/datahub_export.py tests/unit/test_datahub_export.py
mypy src/streamt/integrations/catalog/datahub.py src/streamt/integrations/catalog/datahub_export.py
```

## Slice 3 — canonical CLI and atomic output

### Ownership

- `src/streamt/cli/commands/docs.py`
- `src/streamt/core/errors.py`
- `tests/unit/test_cli_datahub.py`
- one small dependency-free canonical/atomic helper only if necessary

Do not edit mapper tests, the projection, Backstage, packaging, docs, state, or
deployers.

### Work

1. Register `E508_DATAHUB_INVALID` and W115-W118.
2. Add only frozen options; no owner map, URL, token, publisher, state, delete,
   force, or environment fallback.
3. Validate primitive inputs, compile dry-run exactly once, build projection,
   enforce conditional Gateway options, map, validate, serialize, then
   optionally atomically replace.
4. Serialize canonical UTF-8 JSON with two spaces, Unicode unescaped, sorted
   object keys, LF, and one final newline while preserving array order.
5. Text/no-file stdout is raw only; text/file stdout empty; quiet suppresses
   text; JSON uses the exact specified envelope/counts.
6. Build all bytes before same-directory private write/flush/fsync/replace and
   clean staging files on every failure.
7. Redact all expected/unexpected errors to bounded E508.

### Acceptance and gate

Tests prove one compile; zero state/provider/network/subprocess/DataHub imports;
raw/structured proposal equality; exact counts/warnings; deterministic bytes;
all text/JSON/quiet/file modes; and destination preservation/cleanup.

```text
pytest -q tests/unit/test_cli_datahub.py tests/unit/test_cli_backstage.py
ruff check src/streamt/cli/commands/docs.py src/streamt/core/errors.py tests/unit/test_cli_datahub.py
mypy src/streamt/cli/commands/docs.py src/streamt/integrations/catalog/datahub.py
```

## Slice 4 — distribution and compatibility gates

### Ownership

- `tests/package/datahub_catalog_wheel_smoke.py`
- isolated oracle script under `tests/package/`
- `.github/workflows/ci.yml` package-job additions
- Slice 0 integration fixtures/test

Do not edit production, unit tests, docs, or other jobs.

### Gates

1. Build wheel/sdist and assert no DataHub dependency/extra/payload.
2. Run installed `streamt` outside the checkout with DNS/socket/HTTP/provider
   and subprocess network seams denied and no SDK installed.
3. Exercise source, topic, Flink, Gateway, sink, owners/tags, contracts,
   duplicate exposures, sentinels, raw/structured/file modes, and repeated
   canonical bytes.
4. In a separate environment install exact `acryl-datahub==1.7.0`; validate
   wrappers, compare simplified objects, and round-trip metadata files.
5. On Python 3.11, run the proven network-free official file-source CLI dry-run
   oracle with its file sink confined to a temporary directory. Scrub inherited
   `DATAHUB_*` variables and set the two frozen lowercase/telemetry values
   before every SDK or CLI import. On Python 3.10-3.12, run wrapper, reader, and
   canonical parity.

```text
python -m build
pytest -q <distribution-inspection-test>
python tests/package/datahub_catalog_wheel_smoke.py --wheel <built-wheel>
python tests/package/datahub_catalog_v170_oracle.py --artifact <generated-json>
```

Use Python 3.10-3.12, existing package artifacts, bounded timeouts/output,
temporary paths, and unconditional teardown/log upload conventions. Never add
the SDK to runtime/unit jobs.

## Slice 5 — public documentation

Starts only after Slices 0-4 pass. It owns a new DataHub reference, CLI/support/
release references, spec/plan status, roadmap, mkdocs nav, and focused docs
tests. It documents exact identity/options/warnings/sensitivity and calls the
artifact offline DataHub MCP metadata, never publication. It keeps ownership,
tags, native contracts, multiple instances, publication, state, deletion, and
Conduktor Console publication deferred.

```text
pytest -q tests/unit/test_docs_datahub_example.py tests/unit/test_doc_yaml_validation.py
mkdocs build --strict
git diff --check
```

## Merge order and release gates

```text
Slice 0 oracle -> Slice 1 identity -> Slice 2 mapper -> Slice 3 CLI
               -> Slice 4 package/oracle gates -> Slice 5 public docs
```

The SDK oracle may be prepared beside Slice 1, but production code cannot land
without exact vectors. Slice 4 tests committed artifacts, not a shared dirty
tree. Slice 5 reports only landed green behavior. Agents never modify/stage
user-owned untracked files, lockfiles, prompts, or unrelated changes.

Offline export support requires: SDK/file-source oracle parity; focused/full
tests, Ruff, mypy, strict docs on Python 3.10-3.12; no distribution dependency;
isolated installed-wheel behavior; network-free Python 3.11 CLI oracle;
canonical byte and structured equality; secret-neutrality; atomic failure
coverage; and public truth updates only after all prior gates pass.

## Deferred follow-ups

Separate normative contracts are required for owner/tag maps and authoritative
replacement; live REST/emitter publication; server reads/reconciliation/state/
deletion; native DataContracts/assertions; schemas/columns/field lineage/
destinations/exposures/domains/containers/status/subtypes/telemetry; multiple
instances; DataPlatform bootstrap; real-GMS acceptance/live lineage behavior;
and Conduktor Console publication.
