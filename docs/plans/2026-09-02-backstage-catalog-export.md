# Backstage catalog export implementation plan

## Objective

Ship a useful catalog integration without coupling streamt's compiler to one
catalog product or inventing an unverified mutating API. The first delivery is
therefore a deterministic, offline Backstage catalog export built from a
private, immutable, versioned neutral catalog projection.

The implementation must follow the normative
[`Backstage catalog export specification`](../specs/backstage-catalog-export.md).
This plan orders the work into independently committable slices and assigns
non-overlapping file ownership so that focused agents can work without merging
partially compatible contracts.

## Status — 2026-09-02

Implemented through the exact offline Backstage core-entity boundary. The
normative specification is frozen and this plan is reconciled to its
2026-09-02 contract. DataHub export and Conduktor Console publication remain
separate, unimplemented work.

| Slice | Status | Landed evidence |
| --- | --- | --- |
| Normative contract | Complete | `35bf386`, `e596275`, `baae40e` |
| 1 — schemas and validator | Complete | `7ccdd13`; wheel/sdist inspection and Node validator foundation in `6fe5d91` |
| 2 — neutral projection | Complete | `4fc715b`; strict adapter inputs in `9ee7439` |
| 3 — mapper and serializer | Complete | `8c26b4a` |
| 4 — CLI and atomic output | Complete | `1d4404f` |
| 5 — distribution and parity gates | Complete | `6fe5d91`, `6057a6e` |
| 6 — public documentation | Complete | Public reference, CLI reference, support matrix, release notes, and roadmap in this documentation slice |

The test, lint, type, documentation, wheel/sdist, isolated-wheel, and pinned
Node parity checks are executable release gates. A release still requires its
own green CI run; this status is not a substitute for that run's result.

The agreed first product boundary is:

- one Backstage `System` for the explicit streamt project and environment;
- `Resource` entities for declared sources, physical topic outputs, and
  Gateway virtual topics;
- `Component` entities only for actual stream-processing units;
- exact direct relationships derived from compiler resolution, never guessed
  transitive edges;
- the exact `streamt.dev/contract` state annotation on model outputs with a
  declared contract, not a schema body or copy of a full ODCS document;
- canonical multi-document YAML for text output and the ordinary streamt JSON
  envelope for structured output;
- pinned, offline Backstage schema validation; and
- no catalog network client, provider mutation, deployment state access, or
  deployed-runtime observation.

DataHub mapping and Conduktor Console publication are explicitly deferred. A
successful Backstage export must not be described as support for either one.

## Normative dependency and reconciliation gate

`docs/specs/backstage-catalog-export.md` owns the frozen wire contract. If an
implementation detail below appears to conflict with it, the specification
wins and this plan must be corrected before code is merged; tests must not
establish a third contract.

The concurrently agreed values are:

- Backstage tag `v1.54.2`, commit
  `4bfa231152c6e454a2728850f63c6feb3d396191`;
- Node parity package `@backstage/catalog-model@1.10.0`;
- private projection type names `CatalogSnapshot`, `CatalogDataset`,
  `CatalogProcess`, `CatalogDependency`, `CatalogOwnerLabel`, and
  `CatalogContractSummary`;
- command shape:

  ```text
  streamt docs backstage \
    --catalog-id <ID> \
    --catalog-namespace <NAMESPACE> \
    --default-owner-ref <REF> \
    --lifecycle <LIFECYCLE> \
    [--owner-map <PATH>] \
    [--kafka-cluster-ref <REF>] \
    [--gateway-cluster-ref <REF>] \
    [--domain-ref <REF>] \
    [--output-file <PATH>]
  ```

- command error `E507_BACKSTAGE_INVALID`;
- bounded warnings `W113_BACKSTAGE_SINK_OUTPUT_OMITTED` and
  `W114_BACKSTAGE_EXPOSURE_OMITTED`;
- exact upstream schema closure:
  `Entity.schema.json`, `EntityEnvelope.schema.json`,
  `EntityMeta.schema.json`, `shared/common.schema.json`, and
  `kinds/{System,Resource,Component}.v1alpha1.schema.json`.

The following private implementation-level names may be refined without
changing the normative wire contract:

- the compressed package-resource filenames listed in Slice 1;
- projection version `1` and the private builder name
  `build_catalog_snapshot`;
- Backstage result type `BackstageCatalogExport` and mapper name
  `generate_backstage_catalog`.

The owner-map grammar is already exact: strict JSON with only integer
`version: 1` and an `owners` object, with duplicate-key detection at every
level and exact case-sensitive owner labels. The frozen inclusive limits are
1,048,576 UTF-8 bytes, nesting depth 4, 10,000 owner entries, 256 code points
per label, and 256 code points per reference. The parser must reject a byte
order mark and unpaired surrogate, boundary-test every limit, and never treat
the file as YAML.

None of these names is public Python API. Compatibility attaches to the CLI,
entity documents, deterministic serialization, codes, and documented mapping.

## Non-goals

- publishing, updating, or deleting entities through a Backstage API;
- reading an existing Backstage catalog to infer ownership or identity;
- Conduktor Console publication or use of Console topic-creation APIs as a
  metadata substitute;
- DataHub Metadata Change Proposal generation or emitter integration;
- catalog ingestion configuration, locations, refresh jobs, or authentication;
- copying generated SQL, connection configuration, endpoints, credentials,
  state locations, operation history, or provider observations into metadata;
- claiming runtime lineage, health, deployment, or job lifecycle visibility;
- adding Backstage, Node, DataHub, or Conduktor client dependencies to the
  streamt runtime package; or
- changing the streamt DSL solely to fill optional catalog fields.

## Frozen invariants

Every slice must preserve these properties.

1. **Pure and offline.** Parsing and one `Compiler.compile(dry_run=True)` call
   are the only project-processing steps. No state backend, deployer, provider,
   DNS, HTTP, Kafka, or catalog client may be constructed.
2. **Neutral first.** The projection contains streamt catalog meaning, not
   Backstage `apiVersion`, `kind`, annotations, or entity refs. The Backstage
   mapper is a consumer of the projection.
3. **Immutable and versioned.** Projection containers and nested values are
   deeply immutable, retain a literal projection version, and do not retain a
   mutable `StreamtProject`, manifest, resolved-model, or compiled-model object.
4. **Allowlisted metadata.** Only names, descriptions, declared tags and
   owners, absent/declared/enforced Model-contract state, materialization
   identity, and
   exact resolved direct dependencies may enter the projection. SQL and all
   runtime/provider configuration are forbidden even when they appear benign.
5. **No invention.** Catalog identity, namespace, lifecycle, default owner,
   owner mappings, and optional domain/cluster refs are explicit inputs.
   Missing optional knowledge is omitted or produces one specified bounded
   warning; it is never synthesized from an endpoint or environment name.
6. **Stable identity.** Backstage names and refs are produced by one documented
   canonical encoder. The exporter rejects blank values, invalid refs,
   duplicate generated identities, collisions across entity kinds, ambiguous
   ownership, and dangling internal relations before writing any bytes.
7. **Exact topology.** Relationships reflect direct compiler-resolved edges.
   They do not claim transitive dependencies. Sources and topic outputs are
   resources; actual processing models are components; Gateway virtual topics
   are separate resources rather than aliases for physical Kafka topics.
8. **All-or-nothing validation.** Every generated entity passes the pinned
   envelope and kind validator, plus local cross-entity semantic checks, before
   stdout or `--output-file` changes. The validator performs no remote `$ref`
   retrieval.
9. **Deterministic output.** The same parsed project, environment, compiler
   result, and explicit catalog options produce byte-identical YAML. Entity,
   relation, owner, tag, mapping, and annotation order is stable;
   timestamps and random identifiers are absent; YAML aliases are disabled.
10. **Safe output.** Text mode emits only validated canonical YAML to stdout;
    warnings go to stderr. JSON mode uses the normal structured formatter.
    File output is an atomic same-directory replace and never exposes partial
    output. Errors and warnings are secret-neutral and use stable locations.
11. **Portable packaging.** The wheel and source distribution contain the
    exact pinned schemas. Installed-wheel export works from outside the
    checkout with repository imports removed and network calls disabled.

## Slice 1 — pinned Backstage schemas and offline validator

### Files owned by the schema agent

- `src/streamt/integrations/catalog/__init__.py`
- `src/streamt/integrations/catalog/backstage_validation.py`
- `src/streamt/docs/schemas/backstage-1.54.2-entity.json.gz.b64`
- `src/streamt/docs/schemas/backstage-1.54.2-entity-envelope.json.gz.b64`
- `src/streamt/docs/schemas/backstage-1.54.2-entity-meta.json.gz.b64`
- `src/streamt/docs/schemas/backstage-1.54.2-common.json.gz.b64`
- `src/streamt/docs/schemas/backstage-1.54.2-system-v1alpha1.json.gz.b64`
- `src/streamt/docs/schemas/backstage-1.54.2-resource-v1alpha1.json.gz.b64`
- `src/streamt/docs/schemas/backstage-1.54.2-component-v1alpha1.json.gz.b64`
- `src/streamt/docs/schemas/README.md`
- `tests/unit/test_backstage_validation.py`

The decoded documents must remain byte-for-byte upstream artifacts; do not
rewrite `$id`, `$ref`, formatting, or property order to make resolution easier.

### Work

1. Vendor the seven-document schema closure from the pinned Backstage commit.
   Encode each as deterministic gzip (`mtime=0`) plus base64, matching the
   existing schema-resource convention.
2. Record upstream tag, commit, source path, license, decoded byte size, and
   SHA-256 for every document in `src/streamt/docs/schemas/README.md` and as
   constants next to the loader.
3. Decode with strict base64, decompress, verify size and SHA-256, decode UTF-8,
   parse JSON, require an object root, and call
   `Draft7Validator.check_schema` for every schema.
4. Build an explicit in-memory registry for Backstage's nonstandard bare IDs
   and refs such as `Entity`, `EntityMeta`, and `common#...`. Unknown or remote
   resolution must fail locally; the validator must not fetch schemas.
5. Validate every entity first against `EntityEnvelope.schema.json` and then
   against exactly one allowed kind schema. Reject other kinds and versions.
   Sort validation failures by JSON path and message and expose one stable,
   secret-neutral `BackstageValidationError`.
6. Add local checks that the schema set has exactly the expected IDs and refs,
   so a future upstream pin cannot silently broaden the accepted vocabulary.

### Acceptance

- all seven resources pass checksum, Draft-07 schema, registry, and positive
  entity tests;
- corrupt base64, gzip, checksum, JSON, schema, missing-ref, remote-ref,
  envelope, wrong-kind, wrong-version, and kind-schema failures are bounded and
  deterministic;
- tests monkeypatch network entry points and prove validation stays offline;
- the validator accepts representative `System`, `Resource`, and `Component`
  documents that also pass the pinned Node implementation.

### Gate

```bash
uv run pytest -q tests/unit/test_backstage_validation.py
uv run ruff check src/streamt/integrations/catalog/backstage_validation.py \
  tests/unit/test_backstage_validation.py
uv run mypy src/streamt
uv run python scripts/check_mypy_baseline.py
git diff --check
```

Suggested commit: `feat(catalog): pin Backstage schemas and offline validator`.

## Slice 2 — immutable provider-neutral projection

Slice 2 starts only after the schema/resource names and the normative mapping
inputs are frozen. It does not depend on Backstage validation at runtime, but
the order prevents two agents from inventing incompatible identity contracts.

### Files owned by the projection agent

- `src/streamt/integrations/catalog/model.py`
- `src/streamt/integrations/catalog/__init__.py` (projection exports only,
  serialized after the Slice 1 edit)
- `tests/unit/test_catalog_projection.py`

No other agent edits the package `__init__.py` during this slice.

### Work

1. Define frozen private value types `CatalogSnapshot`, `CatalogDataset`,
   `CatalogProcess`, `CatalogDependency`, `CatalogOwnerLabel`, and
   `CatalogContractSummary`. Use tuples, frozen dataclasses, and immutable
   scalar values all the way down; a frozen outer dataclass containing a list
   or mutable mapping is not sufficient.
2. Set the exact projection version to `1`. Reject unsupported versions at
   every mapper boundary rather than optimistically interpreting them.
3. Implement `build_catalog_snapshot(project, resolved_models,
   compiled_models, *, effective_environment)` as a pure function. Inputs must
   be the complete post-compile views from the same `Compiler` instance.
   Reject partial/mismatched model keysets. Catalog ID and namespace are
   Backstage adapter inputs and must not enter the neutral snapshot.
4. Represent separately:
   - each declared source dataset and its physical Kafka topic;
   - each model's physical topic output when one exists;
   - each compiled Gateway virtual topic output when one exists;
   - each model that represents an actual streaming process; and
   - every exact direct source/model dependency needed by that process.
5. Preserve only the allowlisted declared description, tags, owner label,
   absent/declared/enforced Model-contract state, and materialization facts
   required by the mapping. Do not retain columns, schema bodies, counts,
   checksums, SQL, a complete ODCS document, or arbitrary user configuration.
6. Produce stable logical keys independent of Backstage syntax. Reject missing
   targets, generated-key collisions, duplicate physical identities, and any
   compiler/project disagreement before returning a snapshot.
7. Retain only exact exposure logical names for later omission warnings,
   preserving duplicate declarations as separate omissions. The mapper emits
   only the specified sink/exposure warnings with stable safe locations; they
   never contain other exposure fields or connector configuration values.

### Acceptance

- full and minimal projects yield exact immutable snapshots;
- source-only, process, physical-output, and Gateway-virtual-topic cases remain
  distinct;
- direct dependency order and generated logical identity are deterministic;
- mutation attempts against every nested container fail;
- changes to an input object after construction cannot change a snapshot;
- partial compiler views, name collisions, ambiguous outputs, dangling edges,
  sinks, and exposures follow the frozen error/warning policy;
- repr, exception, and warning tests contain no SQL, endpoint, credential,
  connector config, or environment-secret sentinel.

### Gate

```bash
uv run pytest -q tests/unit/test_catalog_projection.py \
  tests/unit/test_compiled_models.py tests/unit/test_macros.py
uv run ruff check src/streamt/integrations/catalog/model.py \
  tests/unit/test_catalog_projection.py
uv run mypy src/streamt
uv run python scripts/check_mypy_baseline.py
git diff --check
```

Suggested commit: `feat(catalog): add immutable neutral projection`.

## Slice 3 — Backstage mapper, semantic checks, and serializer

Slice 3 consumes, but never reconstructs, the neutral snapshot. It may start
only after the Slice 2 types, version, identity keys, and warning behavior are
committed.

### Files owned by the mapper agent

- `src/streamt/integrations/catalog/backstage.py`
- `src/streamt/integrations/catalog/__init__.py` (Backstage exports only,
  serialized after Slice 2)
- `tests/unit/test_backstage_export.py`

The mapper agent does not edit validation resources, the projection builder,
CLI code, or public documentation.

### Work

1. Define immutable `BackstageCatalogExport` containing the validated entity
   tuple, warnings, and canonical YAML bytes/text. Define one pure
   `generate_backstage_catalog(snapshot, *, default_owner_ref, lifecycle,
   owner_map, kafka_cluster_ref, gateway_cluster_ref, domain_ref)` entry point.
2. Map one project/environment `System`, dataset `Resource` entities, and
   actual process `Component` entities exactly as frozen by the specification.
   A sink emits its proved Connect-process Component but no destination
   Resource. Do not emit placeholder Components for sources, exposures, or
   infrastructure.
3. Resolve owners by exact declared label through the strict owner map, then
   use the explicit default. Validate canonical Backstage refs. Never derive an
   owner from an email, endpoint, path, or current OS user.
4. Add only exact direct `dependsOn`/system/owner/domain relations. Use optional
   explicit Kafka and Gateway cluster refs only where the mapping is semantically
   exact. Sort and deduplicate relations without inventing missing entities.
5. Emit only the frozen `streamt.dev/...` annotations, including the compact
   contract summary. Annotation values must be bounded and deterministic. No
   raw manifest, SQL, full contract, runtime config, or provider address may be
   serialized.
6. Check name/ref collisions and all internal relation targets across the
   complete entity set, validate every entity through Slice 1, and only then
   serialize.
7. Serialize canonical multi-document YAML in the frozen entity order, using
   stable mapping order, UTF-8, `\n`, an explicit document separator policy,
   and a dumper that disables aliases. Reparse the YAML in tests and prove it
   is semantically identical to the validated entity tuple.

### Acceptance

- golden tests cover one System plus source/topic/Gateway Resources and actual
  process Components with exact direct relations;
- owner overrides, default owner, lifecycle, optional cluster refs, and domain
  refs have exact positive and negative tests;
- invalid name/ref, duplicate identity, dangling relation, unsupported
  projection version, unrepresentable metadata, and schema failures produce
  `E507_BACKSTAGE_INVALID`-ready safe failures before serialization;
- repeated generation and shuffled declaration inputs produce byte-identical
  YAML;
- the output contains no YAML anchors, Python tags, timestamps, SQL, config,
  endpoints, or secret sentinels;
- every golden entity passes both the vendored validator and the Node parity
  validator described in Slice 5.

### Gate

```bash
uv run pytest -q tests/unit/test_backstage_validation.py \
  tests/unit/test_catalog_projection.py tests/unit/test_backstage_export.py
uv run ruff check src/streamt/integrations/catalog tests/unit/test_backstage_*.py \
  tests/unit/test_catalog_projection.py
uv run mypy src/streamt
uv run python scripts/check_mypy_baseline.py
git diff --check
```

Suggested commit: `feat(catalog): map neutral metadata to Backstage`.

## Slice 4 — CLI and atomic output

### Files owned by the CLI agent

- `src/streamt/cli/commands/docs.py`
- `src/streamt/core/errors.py`
- `tests/unit/test_cli_backstage.py`

The CLI agent does not change the projection, mapper, validator, schema
resources, packaging workflow, or documentation. One owner must make all edits
to the already large `docs.py` command module.

### Work

1. Add the exact `streamt docs backstage` options from the normative contract,
   plus the existing `--project-dir/-p` and `--env/-e` project-selection
   options. Do not add a network target, token, publish flag, or implicit mode.
2. Require and validate catalog identity, namespace, default owner ref, and
   lifecycle before compilation. Parse the optional owner map strictly and
   provider-free. Reject duplicate keys, unknown top-level keys, blank values,
   non-string pairs, wildcard rules, and environment placeholders.
3. Parse the project with the standard warning routing. Construct one
   `Compiler`, call `compile(dry_run=True)` exactly once, and pass that same
   compiler's complete `resolved_models` and `compiled_models` to the neutral
   builder. No output artifact directory is written.
4. Convert projection and mapper failures to `E507_BACKSTAGE_INVALID` with a
   stable safe location. Emit only `W113_BACKSTAGE_SINK_OUTPUT_OMITTED` and
   `W114_BACKSTAGE_EXPOSURE_OMITTED` for the frozen lossy cases. Parser warnings
   retain their current codes and stay off the YAML stream.
5. In text mode, emit only canonical YAML to stdout unless `--output-file` is
   set. With a file, validate all entities and bytes first, write a sibling
   temporary file, fsync/close as required by the repository convention, and
   atomically replace the target. Do not print a success banner into document
   stdout.
6. In global JSON mode, use `OutputFormatter`. Its `data` object has exactly
   `standard`, `release`, `api_version`, `entities`, `counts`, and
   `output_file`; `counts` has exact ordered `System`, `Resource`, and
   `Component` keys. Never embed YAML as an escaped second representation.
   Quiet behavior follows existing docs commands without skipping file
   creation.
7. Prove via construction spies that every success and failure path is free of
   state, deployer, provider, and network construction.

### Acceptance

- exact help/options, missing-required-input, owner-map, parse, compile,
  projection, mapping, validation, and file failures use the frozen contract;
- text stdout is parseable multi-document YAML and warnings appear only on
  stderr; JSON stdout is one parseable formatter envelope;
- stdout and file modes contain the same canonical document bytes;
- a failed export leaves no new file and preserves an existing destination;
- compile is called once with `dry_run=True`; no generated project artifact is
  written;
- network/state/deployer/provider constructors are forbidden in tests;
- `STREAMT_*` and configuration secret sentinels do not appear in stdout,
  stderr, structured error fields, output files, or exception reprs.

### Gate

```bash
uv run pytest -q tests/unit/test_cli_backstage.py \
  tests/unit/test_backstage_export.py tests/unit/test_cli_docs_odcs.py \
  tests/unit/test_cli_openlineage.py
uv run ruff check src/streamt/cli/commands/docs.py src/streamt/core/errors.py \
  tests/unit/test_cli_backstage.py
uv run mypy src/streamt
uv run python scripts/check_mypy_baseline.py
git diff --check
```

If either adjacent test filename does not exist at implementation time, use the
repository's actual ODCS/OpenLineage CLI test filename; do not create an alias
solely to satisfy this command.

Suggested commit: `feat(cli): export validated Backstage catalog YAML`.

## Slice 5 — distribution and Node parity release gates

The Node validator is an independent release oracle, not a runtime dependency
and not a second serializer.

### Files owned by the distribution agent

- `tests/package/backstage_catalog_wheel_smoke.py`
- `tests/package/backstage_catalog_node_validate.cjs`
- `tests/unit/test_backstage_validation.py` (distribution-resource inspection
  section only, serialized after the schema agent)
- `.github/workflows/ci.yml` (package job only)

No package-gate work changes production source, schema payloads, unit mapping
behavior, docs, or roadmap. The schema agent and distribution agent must not
edit `test_backstage_validation.py` concurrently; transfer ownership after
Slice 1 merges.

### Work

1. Extend distribution inspection to build one wheel and one source
   distribution, enumerate the expected seven resources, decode them, and
   verify exact size/SHA pins from outside the checkout.
2. Run `backstage_catalog_wheel_smoke.py` with the isolated installed-wheel
   executable, `PYTHONPATH` removed, Python isolated mode where applicable,
   working directory outside the repository, and network entry points patched
   to fail. Compile a representative project and assert exact canonical YAML,
   warnings, relations, schema validation, secret neutrality, and repeat-byte
   determinism.
3. Install the exact independent parity package without modifying repository
   manifests or locks:

   ```bash
   audit_dir="$(mktemp -d)"
   npm install --prefix "$audit_dir" --ignore-scripts --no-save \
     --package-lock=false @backstage/catalog-model@1.10.0 yaml@2.8.1
   cp tests/package/backstage_catalog_node_validate.cjs "$audit_dir/validate.cjs"
   (cd "$audit_dir" && node validate.cjs < catalog.yaml)
   ```

4. The CommonJS validator must parse every YAML document and call the exact
   `systemEntityV1alpha1Validator`, `resourceEntityV1alpha1Validator`, or
   `componentEntityV1alpha1Validator` export according to kind, awaiting
   `.check(entity)` and failing on an unknown kind. It must not normalize or
   rewrite entities before checking them.
5. Keep `npm install` as a bounded package-job setup step. The installed streamt
   smoke itself remains offline, and normal Python unit/package execution does
   not require Node or npm.

### Acceptance

- wheel and source distribution contain exactly the expected decoded schema
  artifacts and pins;
- the installed executable succeeds outside the checkout without network;
- source-tree, wheel, and repeated wheel exports are byte-identical;
- every emitted entity passes both Python and Backstage Node validators;
- a deliberately invalid entity is rejected by both validators;
- the package job proves all three supported entity kinds and direct relation
  shapes, including a Gateway virtual-topic Resource;
- distribution output and logs contain no test secret, endpoint, SQL, raw
  configuration, or repository path.

### Gate

```bash
uv run python -m build
STREAMT_TEST_DISTRIBUTIONS_DIR="$PWD/dist" \
  uv run pytest -q \
  tests/unit/test_backstage_validation.py::test_built_wheel_and_source_distribution_contain_backstage_resources

isolated_root="$(mktemp -d)"
cp tests/package/backstage_catalog_wheel_smoke.py "$isolated_root/"
(cd "$isolated_root" && env -u PYTHONPATH \
  /tmp/streamt-wheel-smoke/bin/python -I backstage_catalog_wheel_smoke.py)

audit_dir="$(mktemp -d)"
npm install --prefix "$audit_dir" --ignore-scripts --no-save \
  --package-lock=false @backstage/catalog-model@1.10.0 yaml@2.8.1
cp tests/package/backstage_catalog_node_validate.cjs "$audit_dir/validate.cjs"
(cd "$audit_dir" && node validate.cjs < catalog.yaml)

git diff --check
```

CI may use runner-scoped temporary paths instead of `/tmp`, but it must retain
the clean installed-wheel boundary. Suggested commit:
`test(package): gate Backstage export distributions and parity`.

## Slice 6 — public documentation and roadmap truth

This slice lands only after Slices 1–5 and their release gates pass.

### Files owned by the documentation agent

- `docs/specs/backstage-catalog-export.md` (implementation status only)
- `docs/plans/2026-09-02-backstage-catalog-export.md` (status only)
- `docs/reference/backstage-catalog.md`
- `docs/reference/cli.md`
- `docs/reference/support-matrix.md`
- `docs/reference/release-notes.md`
- `mkdocs.yml`
- `ROADMAP.md`
- `tests/unit/test_docs_backstage_example.py`

The documentation agent does not modify source, schemas, unit implementation
tests, package scripts, or CI.

### Work

1. Document the exact offline command, required identity/owner/lifecycle
   inputs, owner-map grammar, YAML/JSON/file behavior, entity mapping,
   annotations, warnings, validation pin, determinism, and safe failure model.
2. Add an executable minimal example and a richer example covering a process,
   topic output, Gateway virtual topic, owner mapping, and optional refs.
3. State the limits prominently: this exports design metadata only; it neither
   publishes to a catalog nor observes deployment/runtime lifecycle.
4. Record the wheel/source-distribution resource gate, isolated-wheel smoke,
   and Backstage Node parity gate in support and release documentation.
5. Do not mark the existing combined Phase 2 roadmap item complete. Split it
   into truthful sub-items, or annotate Backstage export as complete while
   leaving DataHub export and Conduktor Console publication unchecked.
6. Link the normative specification, this execution plan, and reference page
   without making a future Console/DataHub design normative by implication.

### Gate

```bash
uv run pytest -q tests/unit/test_docs_backstage_example.py
uv run mkdocs build --strict
git diff --check
```

Suggested commit: `docs(catalog): document offline Backstage export`.

## Parallel execution and ownership

The dependency order is strict even when agents run concurrently:

```text
normative contract
       |
       v
Slice 1 schemas/validator ----+
       |                      |
       v                      |
Slice 2 neutral projection    |
       |                      |
       v                      v
Slice 3 mapper/serializer --> Slice 5 gate scaffolding
       |
       v
Slice 4 CLI/output ----------> Slice 5 executable gates
                                      |
                                      v
                              Slice 6 docs/roadmap
```

- The schema agent owns the schema resources, README schema section,
  validator, and validator tests until Slice 1 is committed.
- The projection agent owns only `model.py`, model tests, and its
  serialized `__init__.py` export edit.
- The mapper agent starts after the projection commit and owns only
  `backstage.py`, mapper tests, and its serialized `__init__.py` edit.
- The CLI agent starts after mapper APIs are frozen and exclusively owns
  `docs.py`, `core/errors.py`, and CLI tests.
- The distribution agent may prepare isolated scripts after filenames and wire
  contracts freeze, but edits the package workflow and shared validation test
  only after prior ownership transfers.
- The documentation agent runs last and records only executable truth.

No slice may smuggle later-slice behavior into its commit. In particular,
Slice 1 has no mapper, Slice 2 imports no Backstage vocabulary, Slice 3 has no
Click or filesystem output, Slice 4 has no network/publisher, and Slice 5 has
no production fixes hidden in a test commit.

## Final acceptance matrix

| Concern | Unit | CLI | Installed wheel | Wheel/sdist | Node parity |
| --- | --- | --- | --- | --- | --- |
| Pinned schema integrity and offline refs | required | indirect | required | required | independent |
| Neutral deep immutability and exact coverage | required | indirect | smoke | n/a | n/a |
| System/Resource/Component mapping | required | required | required | n/a | required |
| Direct dependency and owner/ref semantics | required | required | required | n/a | required |
| Canonical deterministic YAML | required | required | byte repeat | n/a | parsed |
| Structured JSON envelope | n/a | required | smoke | n/a | n/a |
| Atomic file output and failure preservation | n/a | required | required | n/a | n/a |
| No network/state/deployer/provider | spies | spies | hard fail | n/a | Python smoke only |
| Secret/config/SQL neutrality | sentinels | sentinels | sentinels | resource-only | document sentinels |
| Bounded errors and warnings | required | required | smoke | corrupt artifact | invalid entity |

## Release gates

All required release gates now have executable coverage. Each release must
still run them and remain green:

- [x] The normative spec and this plan agree on every mapping and output rule.
- [x] All seven vendored schemas match the pinned upstream commit and pass
  no-network Draft-07 validation.
- [x] The full projection, mapping, and CLI focused suites run in the Python
  3.10–3.12 test matrix.
- [x] Ruff and the repository's zero-error mypy baseline are CI gates.
- [x] The full unit suite and strict MkDocs build are CI gates.
- [x] Isolated-wheel export and exact wheel/source-distribution resource
  inspection run outside the checkout.
- [x] Every representative emitted entity is checked by
  `@backstage/catalog-model@1.10.0` with release-test-only `yaml@2.8.1`.
- [x] Public docs and the roadmap leave DataHub and Conduktor Console
  publication explicitly deferred.

Console publication requires a separate evidence-backed specification naming
a supported metadata-only API, authentication/TLS/timeout behavior, external
identity and ownership rules, reviewed mutation semantics, idempotency and
delete policy, and recovery behavior. DataHub requires a separate mapping and
versioned Metadata Change Proposal/aspect contract. Neither may reuse the
Backstage serializer or claim support merely because both can consume the same
neutral projection.
