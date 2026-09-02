# Open Data Contract Standard export implementation plan

## Objective

Implement the truthful project-wide ODCS v3.1.0 export specified in
`docs/specs/odcs-export.md` after the AsyncAPI 3 work lands, without adding
runtime, deployment, catalog, or project-DSL behavior.

## Status — 2026-09-01

The normative product and validation contract is specified. No ODCS schema,
generator, CLI command, support claim, or roadmap completion is implemented
yet.

AsyncAPI 3 currently owns overlapping work in `src/streamt/cli/commands/docs.py`,
`src/streamt/docs/schemas/`, the schemas README, CLI documentation, the support
matrix, and `ROADMAP.md`. ODCS implementation starts only after that logical
chunk is committed so neither standards slice overwrites the other.

## Fixed decisions

- Target exactly ODCS `v3.1.0` from release commit
  `b9d3ffc5aabe9e058afe4469cabe5a218fe9946d`.
- Vendor the official 86441-byte companion JSON Schema whose SHA-256 is
  `2cb7dd6fe43344d2233e0406438622681dc3ebadcf8f0d606a15b40c8f6752c0`.
- Emit one ODCS document per parsed streamt project, not one per model or topic.
- Require explicit contract ID and status. Use an explicit contract-version
  option or exact project version; never fabricate lifecycle metadata.
- Include all declared sources and models as ODCS schema objects, with truthful
  streamt resource provenance.
- Export declared schema facts only. Do not compile SQL or query runtime
  systems.
- Validate offline against the pinned official schema and stricter local
  semantic rules before any serialization or write.
- Keep existing AsyncAPI, OpenAPI compatibility, dictionary, schema, HTML docs,
  runtime, deployment, and state behavior unchanged.

## Delivery order

Each slice is independently committable. Do not combine schema vendoring,
mapping behavior, CLI plumbing, and broad documentation in one change.

### Slice 1: normative export contract

Files:

- `docs/specs/odcs-export.md`
- `docs/plans/2026-09-01-odcs-export.md`

Tasks:

1. Record the immutable upstream release commit, primary-source links, schema
   size, checksum, and license.
2. Fix the one-project-document boundary and explain why per-model documents
   are out of scope.
3. Define required identity, status, version, mapping, omissions, deterministic
   IDs, collisions, validation, output, security, and non-goals.
4. Define executable acceptance requirements and this staged implementation
   order.

Acceptance:

- The specification has no dependency on mutable `main` or `latest` artifacts.
- Every required ODCS root field has an explicit source or an explicit error.
- The mapping never claims inferred owners, lifecycle, quality, SLA, servers,
  types, or transformations.
- Only these two documents change in this slice.

Suggested commit:

```text
docs: specify truthful ODCS export
```

### Slice 2: pinned schema and pure export core

Expected files:

- `src/streamt/docs/odcs.py`
- `src/streamt/docs/schemas/odcs-3.1.0.json.gz.b64`
- `src/streamt/docs/schemas/README.md`
- `src/streamt/docs/__init__.py`
- `tests/unit/test_odcs.py`

Tasks:

1. Download the schema only from the pinned release commit and verify its exact
   byte length and SHA-256 before encoding it as a package text resource.
2. Record provenance, checksum, decoded format, and Apache-2.0 attribution next
   to the existing standards-schema documentation.
3. Implement a narrow resource loader using `importlib.resources`, strict
   base64 and gzip decoding, runtime size/checksum verification, JSON decoding,
   and `Draft201909Validator.check_schema`.
4. Implement typed ODCS document construction from a field allowlist.
5. Implement root metadata validation, exact source/model/property mapping,
   safe Flink-to-ODCS logical types, model-contract precedence, physical topic
   rules, primary-key positions, and explicit custom provenance.
6. Implement canonical UUIDv5 seeds, object ordering, declaration-order
   properties, and all collision checks.
7. Validate the completed document with `Draft201909Validator` and
   `FormatChecker`, then apply the textual-standard semantic checks.
8. Return typed export warnings and failures; do not print, write files, read
   network state, or construct runtime/state services in the pure module.

Focused tests:

- The packaged schema decodes to exactly 86441 bytes and the pinned checksum.
- The schema is Draft 2019-09-valid and has no non-local references.
- Minimal and complete generated documents validate against the official
  schema.
- The local validator still requires a non-empty `schema` array.
- Root ID, status, and version are required and copied exactly.
- Source fields and `SchemaRef.fields` map exactly.
- A present model contract wins over model columns, including an empty
  contract; otherwise explicit model columns are used.
- Kafka-output models receive only known topic identities; sinks receive no
  guessed destination.
- Descriptions, tags, required/nullability, classification, primary-key flags,
  positions, and contract enforcement map exactly.
- Every supported scalar type maps correctly. Unknown, complex, and binary
  types retain physical type and have no logical-type fallback.
- UUIDs match golden values, remain stable under source/model reordering, and
  change when the contract ID changes.
- Duplicate names, properties, topics, generated IDs, missing primary-key
  properties, and classification conflicts fail deterministically.
- Runtime URLs, credential-shaped values, SQL, owners, tests, exposures,
  current time, project path, and environment-specific backend details never
  appear in the document or failure text.
- Network clients, sockets, runtime deployer factories, and deployment-state
  factories can be patched to raise while generation still succeeds.

Gates:

```text
pytest -q tests/unit/test_odcs.py
ruff check src/streamt/docs/odcs.py tests/unit/test_odcs.py
mypy src/streamt
python scripts/check_mypy_baseline.py
```

Suggested commit:

```text
feat: generate validated ODCS documents
```

### Slice 3: CLI and serialization

Expected files:

- `src/streamt/cli/commands/docs.py`
- `src/streamt/core/errors.py`
- `tests/unit/test_cli_odcs.py`

Tasks:

1. Add the `docs odcs` sibling command without changing `docs asyncapi`, the
   historical `docs openapi` compatibility alias, or other docs subcommands.
2. Add project/environment, contract ID, status, contract version, document
   format, and output-file options exactly as specified.
3. Validate semantically required options inside the command so global JSON
   mode receives `E505_ODCS_INVALID` in the standard envelope.
4. Emit raw YAML by default or raw JSON when requested. Keep stdout free of
   warnings, Rich markup, and status prose when no output file is requested.
5. Put parser and incomplete-schema warnings on stderr and in structured JSON
   warnings.
6. Implement the global JSON data shape with the document as an object,
   standard/version metadata, selected serialization, and nullable output path.
7. Serialize before writing and atomically replace only the explicitly selected
   output file. Clean temporary files on every failure.
8. Translate generation, validation, serialization, and write failures into
   credential-redacted structured errors with deterministic locations.

Focused tests:

- `docs odcs --help` documents every option and does not rename existing
  commands.
- Missing ID, status, and both forms of version fail with the stable error code
  in text and global JSON modes.
- Text-mode YAML and JSON stdout parse directly and contain exactly one
  document.
- Global JSON stdout parses as exactly one command envelope and contains the
  same document object.
- Local `--format` does not replace or corrupt the global output-envelope
  selection.
- Parser and incomplete-schema warnings never contaminate raw document stdout.
- The output-file bytes equal stdout serialization for the same inputs.
- Output-file replacement is atomic and a forced write failure leaves no
  partial file or temporary artifact.
- Single- and multi-environment parsing remain offline; runtime endpoints do
  not enter output.
- Existing AsyncAPI/OpenAPI, schema, dictionary, and HTML docs command tests
  remain unchanged and pass.

Gates:

```text
pytest -q tests/unit/test_odcs.py tests/unit/test_cli_odcs.py
pytest -q tests/unit/test_cli.py tests/unit/test_cli_json.py tests/unit/test_cli_smoke.py
ruff check src/streamt/cli/commands/docs.py src/streamt/core/errors.py tests/unit/test_cli_odcs.py
mypy src/streamt
python scripts/check_mypy_baseline.py
```

Suggested commit:

```text
feat: expose ODCS project export
```

### Slice 4: public documentation and release gate

Expected files after the command is executable:

- `docs/reference/cli.md`
- `docs/reference/support-matrix.md`
- `ROADMAP.md`
- any existing strict documentation-example test file selected by the
  AsyncAPI implementation

Tasks:

1. Document the exact command, required metadata, YAML default, JSON option,
   global JSON envelope, output-file behavior, and pipe-safe stderr rules.
2. Add a support-matrix row that says ODCS v3.1.0 project-wide schema export is
   supported and explicitly says quality, SLA, team, roles, servers, import,
   catalog publication, runtime enrichment, and per-model documents are not.
3. Mark the ODCS roadmap item complete only after the installed-wheel command
   and offline validator work.
4. Add a complete documentation example whose project parses under the
   production parser and whose generated output validates against the bundled
   official schema.
5. Build and install the wheel in a clean environment and confirm that the
   schema resource is present and `streamt docs odcs` runs without source-tree
   access.
6. Run the broad unit, scenario, documentation, packaging, Ruff, and zero-error
   mypy gates before claiming support.

Release acceptance:

- `mkdocs build --strict` succeeds.
- A clean installed wheel loads the bundled ODCS schema and generates valid
  YAML and JSON offline.
- The complete unit suite and relevant scenarios pass.
- Existing AsyncAPI 3.1 and `docs openapi` compatibility output remain valid.
- The support matrix and roadmap make no claim beyond the implemented schema
  subset.
- No source, runtime, deployment, state, catalog, or connector behavior changes
  in the release documentation chunk.

Suggested commit:

```text
docs: publish ODCS export support
```

## Cross-slice acceptance matrix

| Area | Required coverage |
| --- | --- |
| Upstream pin | Release commit, path, size, checksum, license, no mutable URL |
| Packaging | Resource loads from wheel without repository files |
| Root contract | Exact ID, name, version, status, apiVersion, kind, schema |
| Boundary | One project, one document, every source/model exactly once |
| Source mapping | Topic identity, columns, types, descriptions, tags, classifications |
| Model mapping | Contract precedence, declared-column fallback, sink omission, primary keys |
| Type safety | Exact physical type, conservative logical types, no string fallback |
| Identity | Canonical UUIDv5 seeds, golden IDs, reorder stability, collision rejection |
| Semantics | Non-empty schema, no missing PK fields, no conflicting classifications |
| Official validation | Draft 2019-09 schema and format validation with sorted errors |
| Offline behavior | No HTTP, DNS, sockets, deployers, state, or live schema access |
| Output | Raw YAML/JSON, stderr warnings, atomic file, normal global JSON envelope |
| Security | No credentials, runtime endpoints, SQL, provider errors, or secret values |
| Compatibility | Existing docs commands and AsyncAPI/OpenAPI behavior unchanged |
| Documentation | Strict build, executable examples, precise support boundaries |

## Risks and controls

### Textual standard and schema differ

Control: pin both to the same release, validate against the official companion
schema, and maintain explicit local semantic checks for textual requirements.
Do not patch the vendored schema silently.

### Contract identity is not in the DSL

Control: require it at the CLI. Do not derive root identity from project name,
path, Git remote, environment, or runtime configuration. A future persistent
identity model requires a separate DSL specification and migration.

### Sources and outputs share one contract

Control: add exact `streamtResourceType` provenance and fail on physical-topic
ambiguity. Do not merge or overwrite objects. Per-resource contracts remain a
separate product design.

### Flink types exceed ODCS logical types

Control: preserve the exact physical type and omit uncertain logical types.
Recursive complex-type support requires its own parser and fixtures.

### Current project version is optional

Control: accept an explicit contract version and otherwise require a non-empty
project version. No default is allowed.

### Raw document output can be corrupted by warnings

Control: reserve stdout for the document or global JSON envelope. Send warnings
and diagnostics to stderr and the structured warnings array.

### Standards slices overlap files

Control: land AsyncAPI first, then begin ODCS Slice 2. Stage only exact files for
each commit and rerun both standards' focused tests after every overlapping
edit.

## Explicit non-goals for this plan

- Editing the streamt DSL or generated stream-project JSON Schema.
- Exporting tests as ODCS data quality or exposures as SLAs, teams, or roles.
- Publishing contracts to Conduktor Console, Backstage, DataHub, or any remote
  catalog.
- Runtime observation, enrichment, deployment planning, apply, adoption, or
  deployment state.
- ODCS import, per-model output, selectors, archives, or multiple standard
  versions.
- Changing AsyncAPI 3 output or removing the OpenAPI compatibility alias.

## Commit discipline

Commit each slice separately after its focused gates pass. Do not stage the
unrelated user plan, prompts, lock file, state-backend work, or other concurrent
agent changes. The roadmap and support matrix move only in Slice 4, after the
installed command is truthful and validated.
