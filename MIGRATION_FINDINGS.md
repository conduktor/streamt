# Streamt Migration Findings: Real-World UX Assessment

**Scenario**: Multi-team fintech company (payments, fraud, analytics, compliance) migrating 9
pre-existing Kafka topics to streamt. Teams have legacy consumer groups, inconsistent topic
naming, and evolving schemas.

**Environment**: Kafka 3.8.0, Schema Registry 7.5.0, 9 legacy topics, no Flink available.

**Methodology**: Full CLI traversal — `init`, `validate`, `compile`, `lineage`, `list`, `show`,
`plan`, `test`, `docs`, `status`, `envs` — plus deliberate error scenarios and edge cases.

---

## 1. Onboarding / `init`

### ✅ What works well
- `streamt init --discover --kafka localhost:9092` correctly discovers all 9 topics, names them by
  sanitising `.` → `_`, creates `sources/discovered.yml` with partition counts. Good first step.
- `--include payments*` glob filter works correctly.
- `--dry-run` shows what will be created before writing.

### ❌ Issues found

**[INIT-1] Empty scaffold is misleading**
`streamt init` creates empty `models/`, `sources/`, `tests/` directories with no example files.
A new user is immediately stuck — there's no hint of what goes in these directories.
*Expected*: At minimum one commented-out example in each directory, or a link to docs in the README
scaffold.

**[INIT-2] `.sql` files in `models/` are silently ignored**
If a user creates `models/my_model.sql` (a natural instinct from dbt where `.sql` is the primary
file type), it is silently ignored. Only `.yml`/`.yaml` files are parsed. Zero error, zero warning.
*Impact*: Users waste hours debugging why their models don't appear.
*Fix*: Warn when `.sql` files found in `models/` that don't correspond to a YAML model definition.

**[INIT-3] `sources/` directory layout not documented in scaffold**
The init command creates `sources/` but leaves it empty. The `--discover` flag shows that sources
go in `sources/discovered.yml` but manually adding `sources/my_team.yml` isn't demonstrated.
Users copy the monolithic `stream_project.yml` pattern without knowing about multi-file layout.

**[INIT-4] Schema Registry discovered but not used**
`--schema-registry` option exists on `init --discover` but even with Schema Registry running,
no schema information is populated into discovered sources. The `columns:` field stays empty.
*Expected*: If SR has schemas for discovered topics, populate `columns:` automatically.

---

## 2. Schema / Source Definition

### ❌ Issues found

**[SCHEMA-1] `schema.fields` is silently ignored — CRITICAL**
Users naturally write:
```yaml
sources:
  - name: payments_raw
    topic: payments.raw.v1
    schema:
      fields:
        - name: payment_id
          type: STRING
```
This is a natural attempt to declare inline column types. The `schema:` key maps to `SchemaRef`
(for registry references only: `registry`, `subject`, `format`, `definition`). The `fields:` sub-key
is simply dropped by Pydantic without error.

Result: `Source.columns` is always empty, `list sources` shows `columns: 0`, and the `show source`
command returns `"columns": []` even though the user thinks they've declared the schema.

*Impact*: All type inference for models downstream of this source falls back to `STRING`/`_raw`.
Data contracts on sources become useless. Column validation warnings never fire.

*Fix*: Either (a) support `schema.fields` as an alias for `columns:`, or (b) emit a validation
warning when `schema:` contains unknown keys.

**[SCHEMA-2] Source `columns:` works but isn't shown in `list sources`**
The correct syntax is `columns: [{name: ..., type: ...}]` directly on the source. This works and
columns appear in `show source`. But `list sources` shows `columns: 0` for all sources regardless,
because it calls `len(s.columns)` but the `columns` field refers to `ColumnDefinition` objects
which are only populated when `columns:` is used directly (not via `schema.fields`).
*Fix*: `list sources` should show actual column count when `columns:` is specified.

**[SCHEMA-3] `required:` field in source schema is parsed but ignored**
`columns: [{name: payment_id, type: STRING, required: true}]` — the `required` field doesn't exist
on `ColumnDefinition` and is silently dropped. There's no NOT NULL propagation to generated DDL.
*Fix*: Either add `required` to `ColumnDefinition` or document that it isn't supported.

---

## 3. Environment Configuration

### ❌ Issues found

**[ENV-1] `environments:` in `stream_project.yml` is silently ignored**
The YAML spec apparently supports top-level `environments:` key (users will try this). Once an
`environments/` directory exists, the `runtime:` section in `stream_project.yml` is also ignored
with only a WARNING that's easy to miss:
```
WARNING: runtime: in stream_project.yml is ignored in multi-environment mode
```
*Impact*: A user who puts `environments:` in their main YAML gets no behavior change. When they
later create the `environments/` directory, their `runtime:` config silently stops working.
*Fix*: Either parse `environments:` from `stream_project.yml`, or emit an ERROR (not warning)
when `runtime:` is present alongside `environments/` directory.

**[ENV-2] `environments:` key in YAML causes confusion about where config lives**
The dual-mode system (single: `runtime:` in main file, multi: `environments/` directory) is
not discoverable. The `envs list` command says "No environments configured (single-env mode)"
which is correct but confusing — users don't know they need to create a directory.

**[ENV-3] No `-e` flag auto-resolution from STREAMT_ENV in all commands**
Commands support `STREAMT_ENV` env var but multi-env mode forces you to always pass `-e dev`
explicitly on every single command. Even in CI, every command needs the flag.
*Expected*: `STREAMT_ENV` env var should work consistently as the default.
Test: `STREAMT_ENV=dev streamt validate` — this works, but `-e` is still needed when no env var.

**[ENV-4] Prod environment has no safety gates by default**
`streamt envs show prod` shows `confirm_apply: False` and `allow_destructive: True`. No
confirmation prompt for production deploys unless users manually configure safety settings.
Users migrating from dbt expect `--target prod` to require confirmation.
*Fix*: Default `confirm_apply: True` for environments named `prod`/`production`.

---

## 4. Validation

### ✅ What works well
- `E102_MODEL_NOT_FOUND` with "Did you mean?" — excellent UX.
- `E104_CYCLE_DETECTED` explains the full cycle and how to break it.
- `E202_FLINK_REQUIRED` gives a concrete YAML snippet to copy-paste.
- Pydantic `Field required` error for missing `topic:` is surfaced (though raw Pydantic format).

### ❌ Issues found

**[VALIDATE-1] Invalid SQL passes validation — CRITICAL**
`SELECT FROM WHERE` (syntactically invalid SQL) passes validation with only a FLINK_REQUIRED
warning. No SQL parse error is raised. The `broken` model is classified as valid with
`materialized: flink`. In production, this means the error is deferred to deploy time.
*Impact*: The validate gate is not actually a SQL correctness gate — it's a structural gate only.
*Fix*: Run sqlglot parse on model SQL during validate and surface parse errors.

**[VALIDATE-2] Column warnings don't block on `--strict` mode (missing flag)**
`WARNING: Column 'first_payment_id' not found` — the column reference warning is just a warning,
not an error. There's no `--strict` flag to promote warnings to errors for CI pipelines.
*Expected*: `streamt validate --strict` should exit non-zero on any warning.

**[VALIDATE-3] Raw Pydantic errors for config mistakes**
When `topic:` is missing from a source, the error is:
```
ERROR: 1 validation error for Source
topic
  Field required
    For further information visit https://errors.pydantic.dev/2.12/v/missing
```
This leaks Pydantic internals and sends users to a generic Pydantic docs URL.
*Fix*: Catch Pydantic validation errors and translate to streamt-style error messages with
`E1xx_` codes pointing to streamt docs.

**[VALIDATE-4] `required: notabool` silently accepted**
`required: notabool` in a contract column is parsed without error — Pydantic coerces it to `True`
or ignores it. Users making typos in boolean config fields get no feedback.

**[VALIDATE-5] Source/model name collision goes undetected**
A source and model with the same name (`payments`) both pass validation. This creates ambiguity
when using `ref()` vs `source()` and can cause unexpected DDL conflicts at deploy time.
*Fix*: Error on name collision between sources and models.

**[VALIDATE-6] `payments_raw_v2` source declared but never used — no warning**
A declared source with no downstream models generates no "unused source" warning.
*Expected*: Optional `--warn-unused` flag or at least a hint that the source has no consumers.

---

## 5. Compilation

### ✅ What works well
- Temporal join (`FOR SYSTEM_TIME AS OF`) compiles to correct Flink SQL.
- Interval joins (`BETWEEN ... AND ...`) preserved exactly in generated SQL.
- Window TVFs (TUMBLE, HOP, CUMULATE) pass through correctly.
- `REGEXP_REPLACE` (PII masking) preserved correctly.
- Type inference propagates `DECIMAL(18,4)` from source schema through to sink DDL.

### ❌ Issues found

**[COMPILE-1] `list models` shows `virtual_topic` but compile generates Flink SQL for same model**
`payments_clean` is listed as `materialized: virtual_topic` (correct — it's stateless).
But `generated/flink/payments_clean.sql` exists and contains a full Flink INSERT statement.
When there's no Gateway configured, stateless models fall back to Flink — but `list` shows
the logical materialization, not the effective one. This is confusing.
*Impact*: Users expect `virtual_topic` models to NOT create Flink jobs. They see 11 Flink jobs
from 9 models and don't understand why 2 extra exist (the 2 are test jobs, but the stateless
models also have Flink fallback jobs).
*Fix*: `list models` should show effective materialization, or add a `(flink-fallback)` annotation.

**[COMPILE-2] Source tables generated with `_raw STRING` schema when no columns declared**
When a source has no `columns:` defined, generated DDL is:
```sql
CREATE TABLE IF NOT EXISTS users (`_raw` STRING) WITH (...)
```
This single `_raw STRING` column breaks temporal joins and any SQL that references named fields
from the source. The generated SQL will fail at Flink runtime even though it compiles fine.
*Impact*: All sources discovered via `init --discover` (no columns) produce broken DDL.
*Fix*: Warn at compile time when a source used in a model has no declared columns.

**[COMPILE-3] Test failures topic name inconsistency**
Generated test SQL uses `_streamt_test_failures` (single leading underscore) but codebase and
docs reference `__streamt_test_failures__` (double underscores). This is a naming inconsistency
in the generated artifacts vs code comments.

**[COMPILE-4] No check that `window_start`/`window_end` are in GROUP BY**
The `rapid_payments` model references `first_payment_id` and `last_payment_id` which don't exist
in the source — validate correctly warns. But the `window_start`, `window_end` GROUP BY pattern
in window TVFs is never validated (the HAVING clause with non-existent columns isn't caught either).

**[COMPILE-5] `HAVING` clause with aliases not validated**
`HAVING COUNT(*) >= 3` works, but if user writes `HAVING payment_count >= 3` (using the alias),
this is valid in some SQL dialects but not Flink. No validation catches this.

---

## 6. Lineage

### ✅ What works well
- ASCII DAG rendering is clear and readable.
- `-m model` focus narrows the graph to relevant subgraph.
- `--format json` produces a clean machine-readable graph.
- Exposures are shown as terminal nodes with correct upstream links.

### ❌ Issues found

**[LINEAGE-1] `--upstream` and `--downstream` flags not implemented**
Both flags print a warning `not yet implemented` and fall back to full lineage.
These are documented in `--help`, advertised to users, and do nothing.
*Fix*: Either implement them or remove from help output until implemented.

**[LINEAGE-2] Orphaned source `payments_raw_v2` not highlighted**
The lineage ASCII art shows `payments_raw_v2 (source)` with nothing downstream — it appears
as a floating unconnected node. There's no visual distinction between "source feeding models"
and "source with no consumers" (which might be a mistake or legacy topic).

**[LINEAGE-3] Lineage ASCII art has visual alignment issues**
```
payments_raw_v2 (source)
    │
    ▼
users (source)
```
The indentation suggests `payments_raw_v2` is upstream of `users` — but they're independent
sources. The indentation is misleading. Each independent root should start at the same level.

**[LINEAGE-4] `current_user_risk` missing from JSON lineage downstream chain**
`current_user_risk` has no downstream models and no exposures, yet it appears in the lineage
graph as a dead-end model. No warning that it has no consumers.

---

## 7. `list` Command

### ❌ Issues found

**[LIST-1] No tag filtering on `list models`**
`streamt list models --tag payments` → `Error: No such option: --tag`
Tags are set on models, but there's no way to filter by tag in `list`. This is the primary
discovery mechanism users expect from dbt's `dbt ls --select tag:payments`.
*Fix*: Add `--tag` / `--select` filter to `list models`.

**[LIST-2] `list sources` shows `columns: 0` always**
Even when `columns:` is specified on a source, the list output shows `0`. Confirmed bug in
`list_cmd.py:44` — `len(s.columns)` returns count correctly but something in the pipeline
may reset it. (See SCHEMA-1 for related issue.)

**[LIST-3] Exposure `owner` shows `null` even when `owners:` list is declared**
The `Exposure` model has `owner: Optional[str]` (a single string) but the YAML convention
(matching dbt) uses `owners: [{name: ..., email: ...}]` (a list of objects). The list is
silently dropped. Every exposure shows `owner: null` in `list` output.
*Fix*: Support `owners:` list by mapping `owners[0].name` to `owner`, or add proper `owners`
field to the model.

**[LIST-4] No `list tags` command**
There's no way to discover what tags exist in the project. Users with large projects can't
find all models tagged `tier1` without reading all YAML files.

---

## 8. `show` Command

### ❌ Issues found

**[SHOW-1] Contract information not shown in `show model`**
`show model payments_clean` doesn't display contract columns, enforcement status, or
breaking-change info — even though `payments_clean` has a full contract defined.
*Impact*: The contract feature is invisible from the CLI — users can't verify their contracts
are parsed without reading the raw YAML.
*Fix*: Add a "Contract" section to `show model` output.

**[SHOW-2] SQL is truncated in text output**
`show model` truncates the SQL to ~100 chars. For debugging complex models, users need the
full SQL without switching to `--output json`.
*Fix*: Add `--full-sql` flag to `show model` or show full SQL by default.

**[SHOW-3] Test assertions not shown in `show test`**
`show test payments_not_null_ids` shows:
```
Test: payments_not_null_ids
  Model: payments_clean
  Type: continuous
  Assertions: 1
```
The `1` assertion count is not expanded. Users can't see what the assertion actually checks
without reading the YAML.

**[SHOW-4] No `show contract` command**
The contract system (P1) has no dedicated show command. `show model` ignores contracts.
To inspect a contract, users must read the raw YAML.

---

## 9. `plan` Command

### ❌ Issues found

**[PLAN-1] No offline/dry-run mode**
`streamt plan` immediately tries to connect to Flink and Kafka. There's no `--offline` or
`--dry-run` flag to see what would change without a live connection. This blocks:
- CI validation on PR merge (no Flink in CI)
- Local development without a running Flink cluster
- Code review for infrastructure changes

*Expected*: `streamt plan --offline` should diff the manifest against the last known state
(stored manifest) without requiring live connections.

**[PLAN-2] Plan failure error is raw HTTP exception**
```
ERROR: HTTPConnectionPool(host='localhost', port=8082): Max retries exceeded
```
This is a raw urllib3 error with no actionable guidance. Users don't know if this means
Flink is down, the URL is wrong, or there's a network issue.
*Fix*: `E406_CONNECTION_REFUSED` exists in error codes but the error text in JSON still
contains the raw exception. The human-readable message should say:
"Cannot connect to Flink at http://localhost:8082. Is the cluster running?"

**[PLAN-3] `--env` required but not obvious when multi-env configured**
After adding `environments/` directory, `streamt plan` without `-e` fails:
`ERROR: Multiple environments found. Specify with --env.`
But `apply --select tag:payments` also doesn't propagate the env requirement clearly.

---

## 10. `apply` Command

### ❌ Issues found

**[APPLY-1] `--select tag:X` advertised but not implemented**
`streamt apply --select "tag:payments"` prints:
```
WARNING: --select 'tag:payments' filtering is not yet implemented; deploying all models
```
This is a dangerous footgun — a user who thinks they're doing a safe partial deploy to
test one team's models actually deploys everything.
*Fix*: Either implement tag filtering or make it an ERROR (not a warning) that exits non-zero.

**[APPLY-2] `--target model_name` advertised but not implemented**
Same issue as APPLY-1: `--target payments_clean` warns and deploys everything.
This is the primary mechanism users expect for "deploy only this model and its dependencies."

**[APPLY-3] No manifest diff in apply output**
`apply` doesn't show what it's about to change. In dbt, `dbt run` shows each model as it
deploys. streamt's apply shows no per-model progress until something fails.

---

## 11. `test` Command

### ❌ Issues found

**[TEST-1] Continuous tests report `skipped` instead of `failed` when Flink unavailable**
When Flink can't connect, `payments_not_null_ids` shows `status: "skipped"` — but this
should be `failed` because the test couldn't be verified. `skipped` implies the test was
intentionally bypassed.

**[TEST-2] `kafka-python` missing error is unhelpful**
```
FAIL: sample_large_payments
  - kafka-python not installed. Run: pip install kafka-python
```
`kafka-python` is not in the streamt install — users will get this error on every fresh install.
*Fix*: Add `kafka-python` to streamt's optional dependencies or auto-detect and suggest `pip install streamt[kafka]`.

**[TEST-3] Test output doesn't distinguish test type**
The test results table doesn't show whether each test is `continuous` or `sample` — so users
don't understand why some fail at Flink while others fail at Kafka.

---

## 12. `status` Command

### ❌ Issues found

**[STATUS-1] Flink error swallows all Flink job status**
```
Flink Jobs:
  Cannot connect to Flink: HTTPConnectionPool(...)
```
When Flink is unavailable, no job statuses are shown. The command should still show
topic status (which works fine) without the Flink section blocking it.

**[STATUS-2] Status shows only managed topics, not pre-existing source topics**
`status` only checks for topics that streamt manages (model outputs). It doesn't check
whether the source topics (e.g., `payments.raw.v1`) are present and healthy.
*Impact*: A broken source topic is invisible to `status` — users discover it only when jobs fail.
*Fix*: Add a "Sources" section showing whether source topics exist and have recent activity.

**[STATUS-3] No freshness indicators in status**
Even with `freshness:` config on sources, `status` doesn't check message lag or last-message
timestamp. Users need to know if a source topic has gone stale.

---

## 13. `docs` Command

### ❌ Issues found

**[DOCS-1] Generated HTML is a single large file**
`docs/index.html` is 33KB of inline HTML. No asset directory, no search, no navigation.
Scrolling through 9 models + 4 sources + 3 tests + 3 exposures in one flat page doesn't scale.

**[DOCS-2] Contract columns not rendered in docs**
The generated docs HTML doesn't show contract columns, enforcement status, or data quality
rules — the most important governance information.

**[DOCS-3] No `--serve` flag**
No local server option: `streamt docs serve --port 8000`. Users have to open the raw HTML file.

---

## 14. JSON / Machine-Readable Output

### ❌ Issues found

**[JSON-1] Warnings go to stderr, JSON to stdout — inconsistent with some commands**
Some commands mix Rich console output with JSON. When capturing only stdout for automation,
warnings are lost. When capturing both stderr+stdout, the output can't be parsed as JSON
due to interleaved Rich output.
*Fix*: With `-o json`, all output should go to stdout as a single JSON object. Warnings should
be in the `warnings` array, never on stderr.

**[JSON-2] Error messages contain raw exception text in JSON**
```json
"message": "HTTPConnectionPool(host='localhost', port=8082): Max retries exceeded..."
```
Machine consumers have to regex-parse the message to understand the error type.
*Fix*: Include a `detail` field for the raw exception but keep `message` human-readable.

**[JSON-3] `data.errors` and top-level `errors` contain duplicate information**
The JSON envelope has both `data.errors` (validate-specific) and top-level `errors`. Same
content in two places creates confusion about which field to consume.

---

## 15. Overall UX Patterns

### ❌ Issues found

**[UX-1] Silent data loss is the #1 problem**
Multiple configs are silently ignored with no error or warning:
- `schema.fields` (SCHEMA-1)
- `environments:` in main YAML (ENV-1)
- `owners:` list in exposures (LIST-3)
- `.sql` files in models/ directory (INIT-2)
- `required:` field on columns (SCHEMA-3)

Every case of silent data loss creates a "works on my machine" mystery that's extremely
hard to debug.
*Fix*: Adopt a strict parsing policy: unknown or unsupported YAML keys should at minimum
warn, not silently discard.

**[UX-2] Advertised-but-unimplemented features erode trust**
`--upstream`, `--downstream`, `--select tag:X`, `--target model` are all documented in
`--help` output, silently degraded to warnings at runtime, and execute different behavior
than stated. This is worse than not having the feature.
*Rule*: Features in `--help` must either work or not appear in `--help`.

**[UX-3] No `streamt doctor` or preflight check command**
New users need a way to verify their environment: "Is Kafka reachable? Is Schema Registry
healthy? Is my Flink cluster accessible? Are all required dependencies installed?"
Currently they discover broken connections one command at a time.

**[UX-4] `list` requires positional arg — `streamt list` should default to summary**
`streamt list` → `Error: Missing argument`. In most CLIs, `list` without args shows everything.
*Expected*: `streamt list` shows a summary table with counts per resource type.

**[UX-5] No diff view for compiled SQL changes**
`streamt compile` regenerates everything but shows no diff from the previous compilation.
Teams doing code review want to see "what SQL changed?" not just "9 topics, 11 flink jobs".

**[UX-6] Multi-word error messages in single-line format**
Long warning messages like the `stateless SQL` warning are word-wrapped mid-sentence in the
terminal, making them hard to read and copy-paste for searching.

---

## 16. Kafka Integration

### ✅ What works well
- Topic discovery via `init --discover` correctly reads partition counts.
- Status command correctly detects MISSING topics.
- Test failures topic correctly uses a separate topic for violations.

### ❌ Issues found

**[KAFKA-1] Source topic health not checked in `status`**
See STATUS-2 above. Pre-existing topics are invisible to status.

**[KAFKA-2] `kafka-python` not a declared dependency**
Sample tests require `kafka-python` but it's not installed with `pip install streamt`.
*Fix*: Add `kafka-python` or `confluent-kafka` to optional `[kafka]` extras.

**[KAFKA-3] No Schema Registry integration for type inference**
When Schema Registry URL is configured and a source topic has an Avro/JSON schema registered,
streamt doesn't pull it for type inference. Every source defaults to `_raw STRING`.
*Fix*: On `compile`, attempt SR schema lookup for sources without `columns:` declared.

---

## Priority Matrix

| # | Finding | Severity | Impact |
|---|---------|----------|--------|
| SCHEMA-1 | `schema.fields` silently ignored | Critical | Data loss, broken DDL |
| APPLY-1/2 | `--select`/`--target` not implemented but advertised | High | Safety risk |
| VALIDATE-1 | Invalid SQL passes validation | High | Deferred failures |
| COMPILE-2 | `_raw STRING` DDL for undeclared source columns | High | Runtime failures |
| UX-2 | Advertised unimplemented features | High | Trust erosion |
| ENV-1 | `environments:` in YAML silently ignored | Medium | Confusing config |
| PLAN-1 | No offline mode for plan | Medium | CI blocker |
| LIST-1 | No tag filtering | Medium | Discovery |
| SHOW-1 | Contracts invisible in show | Medium | Contract feature unusable |
| TEST-1 | `skipped` vs `failed` for unavailable Flink | Low | Misleading |
| INIT-2 | `.sql` files silently ignored | Low | Onboarding confusion |
| DOCS-1 | Single-file HTML docs don't scale | Low | Governance |

---

## Suggested Fix Order

1. **SCHEMA-1** — Fix `schema.fields` parsing or emit error (this silently breaks every user trying to declare source schemas inline)
2. **APPLY-1/2** — Remove unimplemented `--select`/`--target` from help OR implement them
3. **VALIDATE-1** — Add SQL parse step to validate
4. **COMPILE-2** — Warn at compile time when source has no columns and is referenced in a model
5. **UX-1** — Global strict mode for unknown YAML keys
6. **PLAN-1** — Offline diff mode using stored manifest
7. **LIST-1** — Tag filtering for `list models`
8. **SHOW-1** — Contract info in `show model`
9. **ENV-1** — Error/warning when `runtime:` ignored in multi-env mode

---

*Generated from live testing on 2026-03-14 with fintech-streaming migration scenario.*
*Project: `/tmp/fintech-streaming/` | Kafka: `localhost:9092` | Schema Registry: `localhost:8081`*
