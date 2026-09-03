---
title: CLI Reference
description: Complete reference for all streamt commands
---

# CLI Reference

Complete reference for all streamt CLI commands.

## Which Command?

| Goal | Command | Needs Kafka? |
|------|---------|:------------:|
| Check YAML is valid | `validate` | No |
| Generate SQL/JSON artifacts | `compile` | No |
| Package artifacts + manifest + checksums | `build` | No |
| View data lineage | `lineage` | No |
| Export Backstage catalog entities | `docs backstage` | No |
| Export DataHub metadata-file proposals | `docs datahub` | No |
| Export OpenLineage design metadata | `docs openlineage` | No |
| Declare existing Kafka topics as external sources | `import` | **Yes** |
| See what would change on deploy | `plan` | **Yes** |
| Compare local vs deployed state | `diff` | **Yes** |
| Initialize configured PostgreSQL state | `state init` | No |
| Migrate configured PostgreSQL state to schema v2 | `state migrate-postgres-v2` | No |
| Probe instantaneous PostgreSQL lock availability | `state lock-status` | No |
| Inspect ownership/recovery metadata | `state status` | No |
| Create reviewed evidence for one unfinished operation | `state recovery-plan` | Sometimes[^recovery-observation] |
| Execute one exact reviewed recovery | `state recover` | Sometimes[^recovery-observation] |
| Claim an existing declared topic or schema subject | `adopt` | **Yes** |
| Deploy to infrastructure | `apply` | **Yes** |

[^recovery-observation]: `observed` and `rolled_back` recovery contact every
    runtime provider needed by the blocked action. `abandoned_before_mutation`
    requires empty durable progress and does not contact runtime targets. An
    exact retry of an already-completed recovery also skips provider observation
    and verifies the expected result against recovery history.

## Global Options

These options are available for all commands:

```bash
streamt [OPTIONS] COMMAND [ARGS]

Options:
  --output, -o FORMAT   Output format: text (default) or json
  --version            Show version and exit
  --help               Show help message and exit
```

!!! info "Environment Selection"
    In multi-environment mode, specify the target environment with `--env` or the `STREAMT_ENV` variable. The CLI flag takes precedence over the environment variable.

### Structured JSON Output

Use `--output json` (or `-o json`) to get machine-readable output from any command. All JSON responses follow a consistent envelope:

```json
{
  "status": "ok",
  "command": "validate",
  "data": { ... },
  "errors": [],
  "warnings": []
}
```

On error, `status` is `"error"` and `errors` contains structured entries with machine-readable codes:

```json
{
  "status": "error",
  "command": "validate",
  "data": {},
  "errors": [
    {
      "code": "E101_SOURCE_NOT_FOUND",
      "message": "Source 'orders' not found",
      "suggestion": "Available: events, payments"
    }
  ],
  "warnings": []
}
```

State-operation failures that occur after a durable intent exists also include
the canonical `operation_id`. The same identifier appears in text output so an
operator can inspect status or run recovery without guessing which attempt the
error describes. Provider credentials, endpoints, lock handles, and revision
tokens are never included.

This makes streamt suitable for LLM agents, CI/CD pipelines, and programmatic integrations.

## Commands

### init

Initialize a new streamt project.

```bash
streamt init [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Directory to initialize (default: current) |
| `--project-name NAME` | Project name (default: directory name) |
| `--force` | Overwrite existing project files |
| `--discover` | Discover sources from existing Kafka infrastructure |
| `--kafka SERVERS` | Kafka bootstrap servers (required with `--discover`) |
| `--schema-registry URL` | Schema Registry URL (extracts column definitions) |
| `--include PATTERN` | Include only topics matching glob pattern |
| `--exclude PATTERN` | Exclude topics matching glob pattern |
| `--dry-run` | Show what would be created without writing |

**Scaffold mode** (default):

```bash
# Create empty project skeleton
streamt init

# Custom name and directory
streamt init --project-dir ./my-pipeline --project-name payments
```

**Discover mode** (from existing Kafka):

```bash
# Discover topics
streamt init --discover --kafka localhost:9092

# With Schema Registry (extracts columns from Avro schemas)
streamt init --discover --kafka localhost:9092 --schema-registry http://localhost:8081

# Filter topics
streamt init --discover --kafka localhost:9092 --include "orders.*"

# Preview without writing
streamt init --discover --kafka localhost:9092 --dry-run
```

---

### import

Add existing Kafka topics to an existing project as external source declarations.
Infrastructure access is read-only: import reads topic metadata and, when configured,
Schema Registry metadata. It never creates or changes Kafka topics, registers schemas,
or writes ownership state.

```bash
streamt import [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Existing project directory (default: current) |
| `--env ENV` | Target environment in multi-environment mode |
| `--include PATTERN` | Include topics matching a glob; repeatable |
| `--exclude PATTERN` | Exclude topics matching a glob; repeatable |
| `--output-file PATH` | New declaration directly in `sources/` (default: `sources/imported.kafka.yml`) |
| `--schemas / --no-schemas` | Use configured Schema Registry enrichment (default: enabled) |
| `--dry-run` | Return the complete deterministic preview without writing |

```bash
# Preview existing topics using runtime credentials from the project
streamt import --dry-run

# Import selected domains into a new declaration file
streamt import \
  --include "orders.*" \
  --include "payments.*" \
  --exclude "*.retry" \
  --output-file sources/commerce.kafka.yml

# Import Kafka metadata without Schema Registry enrichment
streamt import --no-schemas
```

Import skips topics already declared by exact topic name. Every generated declaration
contains `ownership: {mode: external}`. The command refuses sanitized source-name
collisions, incomplete live topic metadata, and newly introduced project-validation
errors. It stages and durably installs a new file with atomic no-replace semantics;
use `--dry-run` and merge manually or select another output path when the target exists.

Schema enrichment currently checks only the conventional `{topic}-value` subject.
It reads that subject directly without requiring subject-list or compatibility-config
permissions and pins the resolved numeric version. Avro and JSON Schema can contribute
top-level columns; Protobuf contributes only its external subject/version/format
reference. A service outage stops further enrichment requests while Kafka-only import
continues. Key subjects and non-topic resources are outside this MVP.

Example generated declaration:

```yaml
sources:
  - name: orders_events
    topic: orders.events
    ownership:
      mode: external
    schema:
      registry: confluent
      subject: orders.events-value
      version: 4
      format: avro
    columns:
      - name: order_id
        type: STRING
```

With global `--output json`, resources, declarations, counts, warnings, and created
files are emitted in stable topic order for automation.

---

### validate

Validate project configuration, syntax, and governance rules.

```bash
streamt validate [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--all-envs` | Validate all environments sequentially |
| `--model, -m MODEL` | Validate only this model and its dependencies |
| `--check-schemas` | Read external subjects, versions, formats, and references from Schema Registry |
| `--strict` | Treat warnings as errors |

**Examples:**

```bash
# Basic validation
streamt validate

# Validate specific environment
streamt validate --env dev
streamt validate --env prod

# Validate all environments at once
streamt validate --all-envs

# With schema registry validation
streamt validate --check-schemas

# Strict mode (fail on warnings)
streamt validate --strict
```

Without `--check-schemas`, validation is offline. The live check never registers
or changes schemas. It decodes Avro and JSON Schema content and resolves their
version-pinned references. For Protobuf it verifies subject/version/type and the
reference graph, but does not parse messages or infer columns.

**Output:**

```
✓ Project 'payments-pipeline' is valid

  Sources:   3
  Models:    5
  Tests:     4
  Exposures: 2

Governance:
  ✓ All topics meet minimum partition requirement
  ✓ All models have descriptions
  ⚠ 1 model missing tests (warning)
```

---

### compile

Parse, validate, and generate deployment artifacts.

```bash
streamt compile [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--output-dir PATH` | Output directory (default: `generated/`) |
| `--dry-run` | Show what would be generated without writing |

**Examples:**

```bash
# Compile to default directory
streamt compile

# Compile for specific environment
streamt compile --env prod

# Custom output directory
streamt compile --output-dir ./build

# Preview without writing files
streamt compile --dry-run
```

**Output:**

```
Compiling project...

Generated artifacts:
  topics/
    orders.clean.v1.json
    orders.metrics.v1.json
  flink/
    order_metrics.sql
    order_metrics.json
  connect/
    orders_snowflake.json

Manifest written to: generated/manifest.json
```

---

### build

Compile and package artifacts with manifest and checksums.

```bash
streamt build [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--output-dir PATH` | Output directory (default: `build/`) |

**Examples:**

```bash
# Build artifacts
streamt build

# Build for specific environment
streamt build --env prod --output-dir ./dist
```

**Output:**

```
Building project...
  flink/order_metrics.sql
  topics/orders.clean.v1.json

Manifest: build/manifest.json
Checksums: build/checksums.sha256
```

The `manifest.json` contains project metadata and file listing. The `checksums.sha256` file contains SHA-256 hashes of all artifacts for verification.

---

### diff

Compare local definitions against deployed state.

```bash
streamt diff [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |

**Examples:**

```bash
# Show all diffs
streamt diff

# JSON output for CI
streamt -o json diff
```

**Output:**

```
Topics:
  ~ orders.clean.v1: partitions 6 → 12
  = orders.metrics.v1: no changes

Flink Jobs:
  + order_metrics: new
```

---

### plan

Show planned changes without applying them (like `terraform plan`).

```bash
streamt plan [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--offline` | Plan without connecting to infrastructure (assumes fresh deploy) |
| `--out PATH` | Atomically save a deterministic reviewed-plan JSON file |

**Examples:**

```bash
# Plan all changes
streamt plan

# Plan for specific environment
streamt plan --env staging

# Offline plan (no Kafka/Flink needed)
streamt plan --offline

# Save the exact reviewed actions for a later apply
streamt plan --env staging --out staging.plan.json
```

!!! tip "Offline Plan"
    Use `--offline` to preview what a fresh deployment would create, without connecting to Kafka or other infrastructure. Useful for evaluating the tool, CI validation, or reviewing changes before infrastructure is available. The offline plan assumes no existing resources — all artifacts show as "create". Offline plan files are preview-only and cannot authorize `apply`; generate a fresh online plan with live evidence before mutation.

Online planning reports ordered `safety_blockers` for Kafka partition
reductions, incompatible schemas, and Flink job updates. A blocked plan still
exits successfully and can be saved for review; its JSON data sets
`is_apply_blocked: true`. Reviewed plan format version 4 includes these blockers,
an exact ownership-state reference, and canonical ordered action evidence in its
integrity checksum. Version 1 through 3 files cannot authorize apply and are
rejected with regeneration guidance.

**Output:**

```
Plan: 2 to create, 1 to update, 0 to delete

Topics:
  + orders.clean.v1 (12 partitions, rf=3)
  ~ orders.metrics.v1 (partitions: 6 → 12)

Flink Jobs:
  + order_metrics (parallelism: 8)

Connectors:
  (no changes)
```

---

### apply

Deploy artifacts to infrastructure.

```bash
streamt apply [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--target MODEL` | Deploy only specific model |
| `--select SELECTOR` | Filter by tag or selector |
| `--plan PATH` | Verify and apply a saved reviewed-plan file |
| `--dry-run` | Show what would be deployed |
| `--confirm` | Skip confirmation prompt for protected environments |
| `--confirm-env ENV` | Non-interactive confirm: pass environment name to verify (for agents/CI) |
| `--force` | Allow destructive operations when environment policy does not already permit them |
| `--emit-openlineage` | Emit one validated finite apply-command lifecycle through an explicit transport |
| `--openlineage-job-namespace NAMESPACE` | Job namespace; falls back to `OPENLINEAGE_NAMESPACE` |
| `--openlineage-kafka-namespace KAFKA-URI` | Validated shared Kafka namespace option; apply emits no datasets |
| `--openlineage-gateway-namespace KAFKA-URI` | Validated shared Gateway namespace option; apply emits no datasets |

**Examples:**

```bash
# Deploy all
streamt apply

# Deploy to specific environment
streamt apply --env dev

# Create the required reviewed plan for a protected environment
streamt plan --env prod --out prod.plan.json

# After review, verify and apply that exact plan in CI/CD
streamt apply --env prod --plan prod.plan.json --confirm-env prod

# Override destructive policy after review (unsupported migrations remain blocked)
streamt apply --env prod --plan prod.plan.json --confirm-env prod --force

# Deploy specific model
streamt apply --target order_metrics

# Deploy by tag
streamt apply --select tag:critical

# Apply the reviewed plan; target/select cannot be added after review
streamt apply --env staging --plan staging.plan.json

# Emit the finite durable apply lifecycle through an explicit File/HTTP transport
streamt apply --env prod --plan prod.plan.json --confirm-env prod \
  --emit-openlineage \
  --openlineage-job-namespace https://lineage.example/namespaces/prod
```

A reviewed plan records the project, environment fingerprint, manifest
checksum, exact ownership-state backend/store/address/serial/checksum, resource
actions, ownership requirements, and an integrity checksum. `apply --plan`
recompiles the project, acquires the state operation lock, verifies that exact
state before runtime setup, and then replans live infrastructure before making
changes. It rejects modified plans or drift in the project, environment,
ownership state, resource actions, or ownership decisions. Offline plans record
`state: null` and cannot authorize apply.
The checksum detects accidental or unreviewed modification; it is not a digital
signature and does not establish author identity.

#### OpenLineage apply telemetry

OpenLineage emission is strictly opt-in: transport environment variables do
not enable it without `--emit-openlineage`. The namespace options take
precedence over `OPENLINEAGE_NAMESPACE`,
`STREAMT_OPENLINEAGE_KAFKA_NAMESPACE`, and
`STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE`, respectively, after project `.env`
loading. The job namespace is required and is never inferred. Explicit Kafka
and Gateway namespace values are validated for the shared CLI contract, but
apply emits no input or output datasets.

Runtime emission requires an explicit bounded File or synchronous HTTP
transport. For example, `/etc/streamt/openlineage.yml` can contain:

```yaml
# streamt:skip
transport:
  type: file
  log_file_path: /var/log/streamt/openlineage.jsonl
```

Then run:

```bash
OPENLINEAGE_CONFIG=/etc/streamt/openlineage.yml \
  streamt apply --env prod --plan prod.plan.json --confirm-env prod \
  --emit-openlineage \
  --openlineage-job-namespace https://lineage.example/namespaces/prod
```

An HTTP transport uses the same syntax documented in the
[OpenLineage integration reference](openlineage.md#runtime-transport-configuration).

The run ID is exactly the UUIDv4 persisted in the durable apply intent, and
START uses that intent's exact `started_at` time. START follows durable
`begin_operation` and precedes the first provider mutation. COMPLETE follows a
confirmed ownership-state commit and durable marker clear. Runtime failure,
unknown commit, authority loss before confirmed commit, or a recovery-required
outcome produces FAIL; interruption after START produces ABORT. A verified
post-commit authority-release failure remains COMPLETE even though the CLI
returns `E426_STATE_RELEASE_FAILED_AFTER_COMMIT` with `committed: true`.

Parse, validation, confirmation, review, safety, planning, existing-recovery,
final-drift, telemetry-preflight, and `--dry-run` exits before the durable
operation begins emit no RunEvent. After START, delivery and transport-close
failures add only `W112_OPENLINEAGE_EMIT_FAILED`; they never alter apply output,
rollback, recovery truth, exit status, or the original exception.

These events describe only the finite streamt control-plane command. They
contain no deployment actions, plan or state details, infrastructure datasets,
credentials, or runtime lifecycle claims. COMPLETE does not mean that a
submitted Flink, Gateway, Connect, or Kafka workload finished or processed
data. See [OpenLineage integration](openlineage.md) for the complete boundary.

#### Remove an exact Gateway rule

Gateway removal is an explicit reviewed workflow. Declare one exact
`lifecycle.gateway_rule_removals` tombstone using the prior compiler-level
artifact fields shown in the
[YAML reference](yaml-schema.md#explicit-gateway-rule-removals), then create an
online plan and apply that exact plan:

```bash
streamt plan --env prod --out gateway-removal.plan.json
streamt apply --env prod --plan gateway-removal.plan.json --confirm-env prod --force
```

The tombstone always requires a fresh online reviewed plan, even in an
unprotected environment. Direct `apply`, direct `apply --dry-run`, offline plan
files, and `apply` combined with `--target` or `--select` cannot authorize the
removal. An actual delete also requires `--force` unless the environment sets
`safety.allow_destructive: true`; an exact reviewed already-absent no-op does not
need that destructive override.

streamt deletes only the exact live AliasTopic and owned Interceptors bound by
the tombstone, reviewed action evidence, and ownership state. Removing a model
or omitting a rule does not request deletion, and streamt does not discover
Gateway deletion candidates by prefix or cluster-wide search. Gateway adoption
is a separate exact alias-only workflow described below.

Online JSON plan output also includes `operation_status`, containing only safe
status, operation ID/kind, stable failure code, and last safely successful
action index. Planning is read-only and does not clear an unfinished operation.
Offline output reports this status as `unavailable` without constructing a
state backend.

After a successful non-dry-run apply, streamt atomically records resources it
manages or has adopted in the configured deployment-state provider. External,
unowned, and ownership-blocked resources are never recorded. Local state uses
`.streamt/state/<environment>.json` and is appropriate for a single-user
development checkout only. PostgreSQL online plan/apply/adopt require
`writer_dsn_env`, an exact v2 writer/catalog/ACL, and a direct standalone
primary. They never fall back to the owner/admin credential, local state, or
empty state. Version 1 remains administrative only.
Failed and rolled-back planner results do not advance ownership state before
final commit.

An environment may set `safety.require_remote_state: true` to reject `apply`
and `adopt` with `E421_REMOTE_STATE_REQUIRED` while the effective provider is
local. This check runs after reviewed/offline plan-file gates but before
confirmation, compilation, state access, or runtime deployer construction.
`--force` cannot bypass it. Read-only plan and state status are not blocked.

For local apply/adopt, `.streamt/state/<environment>.control.json` records a
versioned durable intent before mutation and safe ordered action progress.
PostgreSQL records the same protocol atomically in its v2 catalog. Both
`in_progress` and `recovery_required` block later apply/adopt commands
indefinitely with `E419_STATE_RECOVERY_REQUIRED`; elapsed time is not proof that
a runtime call failed. Do not delete or edit the sidecar, and do not roll back
streamt versions while a marker exists. Use the explicit two-command reviewed
recovery workflow below for an exactly representable outcome; unsupported or
ambiguous targets remain blocked.
This sidecar complements a same-host file lock and does not provide cross-host
or distributed exclusion.

!!! warning "Protected Environments"
    Protected environments reject direct apply with
    `E418_REVIEWED_PLAN_REQUIRED`. First save and review an online plan with
    `streamt plan --env ENV --out ENV.plan.json`, then apply it with
    `streamt apply --env ENV --plan ENV.plan.json`. Confirmation is still
    required: type the environment name interactively, use `--confirm`, or use
    `--confirm-env ENV` (which also verifies the name). `--confirm`, `--force`,
    and `--dry-run` do not bypass the reviewed-plan requirement.

    An unprotected environment can opt into the same shared-workflow gate with
    `safety.require_reviewed_plan: true`. streamt never infers this policy from
    names such as `staging`, `shared`, or `prod`.

!!! danger "Unsupported migrations are not forceable"
    `apply` rejects Kafka partition reductions, incompatible Schema Registry
    updates, and every existing Flink job update with
    `E417_SAFETY_BLOCKED`. This happens before backend mutation for both direct
    apply and `apply --plan`; `--force` does not bypass it. Flink creates and
    no-op jobs remain allowed.

!!! tip "LLM/Agent Usage"
    Use `--confirm-env ENV` instead of `--confirm` for programmatic deployments. It provides an extra safety check by verifying the environment name matches, preventing accidental deployments to the wrong environment.

**Output:**

```
Applying changes...

Topics:
  + orders.clean.v1 .............. created
  ~ orders.metrics.v1 ............ updated

Flink Jobs:
  + order_metrics ................ deployed

Summary: 2 created, 1 updated, 0 unchanged
```

!!! info "Idempotency & Existing Resources"
    `apply` is idempotent — running it twice with the same project produces the same result:

    - **Topic already exists, same config** → skipped (`unchanged`)
    - **Topic already exists, different config** → updated (partitions increased, config altered)
    - **Topic doesn't exist** → created
    - **Partitions decreased** → plan records `kafka_partition_reduction`; apply
      is refused before mutation (Kafka does not support partition reduction)

    A controlled apply stops before starting any later action after one runtime
    call fails. It may attempt rollback of earlier created resources while the
    lock remains healthy, but it preserves a recovery marker because the failed
    call's result can be unknown. Do not automatically rerun apply; inspect the
    online plan's `operation_status`, retain the sidecar evidence, and use the
    explicit reviewed recovery workflow only after establishing the exact live
    outcome.

!!! warning "Flink Job Lifecycle"
    Existing Flink job updates are currently blocked because the available
    cancel-and-resubmit path is not savepoint-safe. New jobs may still be
    submitted and unchanged jobs are left alone. See [Flink Options Reference —
    Job Lifecycle on Apply](flink-options.md#job-lifecycle-on-apply) for the
    underlying lifecycle boundary.

---

### adopt

Explicitly claim one existing Kafka topic, Schema Registry subject, Kafka
Connect connector, or alias-only Conduktor Gateway rule for lifecycle
management. Adoption changes only the configured ownership state; it never
mutates the provider resource.

```bash
streamt adopt \
  --project-dir . \
  --env prod \
  --kind topic \
  --name orders \
  --confirm-resource streamt://payments/prod/topic/orders \
  --confirm-env prod
```

For a compiled schema subject, use the same logical owner and confirmation
protocol with `--kind schema`:

```bash
streamt adopt \
  --project-dir . \
  --env prod \
  --kind schema \
  --name orders \
  --confirm-resource streamt://payments/prod/schema/orders \
  --confirm-env prod
```

For a compiled Kafka Connect connector bound to the configured default Connect
cluster, use `--kind connector`:

```bash
streamt adopt \
  --project-dir . \
  --env prod \
  --kind connector \
  --name orders_sink \
  --confirm-resource streamt://payments/prod/connector/orders_sink \
  --confirm-env prod
```

For one compiled Gateway rule with no desired Interceptors, use
`--kind gateway_rule` and the model's logical ownership name:

```bash
streamt adopt \
  --project-dir . \
  --env prod \
  --kind gateway_rule \
  --name orders_view \
  --confirm-resource streamt://payments/prod/gateway_rule/orders_view \
  --confirm-env prod
```

`--name` is the stable logical declaration name, not a physical topic, subject,
connector, Gateway rule, or AliasTopic name. It must resolve to exactly one compiled artifact whose
declaration explicitly sets `ownership.mode: adopted`. Topic adoption displays
partitions, replication factor, dynamic configuration, and pending differences.
Schema adoption displays only the subject, type, version, schema ID,
compatibility, content checksums, and pending differences; schema bodies are
never printed. Connector adoption accepts an omitted cluster or an explicit
cluster equal to the effective default; an explicit non-default cluster fails
closed. The claim binds the default alias, versioned normalized-endpoint
fingerprint, and exact connector name. Review output contains checksums for the
whole configuration plus sanitized changed-key categories and directions,
never raw connector configuration or per-value fingerprints. Gateway adoption
requires the exact AliasTopic to exist at the bound endpoint and effective
vCluster with canonical physical cluster `main`. Both the desired rule and the
selected live aggregate must have zero rule-owned Interceptors. Its review
shows only the alias, binding fingerprint, mapping and aggregate fingerprints,
artifact checksum, and pending-change categories; physical topic names,
credentials, endpoint, and Interceptor configuration are omitted.

Interactive use requires typing an exact token containing both the full
resource ID and environment. Non-interactive use requires both exact
`--confirm-resource` and `--confirm-env` values; there is no generic yes/force
flag. A successful adoption atomically advances only the configured
environment-scoped ownership state. Repeating an identical adoption is a no-op
and does not advance its serial. Connector idempotency returns after one strict
resource `GET`; Gateway idempotency returns after one complete two-list
observation. Neither path asks for confirmation or writes state.

The accepted `--kind` values are `topic`, `schema`, `connector`, and
`gateway_rule`. A new
Connector adoption performs exactly one percent-encoded
`GET /connectors/<connector-name>` before confirmation and the same strict read
again after confirmation. It does not call Connect list, status, task, create,
update, delete, pause, resume, or restart APIs. A legacy unbound
`backend: kafka-connect` ownership record fails closed; it is never silently
rebound to the selected endpoint. A new Gateway adoption makes exactly two
ordered collection GETs for review and repeats them after confirmation. The
AliasTopic and Interceptor reads are sequential rather than provider-atomic,
so external writers remain a TOCTOU boundary; any changed or third-state
aggregate fails closed. Gateway POST, PUT, PATCH, and DELETE endpoints are not
used. Full Gateway Interceptor adoption and Flink adoption remain unsupported
under the gates in the [extended resource adoption
plan](../plans/2026-09-02-extended-resource-adoption.md).

!!! warning "Adoption uses configured state"
    Local adoption state is safe only for a single-user checkout. PostgreSQL v2
    adoption for every supported kind, including Connector and Gateway, uses the same exact
    writer and distributed address lock as apply, and requires
    `writer_dsn_env`; owner/admin and local fallback are forbidden.
    Run `streamt plan --out ...` after adoption and review that plan before
    applying any pending differences.

---

### test

Run data quality tests.

```bash
streamt test [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--model MODEL` | Test specific model only |
| `--type TYPE` | Filter by type: `schema`, `sample`, `continuous` |
| `--deploy` | Reserved deployment path; currently warns and runs locally without OpenLineage emission |
| `--coverage` | Show test coverage report (which models have tests) |
| `--emit-openlineage` | Emit one validated finite command-run lifecycle through an explicit transport |
| `--openlineage-job-namespace NAMESPACE` | Job namespace; falls back to `OPENLINEAGE_NAMESPACE` |
| `--openlineage-kafka-namespace KAFKA-URI` | Sample-input namespace; falls back to `STREAMT_OPENLINEAGE_KAFKA_NAMESPACE` or safe Kafka bootstrap derivation |
| `--openlineage-gateway-namespace KAFKA-URI` | Validated shared Gateway namespace option; test inputs still use Kafka because the runner consumes `runtime.kafka` |

**Examples:**

```bash
# Run all tests
streamt test

# Run tests against specific environment
streamt test --env staging

# Run schema tests only
streamt test --type schema

# Test specific model
streamt test --model orders_clean

# Emit a finite command lifecycle using an explicitly configured transport
streamt test --emit-openlineage \
  --openlineage-job-namespace https://lineage.example/namespaces/prod

# Reserved deployment path; currently warns and runs locally
streamt test --deploy
```

OpenLineage emission is strictly opt-in and requires File or HTTP transport
configuration. One non-empty invocation uses a new UUIDv4 and attempts START
plus exactly one COMPLETE, FAIL, or ABORT event. Only selected sample-test
topics appear as Kafka inputs; schema and continuous tests contribute no
dataset input. Coverage, an empty selection, and `--deploy` emit no run.
Preflight construction/configuration failures use
`E506_OPENLINEAGE_INVALID` before the runner starts. Delivery or close failures
use `W112_OPENLINEAGE_EMIT_FAILED` without changing the test output or exit
status. This lifecycle describes the finite streamt invocation, not completion
of a deployed streaming job. See [OpenLineage integration](openlineage.md).

**Output:**

```
Running tests...

Schema Tests:
  ✓ orders_schema_validation (3 assertions)

Sample Tests:
  ✓ orders_data_quality (1000 messages)

Continuous Tests:
  ○ orders_monitoring (deployed)

Results: 2 passed, 0 failed, 1 deployed
```

---

### lineage

Display data lineage and dependencies.

```bash
streamt lineage [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--model MODEL` | Focus on specific model |
| `--upstream` | Show only upstream dependencies |
| `--downstream` | Show only downstream dependencies |
| `--columns` | Show column-level lineage (requires `--model`) |
| `--format FORMAT` | Output format: `ascii`, `json`, `mermaid` |

**Examples:**

```bash
# Full lineage (ASCII)
streamt lineage

# Full lineage for specific environment
streamt lineage --env prod

# Focus on one model
streamt lineage --model order_metrics

# Upstream only
streamt lineage --model order_metrics --upstream

# Downstream only
streamt lineage --model orders_clean --downstream

# JSON output
streamt lineage --format json

# Mermaid diagram
streamt lineage --format mermaid
```

**Output (ASCII):**

```
orders_raw (source)
    └── orders_clean (topic)
            ├── order_metrics (flink)
            │       └── ops_dashboard (exposure)
            └── billing_service (exposure)
```

---

### state init

Explicitly initialize a new PostgreSQL version-1 store or register one empty
canonical address in an exact compatible version-1 or version-2 store. This is
an administrative command, not ordinary deployment-state authority.

```bash
streamt state init [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--project-dir PATH`, `-p PATH` | Project directory |
| `--env ENV`, `-e ENV` | Target environment (reads `STREAMT_ENV` if omitted) |
| `--confirm-project NAME` | Exact parsed project name |
| `--confirm-env ENV` | Exact effective environment, including `default` |
| `--confirm-address URI` | Exact `streamt-state://<namespace>/<project>/<environment>` address |

All three confirmation values are required and must match exactly. A mismatch
fails before the initializer is constructed or a database connection is
opened. For example:

```bash
streamt state init -p . -e prod \
  --confirm-project payments \
  --confirm-env prod \
  --confirm-address streamt-state://platform/payments/prod
```

When the configured schema is absent, init creates the frozen seven-table
version-1 catalog, one immutable random store ID, the requested address and its
collision-checked advisory-lock mapping, and a clear operation-control row.
When the schema already exists but is empty, the initializer identity must own
it. An exact compatible version-1 or version-2 store can register a previously
unregistered empty address. Repeating init for the same compatible, empty
address is a no-op. Init never changes an existing store's schema version.

The structured `outcome` is `initialized`, `address_registered`, or
`already_initialized`. Every successful result reports the safe store ID,
schema version, canonical address, absent ownership state, clear operation
status, and an `ordinary_state_authority` capability label. A newly initialized
v1 store reports `disabled`; address registration in an exact v2 store reports
`supported_for_v2_writer`. Init does not resolve or probe the writer credential.
It never imports local state, repairs a partial catalog, or migrates a populated
store.

Initialization is serialized by a bounded schema-scoped session advisory lock.
After acquiring it, streamt begins a fresh serializable transaction, sets the
transaction-local `search_path` to `pg_catalog`, creates and precommit-validates
the complete result, commits once, and verifies the result through a fresh
read-only connection. A failure before commit rolls the transaction back. An
ambiguous commit is never retried automatically; run `state status` or repeat
the identical confirmed init to resolve the durable result.

For a newly created schema, init revokes all schema access from `PUBLIC`; it
revokes all table access from `PUBLIC` for every table it creates. It creates no
roles and grants no privileges. The exact catalog requires one common owner for
the schema and tables. A named status role may have only non-grantable schema
`USAGE` and non-grantable table or column `SELECT`; every `PUBLIC`, mutating, or
grantable non-owner ACL is rejected. Partial catalogs, owner/ACL drift, extra
objects, populated or active target addresses, incompatible versions, and
advisory-lock-key collisions fail closed without repair.

Incompatible state uses `E411_STATE_INVALID`. Missing dependencies or
credentials and connection failures use the secret-neutral
`E420_STATE_BACKEND_UNAVAILABLE`; no local fallback occurs.

---

### state migrate-postgres-v2

Explicitly migrate one exact PostgreSQL deployment-state catalog from schema
version 1 to schema version 2 and bind its separately provisioned,
least-privilege writer role. This is an owner-only administrative operation; it
prepares the v2 catalog used by ordinary plan/apply/adopt and recovery, but does
not itself resolve or test the writer DSN.

```bash
streamt state migrate-postgres-v2 [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--project-dir PATH`, `-p PATH` | Project directory |
| `--env ENV`, `-e ENV` | Target environment (reads `STREAMT_ENV` if omitted) |
| `--confirm-store-id UUID` | Exact canonical store UUID reported by `state status` |
| `--confirm-writer-role ROLE` | Exact role value resolved through `postgres.writer_role_env` |

Both confirmations are required. The store ID must be canonical UUID text. The
role must be nonempty, NUL-free, UTF-8 encodable, and no longer than PostgreSQL's
63-byte identifier limit. Missing or malformed confirmations fail before
project parsing or provider construction. The supplied role must exactly match
the value obtained from the configured `postgres.writer_role_env`; the source
store must exactly match the supplied ID. A mismatch fails before mutation, and
no failure echoes a role, DSN, login, endpoint, or schema name.

The owner connection is resolved through `postgres.dsn_env`; the external role
name is resolved separately through `postgres.writer_role_env`. Both named
environment values are required, with no implicit credential or role fallback.
The command requires a direct standalone primary, takes the schema lock and
all registered-address locks under one bounded deadline, validates the complete
v1 catalog and histories, migrates metadata and ACLs atomically, and verifies
the postimage through a fresh connection. It never creates or alters a role.

```bash
streamt state migrate-postgres-v2 -p . -e prod \
  --confirm-store-id 8d04f3f7-0000-4000-8000-000000000000 \
  --confirm-writer-role streamt_state_writer
```

A successful human result reports the outcome, safe store ID, schema version
`2`, `Catalog mutation readiness: catalog_ready`, and `Ordinary state
authority: supported_for_v2_writer`. Structured result data contains only:

```json
{
  "backend": "postgres",
  "outcome": "migrated",
  "store_id": "8d04f3f7-0000-4000-8000-000000000000",
  "schema_version": 2,
  "ordinary_state_authority": "supported_for_v2_writer",
  "mutation_status": "catalog_ready"
}
```

`outcome` is `migrated` or, for an exact same-store/same-role retry,
`already_migrated`. `supported_for_v2_writer` is catalog capability, not proof
that `writer_dsn_env` is present, authenticates as the stored role, or reaches a
supported endpoint. Each ordinary command reproves those conditions.

Migration-specific failures are:

| Code | Operator meaning |
|------|------------------|
| `E411_STATE_INVALID` | A confirmation, role, source catalog, control/history sequence, or resulting ACL is incompatible. No repair or rebind is attempted. |
| `E420_STATE_BACKEND_UNAVAILABLE` | Configuration, optional dependency, named environment value, endpoint, credential, or connection is unavailable. |
| `E422_STATE_LOCK_TIMEOUT` | The bounded lock deadline expired. Resolve contention, recheck clear control, then retry the identical confirmed command. |
| `E425_STATE_UNKNOWN_OUTCOME` | Commit could not be classified. Do not blindly replay; preserve evidence and inspect `state status` and the durable catalog. |
| `E426_STATE_RELEASE_FAILED_AFTER_COMMIT` | The v2 postimage was verified but lock release was not. Structured data contains `committed: true`; treat the migration as committed and do not replay it as an uncommitted write. |

There is no in-place downgrade, repair, or writer-rebind command. See the
[PostgreSQL deployment-state migration guide](../guides/postgres-deployment-state.md)
for backup and preflight steps, the exact writer ACL, ambiguity handling, and
the supported topology boundary.

---

### state lock-status

Probe the instantaneous availability of the configured PostgreSQL address lock.
This separate diagnostic command does not acquire a reservation for later work
or verify ordinary PostgreSQL writer authority.

```bash
streamt state lock-status [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--project-dir PATH`, `-p PATH` | Project directory |
| `--env ENV`, `-e ENV` | Target environment (reads `STREAMT_ENV` if omitted) |

The command validates the complete version-1 or version-2 catalog and requires
a direct standalone primary endpoint. It runs in an explicit
repeatable-read, read-only transaction. For an unregistered address it returns
`unregistered` without invoking an advisory-lock function. For a registered
address it calls
`pg_try_advisory_xact_lock(bigint)` once and returns `available` or `busy`.
`available`, `busy`, and `unregistered` are all successful command outcomes.

Before returning any result, streamt requires the transaction rollback to
succeed. That rollback releases a successful transaction-scoped probe lock, so
the command neither reserves nor leaks it. The observation is instantaneous
and racy: another process may acquire the lock immediately afterward, and the
result cannot authorize mutation. Full catalog validation reads the
operation-control rows, but the command does not report, clear, or interpret
durable operation control as mutation safety; use `streamt state status` to
view it.

Structured output contains only:

```json
{
  "backend": "postgres",
  "store_id": "8d04f3f7-...",
  "address": "streamt-state://platform/payments/prod",
  "lock_status": "available",
  "reservation": "none",
  "ordinary_state_authority": "not_verified"
}
```

PostgreSQL advisory locks are physical-session state. Use a direct connection
to one standalone primary. Every pooler/proxy and every HA or failover topology
is unsupported; streamt cannot reliably detect that a DSN bypasses all poolers.
The operation lock retains one connection for its complete lifetime. The probe creates no
roles, grants, address rows, or operation markers.

An invalid catalog uses `E411_STATE_INVALID`. A replica, missing dependency or
credential, connection failure, or unsuccessful rollback uses secret-neutral
`E420_STATE_BACKEND_UNAVAILABLE`. No local fallback occurs.

```bash
streamt state lock-status -p . -e prod
streamt -o json state lock-status -p . -e prod
```

---

### state status

Inspect safe configured ownership-state and operation-control metadata without
acquiring a mutation lock or connecting to runtime infrastructure.

```bash
streamt state status [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |

For the working local provider, structured output contains its kind, immutable store ID,
canonical state address, ownership presence/serial/checksum, and the same safe
`operation_status` fields exposed by online plan. Provider revisions, resource
contents, credentials, and raw provider errors are not emitted. A missing state
file is reported as `state_status: absent` with serial `0`; the command does not
create `.streamt/`, ownership state, a control sidecar, or lock files.

Both `in_progress` and `recovery_required` are shown without modification. The
text output directs operators to retain the sidecar evidence because those
markers still block apply/adopt indefinitely. `state status` cannot clear or
recover them.

For `backend: postgres`, the optional `postgres` package extra enables a
separate administrative reader. It verifies the exact version-1 or version-2
store catalog, then reports safe store, address, ownership, and
operation-control fields from
one bounded, repeatable-read, read-only snapshot. It never returns the DSN,
endpoint, SQL, raw driver errors, or ownership payload. An absent schema is
`uninitialized`; an initialized store can report an unregistered, absent, or
present address. Missing dependencies or credentials, incompatible stores, and
connection failures use sanitized state errors and never fall back to local.
Catalog verification also requires common schema/table ownership and the exact
version-specific ACL contract. For v2, status reports `mutation_status:
catalog_ready` and `ordinary_state_authority: supported_for_v2_writer`; v1
reports `disabled`. This is a catalog capability label only: status uses
`dsn_env` and does not resolve, authenticate, or test `writer_dsn_env`. The
read-only transaction sets
`search_path` to `pg_catalog` and uses schema-qualified state objects.

```bash
streamt state status -p . -e prod
streamt -o json state status -p . -e prod
```

---

### state recovery-plan

Create integrity-checked evidence for one exact unfinished operation without
clearing its marker or changing ownership state.

```bash
streamt state recovery-plan [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--resolution OUTCOME` | Required exact outcome: `observed`, `rolled_back`, or `abandoned_before_mutation` |
| `--out FILE` | Required new recovery-evidence file; existing paths and symlinks are refused |
| `--project-dir PATH`, `-p PATH` | Project directory |
| `--env ENV`, `-e ENV` | Target environment (reads `STREAMT_ENV` if omitted) |

The command acquires the configured state lock and rereads the complete state,
control marker, durable progress, and ordered action intent. `observed` and
`rolled_back` also recompile the current project and freshly observe every
target. `abandoned_before_mutation` is accepted only when durable progress is
empty and does not construct runtime deployers. A target that cannot be
represented exactly fails closed. Present Flink jobs remain unsupported.

A version-2 Gateway mutation intent contains secret-neutral evidence for the
exact current and desired managed aggregate: its bound endpoint/vCluster
identity, provider rule name, alias, aggregate fingerprint, and owned-interceptor
count. Recovery compares one fresh full alias-plus-owned-interceptor aggregate
with those two surfaces. A current match is classified as prior and can prove a
rolled-back create, update, or delete; a desired match is classified as the
candidate and can prove a completed create, update, or delete. Any partial or
intermediate aggregate fails closed. Gateway performs the shared observation as
a bounded pair of sequential AliasTopic and Interceptor list GETs. The pair is
read-only but not provider-atomic, so freeze concurrent Gateway changes while
creating and first executing the plan.

Legacy control-version-1 Gateway actions have no exact aggregate evidence and
fail before provider access for live `observed` or `rolled_back` recovery. They
may use `abandoned_before_mutation` only when durable progress is empty.

The evidence file is created atomically as a regular file with mode `0600` and
is never overwritten. It binds the configured store/address, blocked and new
recovery operation IDs, prior state and control preimage, current project
fingerprints and normalized target evidence when required, selected resolution,
candidate ownership state when applicable, and an `evidence_checksum`. It
contains no DSN, provider revision token, or raw provider error. The checksum
detects modification; it is not an approval signature.

```bash
streamt state recovery-plan -p . -e prod \
  --resolution observed \
  --out /secure/recovery/payments-prod-observed.json
```

Successful structured result data contains:

```json
{
  "plan_file": "/secure/recovery/payments-prod-observed.json",
  "blocked_operation_id": "00000000-0000-4000-8000-000000000000",
  "recovery_operation_id": "00000000-0000-4000-8000-000000000001",
  "resolution": "observed",
  "evidence_checksum": "sha256:..."
}
```

Creating evidence is read-only with respect to deployment state. Review its
exact operation, actions, live classifications, candidate state, project
fingerprints, and checksum before execution. Do not edit the file; after any
drift, generate and review a new file at a new path.

---

### state recover

Execute one exact reviewed recovery after revalidating its state, project, and
live evidence under the configured lock.

```bash
streamt state recover [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--plan FILE` | Required reviewed recovery-evidence file |
| `--confirm-operation-id UUID` | Exact blocked operation UUID from that file |
| `--confirm-resolution OUTCOME` | Exact `observed`, `rolled_back`, or `abandoned_before_mutation` value from that file |
| `--confirm-evidence-checksum CHECKSUM` | Exact lowercase `sha256:` checksum from that file |
| `--project-dir PATH`, `-p PATH` | Project directory |
| `--env ENV`, `-e ENV` | Target environment (reads `STREAMT_ENV` if omitted) |

All three confirmations are required. Malformed confirmations fail before the
plan is read or a provider is constructed. The command then strictly validates
the plan and checksum, configured store/address, current state/control
preimage, and, when required, project fingerprints and fresh live evidence.
Any mismatch stops without partially accepting a plan. If the exact reviewed
state is already present and operation control is already clear, an identical
completed retry skips project and provider re-observation; the state backend
verifies the recovery operation and result against its durable recovery
history.

```bash
streamt state recover -p . -e prod \
  --plan /secure/recovery/payments-prod-observed.json \
  --confirm-operation-id 00000000-0000-4000-8000-000000000000 \
  --confirm-resolution observed \
  --confirm-evidence-checksum sha256:0000000000000000000000000000000000000000000000000000000000000000
```

Successful structured result data contains:

```json
{
  "store": {
    "backend": "postgres",
    "store_id": "8d04f3f7-0000-4000-8000-000000000000"
  },
  "address": "streamt-state://platform/payments/prod",
  "state_serial": 13,
  "state_checksum": "sha256:...",
  "control_status": "clear",
  "state_changed": true,
  "blocked_operation_id": "00000000-0000-4000-8000-000000000000",
  "recovery_operation_id": "00000000-0000-4000-8000-000000000001",
  "resolution": "observed",
  "evidence_checksum": "sha256:..."
}
```

`observed` may preserve an exact reviewed mix of targets at their prior and
candidate states. `rolled_back` requires every target to match prior state.
`abandoned_before_mutation` retains prior state and requires no started action.
Recovery never repeats a runtime mutation, lowers the state serial, edits
history in place, force-unlocks, or runs automatically.

For Gateway specifically, only a desired-absent match for an exact durable
`delete` action may remove its ownership record. Manifest absence never creates
or implies that action. Ordinary planning currently has no broad discovery of
removed Gateway rules, so deleting a rule declaration does not itself schedule
a provider delete. Alias-only `adopt` recovery requires the exact reviewed
current aggregate; `observed` records the adopted ownership candidate and
`rolled_back` retains the absent prior claim.

Local recovery uses the local state authority and a crash-safe history
sequence. PostgreSQL recovery requires an exact v2 catalog and resolves only
the separately configured `postgres.writer_dsn_env`; it never falls back to
the administrative `postgres.dsn_env`. It proves the stored writer identity,
exact catalog/ACL, and a direct standalone primary, then commits state,
history, and control atomically. Ordinary plan/apply/adopt use the same writer
authority. All poolers and HA/failover topologies are unsupported.

Recovery failures use these stable codes:

| Code | Operator meaning |
|------|------------------|
| `E408_PLAN_FILE_INVALID` | The plan or a confirmation is malformed, modified, incomplete, unsupported, unsafe, or mismatched. Do not repair the plan in place. |
| `E409_PLAN_STALE` | Current project inputs or fresh live observations no longer match the reviewed evidence. Create and review a new plan. |
| `E411_STATE_INVALID` | State, control, history, catalog, or address is incompatible. Do not hand-edit state metadata. |
| `E419_STATE_RECOVERY_REQUIRED` | Recovery planning found no active unfinished operation, or the selected abandonment outcome is forbidden after durable progress. Inspect `state status` and preserve the marker. |
| `E420_STATE_BACKEND_UNAVAILABLE` | A dependency, named credential, endpoint, runtime provider, or exact PostgreSQL writer authority is unavailable; no fallback occurs. |
| `E422_STATE_LOCK_TIMEOUT` | The bounded lock deadline expired. Resolve legitimate contention and retry; never force-unlock. |
| `E423_STATE_LOCK_LOST` | State authority was lost. Start no mutation and inspect `state status`. |
| `E424_STATE_CONFLICT` | State or operation control changed after observation. Create fresh reviewed evidence for the remaining blocker. |
| `E425_STATE_UNKNOWN_OUTCOME` | Recovery may have committed. Inspect status and use only the identical file and confirmations for idempotent verification. |
| `E426_STATE_RELEASE_FAILED_AFTER_COMMIT` | The recovery commit was verified but authority release failed. Structured data reports `committed: true`; treat the recovery as committed. |

See the [deployment-state recovery runbook](../guides/state-recovery.md) for
resolution criteria, independent review, backups, supported target boundaries,
failure handling, and PostgreSQL topology requirements.

---

### status

Check deployment status of resources.

```bash
streamt status [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--lag` | Show consumer lag and message counts for topics |
| `--consumer-groups` | Show per-consumer-group lag |
| `--health` | Exit 1 if any resource is MISSING or DRIFT (for CI/monitoring) |
| `--format FORMAT` | Output format: `text` (default) or `json` |
| `--filter PATTERN` | Filter resources by name pattern (glob-style) |

**Examples:**

```bash
# Full status
streamt status

# Status for specific environment
streamt status --env prod

# With consumer lag info
streamt status --lag

# JSON output (for scripting)
streamt status --format json

# Filter by name pattern
streamt status --filter "payments*"

# Combine options
streamt status --lag --filter "orders*"

# Health check (exit 1 if anything unhealthy)
streamt status --health
```

**Output (text):**

```
Topics:
  OK orders.clean.v1 (partitions: 12, rf: 3) ~15420 msgs
  OK orders.metrics.v1 (partitions: 6, rf: 3) ~8210 msgs

Flink Jobs:
  RUNNING order_metrics

Connectors:
  RUNNING orders_snowflake

Summary: Topics: 2 OK, 0 missing | Jobs: 1 running, 0 other
```

**Output (JSON):**

```json
{
  "project": "payments-pipeline",
  "topics": [
    {
      "name": "orders.clean.v1",
      "exists": true,
      "partitions": 12,
      "replication_factor": 3,
      "message_count": 15420
    }
  ],
  "flink_jobs": [
    {
      "name": "order_metrics",
      "exists": true,
      "job_id": "abc123",
      "status": "RUNNING"
    }
  ],
  "connectors": [],
  "schemas": []
}
```

---

### list

List project resources by type.

```bash
streamt list RESOURCE_TYPE [OPTIONS]
```

**Arguments:**

| Argument | Values |
|----------|--------|
| `RESOURCE_TYPE` | `sources`, `models`, `tests`, `exposures` |

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--select SELECTOR` | Filter by tag or selector expression |
| `--sort-by FIELD` | Sort results by field: `name` (default), `type`, `upstream` |

**Examples:**

```bash
# List all sources
streamt list sources

# List models with JSON output
streamt -o json list models

# List tests for a specific environment
streamt list tests --env prod
```

**JSON Output:**

```json
{
  "status": "ok",
  "command": "list",
  "data": {
    "resource_type": "models",
    "count": 3,
    "items": [
      {"name": "orders_clean", "materialized": "virtual_topic", "upstream": ["orders_raw"]},
      {"name": "order_metrics", "materialized": "flink", "upstream": ["orders_clean"]}
    ]
  }
}
```

---

### show

Show detailed information about a single resource.

```bash
streamt show RESOURCE_TYPE NAME [OPTIONS]
```

**Arguments:**

| Argument | Values |
|----------|--------|
| `RESOURCE_TYPE` | `source`, `model`, `test`, `exposure` |
| `NAME` | Resource name |

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--diff` | Show diff between declared and deployed state |

**Examples:**

```bash
# Show source details
streamt show source orders_raw

# Show model with JSON output (for agents)
streamt -o json show model order_metrics

# Show test details
streamt show test orders_quality
```

**JSON Output (model):**

```json
{
  "status": "ok",
  "command": "show",
  "data": {
    "resource_type": "model",
    "name": "order_metrics",
    "materialized": "flink",
    "upstream": ["orders_clean"],
    "downstream": ["ops_dashboard"],
    "sql": "SELECT ...",
    "flink": {"parallelism": 4, "state_ttl_ms": 3600000},
    "topic": {"partitions": 12}
  }
}
```

---

### observe

Show live runtime health: consumer lag, Flink job status, backpressure. Connects to Kafka (for consumer group lag) and Flink (for job metrics). Does not modify any infrastructure.

```bash
streamt observe [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--model MODEL` | Observe a single model by name |

**Examples:**

```bash
# Full runtime health
streamt observe

# Observe specific model
streamt observe --model payments_clean

# JSON output
streamt -o json observe
```

---

### docs

Generate project documentation.

```bash
streamt docs COMMAND [OPTIONS]
```

#### docs generate

Generate HTML documentation site.

```bash
streamt docs generate [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--output PATH` | Output directory (default: `site/`) |

**Examples:**

```bash
# Generate docs
streamt docs generate

# Generate docs for specific environment
streamt docs generate --env prod

# Custom output
streamt docs generate --output ./public
```

#### docs asyncapi

Generate a deterministic, offline-validated AsyncAPI 3.1 JSON document for
declared Kafka source and model channels.

```bash
streamt docs asyncapi [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |

**Examples:**

```bash
# Generate an AsyncAPI 3.1 document
streamt docs asyncapi

# Save to file
streamt docs asyncapi > asyncapi.json

# Return the document in the structured CLI envelope at data.document
streamt --output json docs asyncapi
```

The export includes stable channel, message, schema, and operation identifiers,
payload schemas from declared columns/contracts, and only the Kafka binding
fields supported by explicit model topic metadata. It does not invent broker
servers, credentials, serialization settings, or payload schemas for resources
without declared columns. Unsupported or malformed Flink types and identifier
collisions fail closed. See [AsyncAPI export](asyncapi.md) for the exact
validation and representation boundary.

#### docs odcs

Generate one deterministic, offline-validated Open Data Contract Standard
(ODCS) 3.1.0 document for one parsed streamt project.

```bash
streamt docs odcs [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--contract-id ID` | Explicit contract identity; semantically required and copied exactly |
| `--status STATUS` | Explicit lifecycle status; semantically required and copied exactly |
| `--contract-version VERSION` | Contract version; defaults to the exact `project.version` |
| `--format yaml\|json` | Raw-document serialization (default: `yaml`) |
| `--output-file PATH` | Atomically replace this file instead of writing the raw document to stdout |
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |

**Examples:**

```bash
# YAML is the default raw format
streamt docs odcs \
  --contract-id urn:acme:data-contract:payments \
  --status active > payments.odcs.yaml

# Write validated JSON through an atomic file replacement
streamt docs odcs \
  --contract-id urn:acme:data-contract:payments \
  --status active \
  --contract-version 2.3.0 \
  --format json \
  --output-file payments.odcs.json

# Keep the document as an object in the standard streamt JSON envelope
streamt --output json docs odcs \
  --contract-id urn:acme:data-contract:payments \
  --status active \
  --format yaml
```

Without `--output-file`, text mode reserves stdout for exactly one raw YAML or
JSON document. Parser notices and incomplete-schema warnings go to stderr. The
global `--output json` option instead returns one normal streamt envelope with
the document under `data.document`; local `--format` remains serialization
metadata and does not replace the envelope. With `--output-file`, serialization
and validation complete before streamt stages and atomically replaces the
explicit target.

Contract ID and status never receive defaults. A version must come from
`--contract-version` or non-blank `project.version`. Invalid contract metadata,
mapping, validation, serialization, or output uses `E505_ODCS_INVALID`.

The document contains every declared source and model once. It maps only
declared schema facts and does not export data quality, SLAs, teams, roles,
servers, runtime endpoints, credentials, SQL, catalog publication state, or a
separate contract per model. See [ODCS export](odcs.md) for the exact mapping,
validation, and omission boundary.

#### docs backstage

Export deterministic, offline-validated Backstage Software Catalog core
entities as canonical multi-document YAML.

```bash
streamt docs backstage [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--catalog-id ID` | Stable catalog identity; semantically required |
| `--catalog-namespace NAMESPACE` | Explicit lowercase namespace for generated entities; semantically required |
| `--default-owner-ref REF` | Full Group or User reference; semantically required |
| `--lifecycle VALUE` | Exact Component lifecycle; semantically required |
| `--owner-map PATH` | Strict version-1 JSON mapping from declared streamt owner labels to full refs |
| `--kafka-cluster-ref REF` | Full Resource ref; required when Kafka Resources are emitted |
| `--gateway-cluster-ref REF` | Full Resource ref; required when Gateway virtual-topic Resources are emitted |
| `--domain-ref REF` | Optional full Domain ref for the generated System |
| `--output-file PATH` | Atomically replace this file with canonical YAML |
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment |

```bash
streamt docs backstage \
  --catalog-id payments-prod \
  --catalog-namespace payments \
  --default-owner-ref group:platform/payments \
  --lifecycle production \
  --kafka-cluster-ref resource:platform/kafka-prod \
  --output-file catalog-info.yaml
```

Text mode writes raw YAML to stdout when no file is selected; warnings remain
on stderr. Global `--output json` returns the validated entities, exact kind
counts, release metadata, and output path in the standard envelope. Sink
destinations and exposures are intentionally omitted with
`W113_BACKSTAGE_SINK_OUTPUT_OMITTED` and
`W114_BACKSTAGE_EXPOSURE_OMITTED`. Invalid catalog inputs or mapping use
`E507_BACKSTAGE_INVALID`.

The command performs one dry-run compile and does not read deployment state,
contact providers, or publish to Backstage, DataHub, or Conduktor Console. See
[Backstage Software Catalog export](backstage-catalog.md) for the exact entity,
ownership, output, and sensitivity boundaries.

#### docs datahub

Export deterministic, offline-validated DataHub v1.7.0 simplified Metadata
Change Proposals as canonical JSON.

```bash
streamt docs datahub [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--catalog-id ID` | Exact caller-owned DataFlow ID; semantically required |
| `--fabric FABRIC` | Exact uppercase DataHub v1.7.0 FabricType; semantically required |
| `--kafka-platform-instance ID` | Optional Kafka platform instance matching DataHub Kafka ingestion identity |
| `--gateway-platform-id ID` | Explicit bare DataHub platform ID; required with the instance when Gateway datasets exist |
| `--gateway-platform-instance ID` | Explicit Gateway platform instance; required with the platform ID when Gateway datasets exist |
| `--output-file PATH` | Atomically replace this file with canonical MCP JSON |
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target streamt environment; it does not imply the DataHub fabric |

Supported fabrics are exactly `DEV`, `TEST`, `QA`, `UAT`, `EI`, `PRE`, `STG`,
`NON_PROD`, `PROD`, `CORP`, `RVW`, `PRD`, `TST`, `SIT`, `SBX`, `SANDBOX`, and
`CERT`.

```bash
# Write canonical simplified MCP JSON to stdout
streamt docs datahub \
  --catalog-id payments-prod \
  --fabric PROD \
  --kafka-platform-instance kafka-main

# Supply an explicit platform and instance for Gateway virtual topics
streamt docs datahub \
  --catalog-id payments-prod \
  --fabric PROD \
  --kafka-platform-instance kafka-main \
  --gateway-platform-id conduktor-gateway \
  --gateway-platform-instance gateway-prod \
  --output-file datahub-mcps.json

# Return proposals, entity/aspect counts, and warnings in the normal envelope
streamt --output json docs datahub \
  --catalog-id payments-prod \
  --fabric PROD
```

One project becomes one `dataFlow`; sources and topic outputs become native
DataHub `dataset` URNs; actual Flink, Gateway, and Connect processes become
`dataJob` entities with exact direct Dataset lineage. Process-free topics do
not invent jobs, and Connect sinks do not invent destination datasets. A model
contract contributes only `streamt.contract.status` with `declared` or
`enforced`; this is not an ODCS document, native DataHub DataContract, schema,
or assertion.

Text mode without a file reserves stdout for the canonical JSON array and puts
warnings on stderr. With `--output-file`, text stdout is empty. Global JSON
mode returns `standard`, pinned release and API metadata, proposals, exact
entity/aspect counts, and `output_file` under `data`; warnings use the normal
top-level array. `--quiet` suppresses raw output and warning text.

The bounded omission warnings are:

| Code | Meaning |
|------|---------|
| `W115_DATAHUB_SINK_OUTPUT_OMITTED` | A sink DataJob is emitted without an invented destination Dataset |
| `W116_DATAHUB_EXPOSURE_OMITTED` | One exposure occurrence is omitted |
| `W117_DATAHUB_TAGS_OMITTED` | One declaration's tag mapping is deferred |
| `W118_DATAHUB_OWNER_OMITTED` | One declaration's ownership mapping is deferred |

Invalid identity, mapping, validation, serialization, or file output fails
closed with `E508_DATAHUB_INVALID` and no partial artifact.

This command is an offline metadata-file export. It constructs no DataHub,
Kafka, Gateway, deployment-state, or subprocess client and has no runtime
`acryl-datahub` dependency. Release gates validate the bytes with the exact
DataHub 1.7.0 SDK, wrapper, and metadata-file reader, but streamt does not call
GMS, ingest the file, publish or reconcile entities, verify platform existence,
or claim live-server lineage behavior. See [DataHub catalog export](datahub-catalog.md)
for identity, mapping, warning, and sensitivity details.

#### docs openlineage

Export deterministic, offline-validated OpenLineage 1.53.0 design metadata as
canonical JSON Lines.

```bash
streamt docs openlineage [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--job-namespace NAMESPACE` | Job namespace; falls back to `OPENLINEAGE_NAMESPACE` and is semantically required |
| `--kafka-namespace KAFKA-URI` | Kafka dataset namespace; falls back to `STREAMT_OPENLINEAGE_KAFKA_NAMESPACE` or safe single-bootstrap derivation |
| `--gateway-namespace KAFKA-URI` | Gateway dataset namespace; falls back to `STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE` or safe single-bootstrap derivation |
| `--output-file PATH` | Atomically replace this file instead of writing JSONL to stdout |
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |

**Examples:**

```bash
# Write validated JSONL to stdout
streamt docs openlineage \
  --job-namespace https://lineage.example/namespaces/prod

# Use explicit catalog identities and atomic file output
streamt docs openlineage \
  --job-namespace https://lineage.example/namespaces/prod \
  --kafka-namespace kafka://catalog-kafka.example:9092 \
  --gateway-namespace kafka://catalog-gateway.example:6969 \
  --output-file lineage.jsonl

# Retain events and warnings in the normal structured envelope
streamt --output json docs openlineage \
  --job-namespace https://lineage.example/namespaces/prod
```

Text-mode stdout contains only compact canonical JSONL when no output file is
selected; warnings go to stderr. In global JSON mode, events and counts are
under `data`, while warnings use the normal top-level `warnings` array. All
events are generated, validated against bundled official schemas, and
serialized before event output begins. Failures use
`E506_OPENLINEAGE_INVALID` and never emit a partial event stream.

This command exports static `DatasetEvent` and `JobEvent` records from one
dry-run compile. It performs no live-service or deployment-state reads and does
not emit `RunEvent` records. Separately, explicitly enabled `apply
--emit-openlineage` and `test --emit-openlineage` support finite command-run
pairs through an explicit File or HTTP transport. Ordinary compile, apply
without the flag, and deployed Flink, Gateway, or Connect processes do not emit
OpenLineage telemetry. See [OpenLineage integration](openlineage.md) for exact
mapping, transport, namespace, validation, and security boundaries.

#### docs openapi

Deprecated compatibility alias for `streamt docs asyncapi`. It emits the exact
same AsyncAPI 3.1 document; despite the historical command name, it does not
emit an OpenAPI document. New automation should use `docs asyncapi`.

```bash
streamt docs openapi [OPTIONS]
```

#### docs dictionary

Export data dictionary (all sources and models with columns).

```bash
streamt docs dictionary [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |
| `--env ENV` | Target environment (multi-env mode) |
| `--format FORMAT` | Output format: `csv` (default) or `json` |

**Examples:**

```bash
# Export CSV data dictionary
streamt docs dictionary

# Export as JSON
streamt docs dictionary --format json

# Save to file
streamt docs dictionary > data-dictionary.csv
```

---

### envs

Manage and inspect environments (multi-env mode only).

```bash
streamt envs COMMAND [OPTIONS]
```

#### envs list

List all available environments.

```bash
streamt envs list [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |

**Examples:**

```bash
streamt envs list
```

**Output:**

```
dev          Local development environment
staging      Staging environment
prod         Production environment [protected]
```

#### envs show

Show resolved configuration for an environment (secrets masked).

```bash
streamt envs show ENV [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project-dir PATH` | Project directory |

**Examples:**

```bash
streamt envs show prod
```

**Output:**

```yaml
environment:
  name: prod
  description: Production environment
  protected: true
runtime:
  kafka:
    bootstrap_servers: prod-kafka.example.com:9092
  schema_registry:
    url: https://prod-sr.example.com
    username: admin
    password: '****'
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `STREAMT_ENV` | Default target environment (overridden by `--env` flag) |
| `STREAMT_PROJECT_DIR` | Default project directory |
| `STREAMT_LOG_LEVEL` | Log level (DEBUG, INFO, WARNING, ERROR) |
| `STREAMT_NO_COLOR` | Disable colored output |

### .env File Loading

In multi-environment mode, environment variables are loaded with precedence:

1. `.env` — Base variables (always loaded)
2. `.env.{environment}` — Environment-specific overrides (e.g., `.env.prod`)
3. Actual environment variables — Highest priority

See [Multi-Environment Support](../guides/multi-environment.md) for details.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (validation, deployment, or runtime) |

### Structured Error Codes

When using `--output json`, errors include machine-readable codes. These codes follow a taxonomy for programmatic handling.

**Validation Errors (E1xx):**

| Code | Meaning |
|------|---------|
| `E101_SOURCE_NOT_FOUND` | Referenced source does not exist |
| `E102_MODEL_NOT_FOUND` | Referenced model does not exist |
| `E103_DUPLICATE_NAME` | Duplicate resource name |
| `E104_CYCLE_DETECTED` | Circular dependency in DAG |
| `E105_JINJA_SYNTAX_ERROR` | Jinja template syntax error in SQL |
| `E106_ACCESS_DENIED` | Access denied by governance rules |
| `E107_TEST_MODEL_NOT_FOUND` | Test references nonexistent model |
| `E108_EXPOSURE_MODEL_NOT_FOUND` | Exposure references nonexistent model |
| `E109_EXPOSURE_SOURCE_NOT_FOUND` | Exposure references nonexistent source |
| `E110_EXPOSURE_DEPENDENCY_NOT_FOUND` | Exposure dependency not found |
| `E111_NAME_COLLISION` | Name collision between resources |

**Configuration Errors (E2xx):**

| Code | Meaning |
|------|---------|
| `E201_GATEWAY_REQUIRED` | Gateway config required for virtual topics |
| `E202_FLINK_REQUIRED` | Flink config required for Flink materializations |
| `E203_CONFLUENT_FLINK_REQUIRED` | Confluent Cloud Flink required (e.g., for `ML_PREDICT`) |
| `E204_MISSING_SINK_CONFIG` | Sink model missing connector configuration |
| `E205_CONNECT_REQUIRED` | Connect cluster config required for sink connectors |
| `E206_SQL_GATEWAY_NOT_CONFIGURED` | Flink SQL Gateway URL not configured |
| `E207_CONTINUOUS_TEST_WITHOUT_FLINK` | Continuous test requires Flink cluster |
| `E208_MISSING_CONFIG` | Required configuration is missing |

**Schema Errors (E3xx):**

| Code | Meaning |
|------|---------|
| `E301_INVALID_STATE_TTL` | Invalid state TTL configuration |

**Deployment Errors (E4xx):**

| Code | Meaning |
|------|---------|
| `E401_CANNOT_REDUCE_PARTITIONS` | Cannot reduce topic partition count |
| `E402_SCHEMA_INCOMPATIBLE` | Schema incompatible with registry |
| `E403_FLINK_SQL_ERROR` | Flink SQL execution error |
| `E404_AUTH_FAILED` | Authentication failed |
| `E405_SSL_ERROR` | SSL/TLS connection error |
| `E406_CONNECTION_REFUSED` | Connection refused by service |
| `E407_DEPLOY_ERROR` | General deployment error |
| `E408_PLAN_FILE_INVALID` | A reviewed deployment/recovery plan or required recovery confirmation is malformed, mismatched, or fails integrity validation |
| `E409_PLAN_STALE` | Project, environment, ownership state, or live action/evidence drifted after review |
| `E410_OWNERSHIP_REQUIRED` | A live resource needs an explicit ownership decision or adoption |
| `E411_STATE_INVALID` | Ownership state is malformed or belongs to another context |
| `E412_ADOPTION_TARGET_INVALID` | Adoption target is missing, ambiguous, or not explicitly declared adopted |
| `E413_ADOPTION_LIVE_NOT_FOUND` | Declared physical topic does not exist in Kafka |
| `E414_ADOPTION_CONFIRMATION_REQUIRED` | Exact resource and environment confirmation is absent or incorrect |
| `E415_ADOPTION_STATE_CONFLICT` | Existing ownership state conflicts with the requested claim |
| `E416_ADOPTION_FAILED` | Live observation or atomic adoption-state persistence failed |
| `E417_SAFETY_BLOCKED` | Apply refused an unsupported partition, schema, or Flink migration before mutation |
| `E418_REVIEWED_PLAN_REQUIRED` | Direct apply is disabled by protected/shared environment policy; create and apply a reviewed plan file |
| `E419_STATE_RECOVERY_REQUIRED` | An unfinished operation marker blocks mutation pending explicit recovery |
| `E420_STATE_BACKEND_UNAVAILABLE` | The configured deployment-state provider cannot be used; no fallback occurs |
| `E421_REMOTE_STATE_REQUIRED` | Environment policy rejects apply/adopt while local deployment state is selected |
| `E422_STATE_LOCK_TIMEOUT` | The bounded wait for deployment-state operation authority expired |
| `E423_STATE_LOCK_LOST` | The operation no longer owns its deployment-state lock; inspect the reported operation ID before continuing |
| `E424_STATE_CONFLICT` | The observed deployment state or operation control changed; re-observe or re-plan |
| `E425_STATE_UNKNOWN_OUTCOME` | A state transition may have committed; inspect status and never blindly replay it (recovery permits only exact idempotent verification) |
| `E426_STATE_RELEASE_FAILED_AFTER_COMMIT` | The commit is verified but authority release is not; `data.committed` is `true` and the commit must not be replayed |

**Parse Errors (E5xx):**

| Code | Meaning |
|------|---------|
| `E501_PARSE_ERROR` | YAML/SQL parsing error |
| `E502_ENV_VAR_ERROR` | Environment variable not set |
| `E503_ENVIRONMENT_ERROR` | Environment configuration error |
| `E504_ASYNCAPI_INVALID` | AsyncAPI generation or validation failed |
| `E505_ODCS_INVALID` | ODCS metadata, mapping, validation, serialization, or output failed |
| `E506_OPENLINEAGE_INVALID` | OpenLineage namespace, mapping, validation, serialization, or output failed |

**Governance Errors (E6xx):**

| Code | Meaning |
|------|---------|
| `E601_NAMING_VIOLATION` | Resource name violates naming convention |

**Import Errors (E7xx):**

| Code | Meaning |
|------|---------|
| `E701_IMPORT_DISCOVERY_FAILED` | Kafka discovery failed before any declaration was written |
| `E702_IMPORT_TARGET_EXISTS` | Import target already exists and was not changed |
| `E703_IMPORT_NAME_COLLISION` | Generated name collides with another generated source or an existing source/model |
| `E704_IMPORT_PATH_INVALID` | Output is not a direct YAML declaration under `sources/` |
| `E705_IMPORT_VALIDATION_FAILED` | Generated declaration failed strict validation |
| `E706_IMPORT_WRITE_FAILED` | Exclusive declaration creation failed |

**Warnings (Wxxx):**

| Code | Meaning |
|------|---------|
| `W101_STATE_TTL_RECOMMENDED` | Stateful query should set state TTL |
| `W102_MISSING_SOURCE_COLUMN` | Source column referenced but not defined |
| `W103_MISSING_REF_COLUMN` | Referenced model column not found |
| `W104_RULE_MAX_RETENTION` | Topic retention exceeds governance limit |
| `W105_RULE_INVALID_OWNER` | Owner not in allowed owners list |
| `W106_LOCAL_STATE_ONLY` | Local ownership state is unsafe for shared CI without remote locking |
| `W107_SCHEMA_ENRICHMENT_SKIPPED` | Optional Schema Registry enrichment was unavailable or unsupported |
| `W108_IMPORT_TARGET_EXISTS` | Dry-run target exists; a real import would refuse to overwrite it |
| `W109_ODCS_SCHEMA_INCOMPLETE` | A source or model has no declared columns, so its ODCS object omits properties |
| `W110_OPENLINEAGE_SCHEMA_INCOMPLETE` | A dataset has no declared columns, so its OpenLineage event omits the schema facet |
| `W111_OPENLINEAGE_SINK_OUTPUT_OMITTED` | A sink job has no normalized OpenLineage output dataset |
| `W112_OPENLINEAGE_EMIT_FAILED` | A finite command started, but OpenLineage START, terminal, or transport-close delivery failed without changing command truth |
| `W201_SQL_PARSE_WARNING` | Non-fatal SQL parsing issue |
| `W202_UNUSED_SOURCE` | Defined source not referenced by any model |
| `W203_SOURCE_NO_COLUMNS` | Source has no column definitions |
| `W301_COLUMN_TYPE_MISMATCH` | Column type mismatch detected |

## Examples

### CI/CD Pipeline (Single Environment)

```bash
#!/bin/bash
set -e

# Validate
streamt -o json validate --strict

# Build artifacts (manifest + checksums)
streamt build

# Show diff against deployed state
streamt diff

# Plan and apply
streamt plan
streamt apply --confirm

# Verify health
streamt status --health

# Run tests
streamt test
```

### CI/CD Pipeline (Multi-Environment)

```bash
#!/bin/bash
set -e

# Validate all environments
streamt validate --all-envs --strict

# Deploy to staging
streamt plan --env staging --out staging.plan.json
streamt apply --env staging --plan staging.plan.json
streamt test --env staging

# Deploy to production (protected, with name verification)
streamt plan --env prod --out prod.plan.json
streamt apply --env prod --plan prod.plan.json --confirm-env prod
streamt test --env prod
```

### Agent/LLM Automation

```bash
# All commands support structured JSON output
streamt -o json list models
streamt -o json show model order_metrics
streamt -o json validate
streamt -o json plan --env prod --out prod.plan.json
streamt -o json apply --env prod --plan prod.plan.json --confirm-env prod
```

### Development Workflow

```bash
# Set environment for session
export STREAMT_ENV=dev

# 1. Validate changes
streamt validate

# 2. View lineage
streamt lineage --model my_new_model

# 3. Plan deployment
streamt plan

# 4. Apply changes
streamt apply

# 5. Run tests
streamt test --model my_new_model
```

### Monitoring Script

```bash
#!/bin/bash
# Health check — exit 1 if any resource MISSING or DRIFT
streamt status --health || notify_team "Unhealthy resources detected"

# Detailed check with lag info
streamt status --lag
```

---

## See Also

- [Multi-Environment Support](../guides/multi-environment.md) — Complete guide for managing dev/staging/prod environments
