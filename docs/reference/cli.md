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
| Export OpenLineage design metadata | `docs openlineage` | No |
| Declare existing Kafka topics as external sources | `import` | **Yes** |
| See what would change on deploy | `plan` | **Yes** |
| Compare local vs deployed state | `diff` | **Yes** |
| Inspect ownership/recovery metadata | `state status` | No |
| Claim an existing declared topic or schema subject | `adopt` | **Yes** |
| Deploy to infrastructure | `apply` | **Yes** |

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
`is_apply_blocked: true`. Reviewed plan format version 3 includes these blockers
and an exact ownership-state reference in its integrity checksum. Older version
1 and 2 files are rejected with regeneration guidance.

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
| `--force` | Allow destructive operations in protected environments |

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

Online JSON plan output also includes `operation_status`, containing only safe
status, operation ID/kind, stable failure code, and last safely successful
action index. Planning is read-only and does not clear an unfinished operation.
Offline output reports this status as `unavailable` without constructing a
state backend.

After a successful non-dry-run apply, streamt atomically records resources it
manages or has adopted in `.streamt/state/<environment>.json`. External,
unowned, and ownership-blocked resources are never recorded. Local state is
appropriate for a single-user development checkout only. Strict PostgreSQL
configuration is recognized, but the provider is unavailable in this release;
online commands return `E420_STATE_BACKEND_UNAVAILABLE` without reading local
state. Shared CI still needs the later remote provider and distributed locking.
Failed and rolled-back planner results do not advance ownership state before
final commit.

An environment may set `safety.require_remote_state: true` to reject `apply`
and `adopt` with `E421_REMOTE_STATE_REQUIRED` while the effective provider is
local. This check runs after reviewed/offline plan-file gates but before
confirmation, compilation, state access, or runtime deployer construction.
`--force` cannot bypass it. Read-only plan and state status are not blocked.

For local apply/adopt, `.streamt/state/<environment>.control.json` records a
versioned durable intent before mutation and safe ordered action progress. Both
`in_progress` and `recovery_required` block later apply/adopt commands
indefinitely with `E419_STATE_RECOVERY_REQUIRED`; elapsed time is not proof that
a runtime call failed. There is no recovery command yet. Do not delete or edit
the sidecar, and do not roll back streamt versions while a marker exists;
retain the evidence and reconcile live infrastructure with ownership state.
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
    online plan's `operation_status` and retain the sidecar evidence until the
    explicit recovery workflow is available.

!!! warning "Flink Job Lifecycle"
    Existing Flink job updates are currently blocked because the available
    cancel-and-resubmit path is not savepoint-safe. New jobs may still be
    submitted and unchanged jobs are left alone. See [Flink Options Reference —
    Job Lifecycle on Apply](flink-options.md#job-lifecycle-on-apply) for the
    underlying lifecycle boundary.

---

### adopt

Explicitly claim one existing Kafka topic or Schema Registry subject for
lifecycle management. Adoption changes only local ownership state; it never
mutates Kafka or Schema Registry.

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

`--name` is the stable logical declaration name, not a physical topic or
subject name. It must resolve to exactly one compiled artifact whose declaration
explicitly sets `ownership.mode: adopted`. Topic adoption displays partitions,
replication factor, dynamic configuration, and pending differences. Schema
adoption displays only the subject, type, version, schema ID, compatibility,
content checksums, and pending differences; schema bodies are never printed.
Credential-shaped values are redacted.

Interactive use requires typing an exact token containing both the full
resource ID and environment. Non-interactive use requires both exact
`--confirm-resource` and `--confirm-env` values; there is no generic yes/force
flag. A successful adoption atomically advances only the environment-scoped
local ownership state. Repeating an identical adoption is a no-op and does not
advance its serial.

!!! warning "Local state only"
    Adoption state is stored at `.streamt/state/<environment>.json`. It is safe
    for a single-user development checkout, not shared CI: remote state and
    locking are not implemented. Run `streamt plan --out ...` after adoption
    and review that plan before applying any pending differences.

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
| `--deploy` | Deploy continuous tests as Flink jobs |
| `--coverage` | Show test coverage report (which models have tests) |

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

# Deploy continuous monitoring
streamt test --deploy
```

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
recover them. Local is the only working backend. PostgreSQL configuration is
recognized but status returns sanitized `E420_STATE_BACKEND_UNAVAILABLE` and
never falls back to local state. `state init`, recovery, migration, remote lock
availability probing, and the PostgreSQL provider are not implemented.

```bash
streamt state status -p . -e prod
streamt -o json state status -p . -e prod
```

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
not emit `RunEvent` records. Ordinary compile/apply/test commands and deployed
Flink, Gateway, or Connect processes do not currently emit OpenLineage
telemetry. See [OpenLineage static export](openlineage.md) for exact mapping,
namespace, validation, and security boundaries.

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
| `E408_PLAN_FILE_INVALID` | Reviewed plan is malformed or its integrity checksum fails |
| `E409_PLAN_STALE` | Project, environment, ownership state, or live actions drifted after review |
| `E410_OWNERSHIP_REQUIRED` | A live resource needs an explicit ownership decision or adoption |
| `E411_STATE_INVALID` | Ownership state is malformed or belongs to another context |
| `E412_ADOPTION_TARGET_INVALID` | Adoption target is missing, ambiguous, or not explicitly declared adopted |
| `E413_ADOPTION_LIVE_NOT_FOUND` | Declared physical topic does not exist in Kafka |
| `E414_ADOPTION_CONFIRMATION_REQUIRED` | Exact resource and environment confirmation is absent or incorrect |
| `E415_ADOPTION_STATE_CONFLICT` | Existing ownership state conflicts with the requested claim |
| `E416_ADOPTION_FAILED` | Live observation or atomic adoption-state persistence failed |
| `E417_SAFETY_BLOCKED` | Apply refused an unsupported partition, schema, or Flink migration before mutation |
| `E418_REVIEWED_PLAN_REQUIRED` | Direct apply is disabled by protected/shared environment policy; create and apply a reviewed plan file |
| `E419_STATE_RECOVERY_REQUIRED` | An unfinished local operation marker blocks apply/adopt pending explicit recovery |
| `E420_STATE_BACKEND_UNAVAILABLE` | The configured deployment-state provider cannot be used; no fallback occurs |
| `E421_REMOTE_STATE_REQUIRED` | Environment policy rejects apply/adopt while local deployment state is selected |

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
