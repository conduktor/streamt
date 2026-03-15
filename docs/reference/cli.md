---
title: CLI Reference
description: Complete reference for all streamt commands
---

# CLI Reference

Complete reference for all streamt CLI commands.

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
| `--check-schemas` | Fetch and validate schemas from Schema Registry |
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
| `--target MODEL` | Plan only for specific model |

**Examples:**

```bash
# Plan all changes
streamt plan

# Plan for specific environment
streamt plan --env staging

# Plan specific model
streamt plan --target order_metrics
```

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

# Deploy to protected environment (CI/CD)
streamt apply --env prod --confirm

# Non-interactive confirm with name verification (agents/CI)
streamt apply --env prod --confirm-env prod

# Override destructive safety (use with caution)
streamt apply --env prod --confirm --force

# Deploy specific model
streamt apply --target order_metrics

# Deploy by tag
streamt apply --select tag:critical
```

!!! warning "Protected Environments"
    When deploying to a protected environment, you must confirm interactively (by typing the environment name), use `--confirm`, or use `--confirm-env ENV` (which also verifies the environment name matches). If destructive operations are blocked (`allow_destructive: false`), use `--force` to override.

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

When using `--output json`, errors include machine-readable codes. These codes follow a taxonomy for programmatic handling:

| Code | Meaning |
|------|---------|
| `E101_SOURCE_NOT_FOUND` | Referenced source does not exist |
| `E102_MODEL_NOT_FOUND` | Referenced model does not exist |
| `E103_DUPLICATE_NAME` | Duplicate resource name |
| `E104_CYCLE_DETECTED` | Circular dependency in DAG |
| `E105_INVALID_REF` | Invalid `ref()` or `source()` reference |
| `E201_MISSING_CONFIG` | Required configuration is missing |
| `E202_INVALID_VALUE` | Configuration value is invalid |
| `E203_ENVIRONMENT_ERROR` | Environment configuration error |
| `E301_SCHEMA_MISMATCH` | Schema compatibility error |
| `E401_DEPLOY_FAILED` | Deployment operation failed |
| `E501_PARSE_ERROR` | YAML/SQL parsing error |

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
streamt plan --env staging
streamt apply --env staging
streamt test --env staging

# Deploy to production (protected, with name verification)
streamt plan --env prod
streamt apply --env prod --confirm-env prod
streamt test --env prod
```

### Agent/LLM Automation

```bash
# All commands support structured JSON output
streamt -o json list models
streamt -o json show model order_metrics
streamt -o json validate
streamt -o json plan --env staging
streamt -o json apply --env prod --confirm-env prod
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
