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
  --project-dir PATH    Path to project directory (default: current)
  --env ENV             Target environment (for multi-env mode)
  --version            Show version and exit
  --help               Show help message and exit
```

!!! info "Environment Selection"
    In multi-environment mode, specify the target environment with `--env` or the `STREAMT_ENV` variable. The CLI flag takes precedence over the environment variable.

## Commands

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
| `--output PATH` | Output directory (default: `generated/`) |
| `--dry-run` | Show what would be generated without writing |

**Examples:**

```bash
# Compile to default directory
streamt compile

# Compile for specific environment
streamt compile --env prod

# Custom output directory
streamt compile --output ./build

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
| `--auto-approve` | Skip confirmation prompt |
| `--confirm` | Confirm deployment to protected environments (required in CI/CD) |
| `--force` | Allow destructive operations in protected environments |

**Examples:**

```bash
# Deploy all
streamt apply

# Deploy to specific environment
streamt apply --env dev

# Deploy to protected environment (CI/CD)
streamt apply --env prod --confirm

# Override destructive safety (use with caution)
streamt apply --env prod --confirm --force

# Deploy specific model
streamt apply --target order_metrics

# Deploy by tag
streamt apply --select tag:critical

# Auto-approve (CI/CD)
streamt apply --auto-approve
```

!!! warning "Protected Environments"
    When deploying to a protected environment, you must confirm interactively (by typing the environment name) or use `--confirm` in CI/CD. If destructive operations are blocked (`allow_destructive: false`), use `--force` to override.

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
| 1 | General error |
| 2 | Validation error |
| 3 | Deployment error |
| 4 | Test failure |

## Examples

### CI/CD Pipeline (Single Environment)

```bash
#!/bin/bash
set -e

# Validate
streamt validate --strict

# Plan and show diff
streamt plan

# Apply (auto-approve in CI)
streamt apply --auto-approve

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
streamt apply --env staging --auto-approve
streamt test --env staging

# Deploy to production (protected)
streamt plan --env prod
streamt apply --env prod --confirm --auto-approve
streamt test --env prod
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
# Check all resources
streamt status

# Alert on failures
if ! streamt status --jobs | grep -q "RUNNING"; then
  echo "Alert: Flink job not running!"
  exit 1
fi
```

---

## Coming Soon

The following commands and options are planned:

| Command/Option | Description | Status |
|----------------|-------------|--------|
| `streamt status --health` | Health checks with thresholds | Planned |
| `streamt rollback` | Rollback to previous deployment | Planned |
| `streamt diff` | Show diff between local and deployed | Planned |
| `streamt init` | Initialize new project from template | Planned |
| `streamt build` | Generate deployable artifacts | Planned |

See the [roadmap](https://github.com/conduktor/streamt#roadmap) for the full list of planned features.

## See Also

- [Multi-Environment Support](../guides/multi-environment.md) — Complete guide for managing dev/staging/prod environments
