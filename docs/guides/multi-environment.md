---
title: Multi-Environment Support
description: Managing dev, staging, and production environments with streamt
---

# Multi-Environment Support

streamt supports managing multiple environments (dev, staging, prod) with isolated configurations, protected environment safeguards, and per-environment secrets.

## Overview

In multi-environment mode, each environment has its own:

- **Runtime configuration** (Kafka clusters, Flink clusters, Schema Registry)
- **Safety settings** (protected environments, destructive operation controls)
- **Environment variables** (via `.env.{env}` files)

## Setup

### Directory Structure

Create an `environments/` directory in your project root:

```
my-project/
├── stream_project.yml      # Project definition (no runtime section)
├── environments/
│   ├── dev.yml             # Development environment
│   ├── staging.yml         # Staging environment
│   └── prod.yml            # Production environment
├── .env                    # Base environment variables
├── .env.dev                # Dev-specific variables
├── .env.staging            # Staging-specific variables
├── .env.prod               # Prod-specific variables
├── models/
└── sources/
```

!!! note "Mode Detection"
    streamt automatically detects the mode:

    - **Single-env mode**: No `environments/` directory → `runtime:` required in `stream_project.yml`
    - **Multi-env mode**: `environments/` directory exists → `runtime:` comes from environment files

### Environment File Format

Each environment file defines runtime configuration and safety settings:

```yaml title="environments/dev.yml"
environment:
  name: dev
  description: Local development environment

runtime:
  kafka:
    bootstrap_servers: localhost:9092
  schema_registry:
    url: http://localhost:8081
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8082
        sql_gateway_url: http://localhost:8084
```

```yaml title="environments/prod.yml"
environment:
  name: prod
  description: Production environment
  protected: true  # Requires confirmation for apply

runtime:
  kafka:
    bootstrap_servers: ${PROD_KAFKA_SERVERS}
  schema_registry:
    url: ${PROD_SR_URL}
    username: ${PROD_SR_USER}
    password: ${PROD_SR_PASS}
  flink:
    default: prod-cluster
    clusters:
      prod-cluster:
        rest_url: ${PROD_FLINK_URL}

safety:
  confirm_apply: true         # Require --confirm flag in CI
  allow_destructive: false    # Block topic deletions, etc.
```

## CLI Usage

### Targeting Environments

Use the `--env` flag to target a specific environment:

```bash
# Validate specific environment
streamt validate --env dev
streamt validate --env prod

# Plan deployment
streamt plan --env staging

# Apply changes
streamt apply --env dev
```

### Environment Variable

Set `STREAMT_ENV` to avoid repeating `--env`:

```bash
export STREAMT_ENV=dev
streamt validate  # Uses dev environment
streamt plan      # Uses dev environment
streamt apply     # Uses dev environment
```

!!! info "CLI Flag Priority"
    The `--env` flag always overrides `STREAMT_ENV`:
    ```bash
    export STREAMT_ENV=dev
    streamt validate --env prod  # Uses prod, not dev
    ```

### Validate All Environments

Validate all environments at once with `--all-envs`:

```bash
streamt validate --all-envs
```

This validates each environment sequentially and fails if any environment is invalid.

### List Environments

View available environments:

```bash
streamt envs list
```

Output:
```
dev          Local development environment
staging      Staging environment
prod         Production environment [protected]
```

### Show Environment Config

View resolved configuration (secrets masked):

```bash
streamt envs show prod
```

Output:
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

## Protected Environments

Mark critical environments as protected to prevent accidental deployments:

```yaml title="environments/prod.yml"
environment:
  name: prod
  protected: true
```

### Behavior

**Interactive mode (terminal):**
```bash
$ streamt apply --env prod
WARNING: Deploying to protected environment 'prod'
WARNING: 'prod' is a protected environment.
Type 'prod' to confirm: _
```

**Non-interactive mode (CI/CD):**
```bash
# Without --confirm: fails
$ streamt apply --env prod
ERROR: 'prod' is a protected environment. Use --confirm flag in non-interactive mode.

# With --confirm: proceeds
$ streamt apply --env prod --confirm
WARNING: Deploying to protected environment 'prod'
Applying changes...
```

## Destructive Safety

Block destructive operations (topic deletions, connector removals) in critical environments:

```yaml title="environments/prod.yml"
safety:
  allow_destructive: false
```

### Behavior

```bash
# Blocked by default
$ streamt apply --env prod --confirm
ERROR: Destructive operations blocked for 'prod' environment. Use --force flag to override.

# Override with --force
$ streamt apply --env prod --confirm --force
WARNING: --force flag used, allowing destructive operations on 'prod'
Applying changes...
```

## Environment Variables

### .env File Precedence

Environment variables are loaded with this precedence (later wins):

1. `.env` — Base variables (always loaded)
2. `.env.{environment}` — Environment-specific overrides
3. Actual environment variables — Highest priority

### Example

```bash title=".env"
KAFKA_SERVERS=localhost:9092
LOG_LEVEL=INFO
```

```bash title=".env.prod"
KAFKA_SERVERS=prod-kafka.example.com:9092
LOG_LEVEL=WARN
```

```yaml title="environments/prod.yml"
runtime:
  kafka:
    bootstrap_servers: ${KAFKA_SERVERS}  # Resolves to prod-kafka.example.com:9092
```

### Override in CI/CD

```bash
# .env.prod has KAFKA_SERVERS=prod-kafka.example.com:9092
# But actual env var takes precedence
export KAFKA_SERVERS=custom-kafka.example.com:9092
streamt apply --env prod --confirm  # Uses custom-kafka.example.com:9092
```

## CI/CD Integration

### GitHub Actions Example

```yaml title=".github/workflows/deploy.yml"
name: Deploy

on:
  push:
    branches: [main]

jobs:
  deploy-staging:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to Staging
        env:
          STAGING_KAFKA: ${{ secrets.STAGING_KAFKA }}
          STAGING_SR_URL: ${{ secrets.STAGING_SR_URL }}
        run: |
          streamt validate --env staging
          streamt plan --env staging
          streamt apply --env staging

  deploy-prod:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production  # GitHub environment protection
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to Production
        env:
          PROD_KAFKA_SERVERS: ${{ secrets.PROD_KAFKA }}
          PROD_SR_URL: ${{ secrets.PROD_SR_URL }}
        run: |
          streamt validate --env prod
          streamt plan --env prod
          # --confirm required for protected environments in CI
          streamt apply --env prod --confirm
```

## Best Practices

### 1. Use Protected Environments for Production

```yaml
environment:
  name: prod
  protected: true

safety:
  confirm_apply: true
  allow_destructive: false
```

### 2. Keep Secrets in .env Files

Never commit secrets to environment YAML files. Use variable references:

```yaml
# Good: Reference variables
runtime:
  kafka:
    bootstrap_servers: ${KAFKA_SERVERS}
  schema_registry:
    password: ${SR_PASSWORD}
```

```bash
# .env.prod (gitignored)
KAFKA_SERVERS=prod-kafka:9092
SR_PASSWORD=secret123
```

### 3. Validate All Environments in CI

```bash
# In CI pipeline
streamt validate --all-envs
```

### 4. Use Descriptive Environment Names

```yaml
environment:
  name: prod-us-east-1
  description: Production cluster in US East region
```

## Migration from Single-Env Mode

To migrate an existing single-env project:

1. Create `environments/` directory
2. Move `runtime:` from `stream_project.yml` to `environments/dev.yml`
3. Create additional environment files as needed
4. Update CI/CD to use `--env` flag

Before:
```yaml title="stream_project.yml"
project:
  name: my-project
  version: "1.0.0"

runtime:
  kafka:
    bootstrap_servers: localhost:9092

sources:
  - name: events
    topic: events.raw.v1
```

After:
```yaml title="stream_project.yml"
project:
  name: my-project
  version: "1.0.0"

sources:
  - name: events
    topic: events.raw.v1
```

```yaml title="environments/dev.yml"
environment:
  name: dev
  description: Development environment

runtime:
  kafka:
    bootstrap_servers: localhost:9092
```

## Troubleshooting

### "No environments configured"

```
ERROR: No environments configured. Remove --env flag or create environments/ directory.
```

You're using `--env` in single-env mode. Either:
- Create an `environments/` directory with environment files
- Remove the `--env` flag

### "Multiple environments found. Specify with --env"

```
ERROR: Multiple environments found. Specify with --env. Available: dev, staging, prod
```

In multi-env mode, you must specify which environment to use:
```bash
streamt validate --env dev
```

Or set the environment variable:
```bash
export STREAMT_ENV=dev
streamt validate
```

### "Environment 'xyz' not found"

```
ERROR: Environment 'xyz' not found. Available: dev, staging, prod
```

Check that the environment file exists: `environments/xyz.yml`

### "Environment name mismatch"

```
ERROR: Environment name mismatch: file is 'dev.yml' but environment.name is 'development'. They must match.
```

The `name` in the YAML must match the filename:
```yaml title="environments/dev.yml"
environment:
  name: dev  # Must match filename 'dev.yml'
```
