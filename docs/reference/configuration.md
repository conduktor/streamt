---
title: Configuration Reference
description: Complete reference for stream_project.yml configuration
---

# Configuration Reference

Complete reference for the `stream_project.yml` configuration file.

## File Structure

```yaml
# streamt:skip
# Project metadata
project:
  name: my-pipeline
  version: "1.0.0"
  description: My streaming pipeline

# Infrastructure connections
runtime:
  kafka: ...
  schema_registry: ...
  flink: ...
  connect: ...
  conduktor: ...

# Default settings
defaults:
  models: ...
  tests: ...

# Governance rules
rules:
  topics: ...
  models: ...
  sources: ...
  security: ...

# Inline definitions (optional)
sources: [...]
models: [...]
tests: [...]
exposures: [...]
```

## Lifecycle Ownership

`owner` names the responsible person or team. The separate strict
`ownership.mode` field controls resource lifecycle authority:

| Mode | Meaning |
|------|---------|
| `external` | Observe only; streamt never creates, updates, or deletes the resource |
| `managed` | streamt may create it; an existing live resource still requires prior ownership state |
| `adopted` | Manage an existing resource only after matching persisted adoption state exists |

Sources default to `external`; model output resources default to `managed`.
Writing `ownership: {mode: adopted}` does not perform adoption or grant authority.

## Project

Basic project metadata:

```yaml
project:
  name: fraud-detection-pipeline
  version: "1.0.0"
  description: |
    Real-time fraud detection pipeline processing
    transactions and scoring risk levels.
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Project identifier |
| `version` | string | Yes | Semantic version |
| `description` | string | No | Human-readable description |

## Deployment Ownership State

`deployment_state` selects the configured state provider. Omitting the block
preserves the existing local version 1 JSON provider used by online plan,
apply, adopt, and `state status`:

```yaml
deployment_state:
  backend: local
```

Local accepts no remote fields and stores ownership at
`.streamt/state/<environment>.json`. It is intended for a single-user checkout,
not shared or distributed runners.

The strict PostgreSQL shape lets projects explicitly initialize, register, and
inspect a future remote authority through the narrow `state init` and
`state status` administrative commands:

```yaml
deployment_state:
  backend: postgres
  namespace: platform
  lock_timeout_seconds: 30
  postgres:
    dsn_env: STREAMT_STATE_POSTGRES_DSN
    schema: streamt
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | `local` or `postgres` | local when the block is omitted | Required discriminator in every explicit block |
| `namespace` | string | - | Required, nonempty, slash-free PostgreSQL state-address namespace |
| `lock_timeout_seconds` | integer | `30` | PostgreSQL initializer advisory-lock and catalog lock wait, from 1 through 300 seconds |
| `postgres.dsn_env` | string | - | Required environment-variable name matching `^[A-Za-z_][A-Za-z0-9_]*$` |
| `postgres.schema` | string | `streamt` | Unquoted schema name matching `^[A-Za-z_][A-Za-z0-9_]*$` |

Provider blocks reject unknown keys, mixed local/PostgreSQL fields, an omitted
discriminator, and partial PostgreSQL shapes. The DSN itself must not appear in
YAML. streamt resolves the named variable only when an online command constructs
the provider, after `.env`, `.env.<environment>`, and the real environment have
been applied. Validation, compilation, and offline plan do not read it.

!!! warning "PostgreSQL is administrative-only"
    With the optional `postgres` package extra, `state status` inspects an exact
    version-1 store in a bounded, repeatable-read, read-only transaction.
    `state init` is the sole PostgreSQL administrative write: it requires exact
    project, effective-environment, and canonical-address confirmations and can
    create or register only an empty address. A missing extra, DSN, invalid
    connection policy, unavailable database, or incompatible store fails with
    a secret-neutral state error. Online plan, apply, and adopt still fail with
    `E420_STATE_BACKEND_UNAVAILABLE` and never fall back to local state.
    Ownership mutation, migration, recovery, ordinary operation locking, and
    lock-availability probing remain deferred.

!!! note "PostgreSQL roles and catalog security"
    The initializer identity owns a newly created schema and all seven state
    tables, and must own a pre-existing empty schema. Initialization revokes all
    table access from `PUBLIC` and, for a newly created schema, all schema access
    from `PUBLIC`. It never creates roles or grants privileges. The exact
    catalog requires one common schema/table owner and rejects every `PUBLIC`
    ACL; a named status role may have only non-grantable schema `USAGE` and
    non-grantable table or column `SELECT`. Any mutating or grantable non-owner
    ACL fails closed. Both administrative paths set the transaction-local
    `search_path` to `pg_catalog`, and all state objects use validated,
    schema-qualified identifiers.

In multi-environment mode, a root `deployment_state` is inherited when the
selected environment omits the block. An environment block replaces the whole
root provider block; it is never deep-merged. Root `runtime` remains ignored in
multi-environment mode, so these two sections intentionally have different
precedence rules.

## Runtime

### Kafka

```yaml
runtime:
  kafka:
    bootstrap_servers: "kafka-1:9092,kafka-2:9092,kafka-3:9092"

    # Security (optional)
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: ${KAFKA_USER}
    sasl_password: ${KAFKA_PASSWORD}

    # SSL/mTLS (optional)
    ssl_ca_location: /path/to/ca.pem
    ssl_certificate_location: /path/to/client-cert.pem
    ssl_key_location: /path/to/client-key.pem
    ssl_key_password: ${SSL_KEY_PASSWORD}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bootstrap_servers` | string/list | Required | Kafka broker addresses |
| `security_protocol` | string | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl_mechanism` | string | - | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `sasl_username` | string | - | SASL username |
| `sasl_password` | string | - | SASL password |
| `ssl_ca_location` | string | - | Path to CA certificate for SSL verification |
| `ssl_certificate_location` | string | - | Path to client certificate for mTLS |
| `ssl_key_location` | string | - | Path to client private key for mTLS |
| `ssl_key_password` | string | - | Password for encrypted client key |

### Schema Registry

```yaml
runtime:
  schema_registry:
    url: http://schema-registry:8081

    # Authentication (optional)
    username: ${SR_USER}
    password: ${SR_PASSWORD}

    # SSL/mTLS (optional)
    ssl_ca_location: /path/to/ca.pem
    ssl_certificate_location: /path/to/client-cert.pem
    ssl_key_location: /path/to/client-key.pem
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | Required | Schema Registry URL |
| `username` | string | - | Basic auth username |
| `password` | string | - | Basic auth password |
| `ssl_ca_location` | string | - | Path to CA certificate for SSL verification |
| `ssl_certificate_location` | string | - | Path to client certificate for mTLS |
| `ssl_key_location` | string | - | Path to client key for mTLS |

### Flink

```yaml
runtime:
  flink:
    default: production    # Default cluster to use
    clusters:
      production:
        type: rest
        rest_url: http://flink-jobmanager:8081
        sql_gateway_url: http://flink-sql-gateway:8083
```

Only REST/SQL Gateway targets are currently supported. Docker, Kubernetes
Operator, and Confluent Cloud backends are planned but are not accepted as
working deployment targets yet.

**REST cluster:**

```yaml
clusters:
  my-cluster:
    type: rest
    rest_url: http://flink-jobmanager:8081
    sql_gateway_url: http://flink-sql-gateway:8083
    # Auth (optional)
    username: ${FLINK_USER}
    password: ${FLINK_PASSWORD}
    # Or Bearer token
    api_key: ${FLINK_API_KEY}
    # SSL/mTLS (optional)
    ssl_ca_location: /path/to/ca.pem
    ssl_certificate_location: /path/to/client-cert.pem
    ssl_key_location: /path/to/client-key.pem
```

### Connect

```yaml
runtime:
  connect:
    default: production
    clusters:
      production:
        rest_url: http://kafka-connect:8083
        # Authentication (optional)
        username: ${CONNECT_USER}
        password: ${CONNECT_PASSWORD}
        # SSL/mTLS (optional)
        ssl_ca_location: /path/to/ca.pem
        ssl_certificate_location: /path/to/client-cert.pem
        ssl_key_location: /path/to/client-key.pem
```

| Field | Type | Description |
|-------|------|-------------|
| `rest_url` | string | Connect REST API URL |
| `username` | string | Basic auth username |
| `password` | string | Basic auth password |
| `ssl_ca_location` | string | Path to CA certificate for SSL verification |
| `ssl_certificate_location` | string | Path to client certificate for mTLS |
| `ssl_key_location` | string | Path to client key for mTLS |

### Conduktor (Optional)

Conduktor Gateway enables [virtual topics](../guides/gateway.md) and data masking.

```yaml
runtime:
  conduktor:
    gateway:
      admin_url: http://localhost:8888       # Gateway Admin API
      proxy_bootstrap: localhost:6969         # Gateway proxy for Kafka clients
      username: ${GATEWAY_USER}               # Admin API username (default: admin)
      password: ${GATEWAY_PASSWORD}           # Admin API password (default: conduktor)
      virtual_cluster: default                # Optional: for multi-tenant setups

    console:
      url: http://conduktor-console:8080
      api_key: ${CONDUKTOR_API_KEY}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `admin_url` | string | Required | Gateway Admin API URL |
| `proxy_bootstrap` | string | - | Gateway proxy for Kafka clients |
| `username` | string | `admin` | Admin API username |
| `password` | string | `conduktor` | Admin API password |
| `virtual_cluster` | string | - | Virtual cluster for multi-tenancy |

!!! note "When to configure Gateway"
    Gateway is required for virtual topic models (auto-inferred from `gateway:` configuration). If you only use simple models without Gateway features, it's optional.

## Defaults

Set default values for topics, models, and tests:

```yaml
defaults:
  # Project-wide topic defaults (simplest)
  topic:
    partitions: 1              # Default: 1 (works for local dev)
    replication_factor: 1      # Default: 1 (works for local dev)

  models:
    cluster: production
    # Model-specific topic defaults (overrides project-wide)
    topic:
      partitions: 6
      replication_factor: 3

  tests:
    flink_cluster: production
```

!!! tip "Local Development vs Production"
    The built-in defaults (`partitions: 1`, `replication_factor: 1`) work out of the box with a single-broker local Kafka. For production, override in your project:

    ```yaml
    defaults:
      topic:
        partitions: 6
        replication_factor: 3
    ```

## Governance Rules

### Topic Rules

```yaml
rules:
  topics:
    min_partitions: 3
    max_partitions: 128
    min_replication_factor: 2
    max_replication_factor: 5
    naming_pattern: "^[a-z]+\\.[a-z]+\\.v[0-9]+$"
    forbidden_prefixes:
      - "_"
      - "test"
      - "tmp"
```

| Rule | Type | Description |
|------|------|-------------|
| `min_partitions` | int | Minimum partition count |
| `max_partitions` | int | Maximum partition count |
| `min_replication_factor` | int | Minimum RF |
| `max_replication_factor` | int | Maximum RF |
| `naming_pattern` | regex | Required topic name pattern |
| `forbidden_prefixes` | list | Disallowed name prefixes |

### Model Rules

```yaml
rules:
  models:
    require_description: true
    require_owner: true
    require_tests: true
    max_dependencies: 10
```

| Rule | Type | Description |
|------|------|-------------|
| `require_description` | bool | Models must have description |
| `require_owner` | bool | Models must have owner |
| `require_tests` | bool | Models must have tests |
| `max_dependencies` | int | Maximum upstream dependencies |

### Source Rules

```yaml
rules:
  sources:
    require_schema: true
    require_freshness: true
```

### Security Rules

```yaml
rules:
  security:
    require_classification: true
    sensitive_columns_require_masking: true
```

## Environment Variables

Use `${VAR_NAME}` to reference environment variables:

```yaml
runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
    sasl_password: ${KAFKA_PASSWORD}
```

Variables can be set:

1. **System environment**: `export KAFKA_PASSWORD=secret`
2. **.env file**: Create `.env` in project root
3. **CI/CD secrets**: Injected by your CI system

```bash title=".env"
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_PASSWORD=secret
SNOWFLAKE_URL=account.snowflakecomputing.com
```

## Complete Example

```yaml title="stream_project.yml"
project:
  name: ecommerce-pipeline
  version: "2.1.0"
  description: E-commerce real-time analytics pipeline

runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BROKERS}
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: ${KAFKA_USER}
    sasl_password: ${KAFKA_PASSWORD}

  schema_registry:
    url: https://schema-registry.example.com
    username: sr-user
    password: sr-secret

  flink:
    default: production
    clusters:
      production:
        type: rest
        rest_url: https://flink.example.com:8081
        sql_gateway_url: https://flink.example.com:8083
        username: flink-user
        password: flink-secret
      staging:
        type: rest
        rest_url: https://flink-staging.example.com:8081
        sql_gateway_url: https://flink-staging.example.com:8083

  connect:
    default: production
    clusters:
      production:
        rest_url: https://connect.example.com:8083
        username: connect-user
        password: connect-secret

defaults:
  models:
    topic:
      partitions: 12
      replication_factor: 3

  tests:
    flink_cluster: production

rules:
  topics:
    min_partitions: 6
    naming_pattern: "^ecom\\.[a-z-]+\\.v[0-9]+$"

  models:
    require_description: true
    require_owner: true
    require_tests: true

  security:
    require_classification: true
    sensitive_columns_require_masking: true
```

## Multi-Environment Mode

For managing multiple environments (dev, staging, prod), create an `environments/` directory. See the [Multi-Environment Guide](../guides/multi-environment.md) for complete documentation.

### Quick Overview

```
project/
├── stream_project.yml     # Project config (no runtime section)
├── environments/
│   ├── dev.yml            # Development environment
│   ├── staging.yml        # Staging environment
│   └── prod.yml           # Production environment
├── .env                   # Base environment variables
├── .env.dev               # Dev-specific variables
├── .env.staging           # Staging-specific variables
└── .env.prod              # Prod-specific variables
```

### Environment File Format

```yaml title="environments/prod.yml"
environment:
  name: prod
  description: Production environment
  protected: true              # Requires a reviewed plan and confirmation

runtime:
  kafka:
    bootstrap_servers: ${PROD_KAFKA_SERVERS}
  schema_registry:
    url: ${PROD_SR_URL}
  flink:
    default: prod-cluster
    clusters:
      prod-cluster:
        rest_url: ${PROD_FLINK_URL}

safety:
  confirm_apply: true          # Require --confirm in CI/CD
  allow_destructive: false     # Block topic deletions, etc.
  require_reviewed_plan: true  # Require apply --plan (also implied by protected)
  require_remote_state: true   # Reject apply/adopt while effective state is local
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `environment.name` | string | Required | Must match filename |
| `environment.description` | string | - | Human-readable description |
| `environment.protected` | bool | `false` | Require a reviewed plan and environment confirmation for apply |
| `safety.confirm_apply` | bool | value of `protected` | Require environment confirmation in CI |
| `safety.allow_destructive` | bool | `false` | Allow destructive operations |
| `safety.require_reviewed_plan` | bool | `false` | Require `apply --plan` for a shared or otherwise review-gated environment |
| `safety.require_remote_state` | bool | `false` | Reject apply/adopt when effective deployment state is local |

Protected environments always require a reviewed plan, even when
`safety.require_reviewed_plan` is `false`. Set `require_reviewed_plan: true` on
an unprotected environment to mark a shared workflow explicitly without relying
on its name. `require_remote_state` is environment-only, is not implied by
`protected`, and cannot be bypassed with confirmation or `--force`. Read-only
plan and state status remain available. Environment sidecars accept only
`environment`, `runtime`, `safety`, and `deployment_state` at the top level.
Policy values are strict booleans, and unknown or
misspelled environment and safety fields are rejected.

---

## File Organization

### Single File (Simple Projects)

```yaml title="stream_project.yml"
project:
  name: simple-pipeline

runtime:
  kafka:
    bootstrap_servers: localhost:9092

sources:
  - name: events
    topic: events.raw

models:
  - name: events_clean
    description: "Cleaned events stream"
    # materialized: topic (auto-inferred)
    sql: |
      SELECT * FROM {{ source("events") }}
      WHERE event_id IS NOT NULL
```

### Multi-File (Large Projects)

```
project/
├── stream_project.yml     # Config + runtime
├── sources/
│   ├── orders.yml
│   └── users.yml
├── models/
│   ├── orders/
│   │   ├── orders_clean.yml
│   │   └── order_metrics.yml
│   └── users/
│       └── user_activity.yml
├── tests/
│   └── orders_tests.yml
└── exposures/
    └── services.yml
```
