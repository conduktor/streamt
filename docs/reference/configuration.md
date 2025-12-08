---
title: Configuration Reference
description: Complete reference for stream_project.yml configuration
---

# Configuration Reference

Complete reference for the `stream_project.yml` configuration file.

## File Structure

```yaml
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

## Runtime

### Kafka

```yaml
runtime:
  kafka:
    bootstrap_servers: kafka:9092
    # Or multiple brokers
    bootstrap_servers:
      - kafka-1:9092
      - kafka-2:9092
      - kafka-3:9092

    # Security (optional)
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: ${KAFKA_USER}
    sasl_password: ${KAFKA_PASSWORD}

    # SSL (optional)
    ssl_ca_location: /path/to/ca.pem
    ssl_certificate_location: /path/to/cert.pem
    ssl_key_location: /path/to/key.pem
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bootstrap_servers` | string/list | Required | Kafka broker addresses |
| `security_protocol` | string | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl_mechanism` | string | - | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `sasl_username` | string | - | SASL username |
| `sasl_password` | string | - | SASL password |

### Schema Registry

```yaml
runtime:
  schema_registry:
    url: http://schema-registry:8081
    # Or multiple URLs
    url:
      - http://sr-1:8081
      - http://sr-2:8081

    # Authentication (optional)
    username: ${SR_USER}
    password: ${SR_PASSWORD}

    # SSL (optional)
    ssl:
      ca_location: /path/to/ca.pem
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string/list | Required | Schema Registry URL(s) |
| `username` | string | - | Basic auth username |
| `password` | string | - | Basic auth password |

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

      local:
        type: docker
        version: "1.18"

      kubernetes:
        type: kubernetes
        namespace: flink-jobs
        service_account: flink-sa
```

**Cluster Types:**

=== "REST"

    ```yaml
    clusters:
      my-cluster:
        type: rest
        rest_url: http://flink-jobmanager:8081
        sql_gateway_url: http://flink-sql-gateway:8083
    ```

=== "Docker"

    ```yaml
    clusters:
      local:
        type: docker
        version: "1.18"
        network: my-network
    ```

=== "Kubernetes"

    ```yaml
    clusters:
      k8s:
        type: kubernetes
        namespace: flink-jobs
        service_account: flink-sa
        image: flink:1.18
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
```

| Field | Type | Description |
|-------|------|-------------|
| `rest_url` | string | Connect REST API URL |
| `username` | string | Basic auth username |
| `password` | string | Basic auth password |

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
    Gateway is required for `materialized: virtual_topic`. If you only use `topic`, `flink`, or `sink` materializations, Gateway is optional.

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
    flink_cluster: production
    # Model-specific topic defaults (overrides project-wide)
    topic:
      partitions: 6
      replication_factor: 3
      config:
        retention.ms: 604800000

  tests:
    flink_cluster: production
    sample_size: 1000
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
    min_tests: 1
    max_dependencies: 10
    allowed_materializations:
      - topic
      - flink
      - sink
```

| Rule | Type | Description |
|------|------|-------------|
| `require_description` | bool | Models must have description |
| `require_owner` | bool | Models must have owner |
| `require_tests` | bool | Models must have tests |
| `min_tests` | int | Minimum test count |
| `max_dependencies` | int | Maximum upstream dependencies |

### Source Rules

```yaml
rules:
  sources:
    require_schema: true
    require_freshness: true
    require_columns: true
    require_owner: true
```

### Security Rules

```yaml
rules:
  security:
    require_classification: true
    sensitive_columns_require_masking: true
    allowed_classifications:
      - public
      - internal
      - confidential
      - sensitive
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
    url: ${SCHEMA_REGISTRY_URL}
    username: ${SR_USER}
    password: ${SR_PASSWORD}

  flink:
    default: production
    clusters:
      production:
        type: rest
        rest_url: ${FLINK_REST_URL}
        sql_gateway_url: ${FLINK_SQL_GATEWAY_URL}
      staging:
        type: rest
        rest_url: ${FLINK_STAGING_URL}
        sql_gateway_url: ${FLINK_STAGING_SQL_URL}

  connect:
    default: production
    clusters:
      production:
        rest_url: ${CONNECT_URL}

defaults:
  models:
    topic:
      partitions: 12
      replication_factor: 3
      config:
        retention.ms: 604800000
        min.insync.replicas: 2

  tests:
    sample_size: 5000

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
    sql: SELECT * FROM {{ source("events") }}
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
