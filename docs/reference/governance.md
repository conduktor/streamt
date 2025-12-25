---
title: Governance Rules
description: Enforce standards and policies across your streaming projects
---

# Governance Rules

Governance rules enforce standards, naming conventions, and policies across your streaming projects. They're checked during `streamt validate`.

## Overview

Rules are defined in `stream_project.yml`:

```yaml
rules:
  topics:
    min_partitions: 6
    naming_pattern: "^[a-z]+\\.[a-z]+\\.v[0-9]+$"

  models:
    require_description: true
    require_tests: true

  sources:
    require_schema: true

  security:
    require_classification: true
```

## Topic Rules

Control Kafka topic configuration:

```yaml
rules:
  topics:
    # Partition requirements
    min_partitions: 3
    max_partitions: 128

    # Replication requirements
    min_replication_factor: 2
    # max_replication_factor: 5  # Planned - not yet implemented

    # Naming conventions
    naming_pattern: "^[a-z]+\\.[a-z-]+\\.v[0-9]+$"
    forbidden_prefixes:
      - "_"
      - "test"
      - "tmp"
      - "dev"
    # forbidden_suffixes:       # Planned - not yet implemented
    #   - "-test"
    #   - "-tmp"

    # Required configurations
    required_config:
      - retention.ms
      - min.insync.replicas
```

### Rule Reference

| Rule | Type | Description | Status |
|------|------|-------------|--------|
| `min_partitions` | int | Minimum partition count | ✅ |
| `max_partitions` | int | Maximum partition count | ✅ |
| `min_replication_factor` | int | Minimum replication factor | ✅ |
| `max_replication_factor` | int | Maximum replication factor | 🚧 Planned |
| `naming_pattern` | regex | Required topic name pattern | ✅ |
| `forbidden_prefixes` | list | Disallowed name prefixes | ✅ |
| `forbidden_suffixes` | list | Disallowed name suffixes | 🚧 Planned |
| `required_config` | list | Configs that must be set | ✅ |

### Naming Pattern Examples

```yaml
# Domain.entity.version pattern
naming_pattern: "^[a-z]+\\.[a-z-]+\\.v[0-9]+$"
# Matches: orders.created.v1, payments.processed.v2
# Rejects: Orders.Created, orders_created_v1

# Environment prefix pattern
naming_pattern: "^(prod|staging|dev)\\.[a-z]+\\.[a-z]+$"
# Matches: prod.orders.raw, staging.users.events

# Team namespace pattern
naming_pattern: "^team-[a-z]+\\.[a-z-]+$"
# Matches: team-payments.transactions, team-fraud.alerts
```

---

## Model Rules

Enforce documentation and quality standards:

```yaml
rules:
  models:
    # Documentation requirements
    require_description: true
    require_owner: true
    min_description_length: 20

    # Testing requirements
    require_tests: true
    min_tests: 1

    # Complexity limits
    max_dependencies: 10
    max_sql_length: 5000

    # Allowed materializations
    allowed_materializations:
      - topic
      - flink
      - sink

    # Tags
    required_tags:
      - tier
    allowed_tags:
      - tier-1
      - tier-2
      - tier-3
      - critical
      - experimental
```

### Rule Reference

| Rule | Type | Description |
|------|------|-------------|
| `require_description` | bool | Models must have description |
| `require_owner` | bool | Models must have owner |
| `min_description_length` | int | Minimum description chars |
| `require_tests` | bool | Models must have tests |
| `min_tests` | int | Minimum test count |
| `max_dependencies` | int | Max upstream dependencies |
| `max_sql_length` | int | Max SQL character count |
| `allowed_materializations` | list | Allowed materialization types |
| `required_tags` | list | Tags that must be present |
| `allowed_tags` | list | Only these tags allowed |

---

## Source Rules

Ensure sources are well-documented:

```yaml
rules:
  sources:
    # Documentation
    require_description: true
    require_owner: true

    # Schema requirements
    require_schema: true
    require_columns: true

    # Freshness monitoring
    require_freshness: true
    max_freshness_warn: 10m
    max_freshness_error: 30m
```

### Rule Reference

| Rule | Type | Description |
|------|------|-------------|
| `require_description` | bool | Sources must have description |
| `require_owner` | bool | Sources must have owner |
| `require_schema` | bool | Must reference Schema Registry |
| `require_columns` | bool | Must document columns |
| `require_freshness` | bool | Must have freshness SLA |
| `max_freshness_warn` | duration | Max warn_after value |
| `max_freshness_error` | duration | Max error_after value |

---

## Security Rules

Enforce data protection policies:

```yaml
rules:
  security:
    # Classification requirements
    require_classification: true
    allowed_classifications:
      - public
      - internal
      - confidential
      - sensitive
      - highly_sensitive

    # Masking requirements
    sensitive_columns_require_masking: true
    highly_sensitive_columns_require_encryption: true

    # PII detection
    pii_column_patterns:
      - email
      - phone
      - ssn
      - credit_card
      - ip_address

    # Access control
    require_access_level: true
```

### Rule Reference

| Rule | Type | Description |
|------|------|-------------|
| `require_classification` | bool | Columns must have classification |
| `allowed_classifications` | list | Valid classification values |
| `sensitive_columns_require_masking` | bool | Sensitive data must be masked |
| `pii_column_patterns` | list | Column names that require PII treatment |
| `require_access_level` | bool | Models must specify access level |

### Classification Levels

| Level | Description | Typical Rules |
|-------|-------------|---------------|
| `public` | Open data | No restrictions |
| `internal` | Internal use | No external exposure |
| `confidential` | Business sensitive | Limited access |
| `sensitive` | PII, personal data | Masking required |
| `highly_sensitive` | Regulated (PCI, HIPAA) | Encryption + audit |

---

## Exposure Rules

Ensure downstream consumers are documented:

```yaml
rules:
  exposures:
    require_description: true
    require_owner: true
    require_sla: true
    require_contacts: true

    # SLA bounds
    max_latency_p99_ms: 10000
    min_availability: 99.0
```

---

## Test Rules

Ensure adequate test coverage:

```yaml
rules:
  tests:
    # Coverage requirements
    require_schema_tests: true
    min_assertions_per_test: 2

    # Continuous monitoring
    require_continuous_for_critical: true
```

---

## Validation Output

When rules are violated:

```bash
$ streamt validate

✗ Project 'my-pipeline' has validation errors

Errors:
  ✗ Model 'orders_raw_v2' violates topic naming pattern
    Expected: ^[a-z]+\.[a-z-]+\.v[0-9]+$
    Got: orders_raw_v2

  ✗ Model 'customer_metrics' missing required tests
    Rule: require_tests = true

Warnings:
  ⚠ Source 'events' missing freshness configuration
    Rule: require_freshness = true

  ⚠ Column 'email' in 'users' appears to be PII but lacks classification
    Rule: pii_column_patterns includes 'email'

Summary: 2 errors, 2 warnings
```

## Strict Mode

In strict mode, warnings become errors:

```bash
$ streamt validate --strict
```

Use in CI/CD to enforce all rules.

---

## Environment-Specific Rules

Use different rules per environment:

```yaml title="stream_project.yml"
rules:
  topics:
    min_partitions: ${TOPIC_MIN_PARTITIONS:-3}
    min_replication_factor: ${TOPIC_MIN_RF:-1}
```

```bash title="Production"
export TOPIC_MIN_PARTITIONS=6
export TOPIC_MIN_RF=3
streamt validate
```

```bash title="Development"
export TOPIC_MIN_PARTITIONS=1
export TOPIC_MIN_RF=1
streamt validate
```

---

## Best Practices

### 1. Start Permissive, Tighten Over Time

```yaml
# Week 1: Basic requirements
rules:
  models:
    require_description: true

# Month 1: Add testing requirements
rules:
  models:
    require_description: true
    require_tests: true

# Quarter 1: Full governance
rules:
  models:
    require_description: true
    require_tests: true
    require_owner: true
  security:
    require_classification: true
```

### 2. Document Rule Rationale

```yaml
rules:
  topics:
    # Minimum 6 partitions for adequate parallelism
    # across our 6-node Kafka cluster
    min_partitions: 6

    # Pattern: domain.entity.version
    # Examples: orders.created.v1, payments.processed.v2
    naming_pattern: "^[a-z]+\\.[a-z-]+\\.v[0-9]+$"
```

### 3. Use CI/CD Enforcement

```yaml title=".github/workflows/validate.yml"
- name: Validate streaming project
  run: |
    streamt validate --strict
```

### 4. Regular Audits

```bash
# Generate compliance report
streamt validate --format json > compliance-report.json

# Check for new violations
streamt validate --strict || notify_team "Governance violations found"
```
