---
title: Exposures
description: Document downstream consumers and data contracts
---

# Exposures

Exposures document how your data is consumed by downstream systems. They don't deploy anything — they provide lineage documentation, SLA tracking, and impact analysis.

## What is an Exposure?

An **exposure** represents:

- An application that consumes your data
- A dashboard that displays your metrics
- An ML model that trains on your streams
- Any downstream dependency

```yaml
exposures:
  - name: fraud_detection_service
    type: application
    role: consumer
    consumes:
      - ref: transactions_clean
```

## Why Document Exposures?

1. **Lineage** — See the full picture of where data goes
2. **Impact Analysis** — Know what breaks when you change a model
3. **SLA Tracking** — Document and monitor service agreements
4. **Ownership** — Know who to contact when things break

## Basic Exposure

```yaml title="exposures/services.yml"
exposures:
  - name: billing_service
    type: application
    description: Processes confirmed orders for invoicing
    owner: billing-team

    role: consumer
    consumer_group: billing-service-cg

    consumes:
      - ref: orders_confirmed
```

## Exposure Types

| Type | Description |
|------|-------------|
| `application` | Backend service or microservice |
| `dashboard` | Visualization or reporting |
| `ml_training` | ML model training pipeline |
| `ml_inference` | Real-time ML inference |
| `api` | External API endpoint |

## Complete Exposure Reference

```yaml
exposures:
  - name: customer_dashboard
    type: dashboard
    description: |
      Real-time customer analytics dashboard showing
      engagement metrics, revenue trends, and churn risk.

    # Ownership
    owner: analytics-team
    tags: [analytics, customer, kpi]

    # URL to the exposure
    url: https://grafana.company.com/d/customer-metrics

    # What it consumes
    role: consumer
    consumer_group: grafana-customer-dashboard

    consumes:
      - ref: customer_metrics
      - ref: customer_segments
      - source: user_events

    # What it produces (if bi-directional)
    produces:
      - ref: dashboard_events

    # SLA expectations
    sla:
      latency_p99_ms: 5000      # 5 second max latency
      availability: 99.9         # 99.9% uptime
      throughput_per_second: 50  # Expected consumption rate

    # Contacts
    contacts:
      - name: Jane Smith
        email: jane@company.com
      - name: Analytics Team
        slack: "#analytics-team"
```

## Roles

### Consumer

The exposure reads from your data:

```yaml
- name: reporting_service
  role: consumer
  consumer_group: reporting-cg
  consumes:
    - ref: daily_metrics
```

### Producer

The exposure writes to your pipeline (usually a source):

```yaml
- name: checkout_service
  role: producer
  produces:
    - source: orders_raw
```

### Both

The exposure both reads and writes:

```yaml
- name: enrichment_service
  role: both
  consumes:
    - ref: events_raw
  produces:
    - source: events_enriched
```

## SLA Configuration

Document service level expectations:

```yaml
sla:
  # Latency requirements
  latency_p50_ms: 100
  latency_p99_ms: 500

  # Availability
  availability: 99.95    # Percentage

  # Throughput
  throughput_per_second: 1000
  max_lag_seconds: 60

  # Data freshness
  freshness_seconds: 300
```

## Lineage Visualization

With exposures defined, `streamt lineage` shows the complete picture:

```bash
$ streamt lineage

orders_raw (source) → checkout_service (producer)
    └── orders_clean (topic)
            ├── order_metrics (flink)
            │       └── operations_dashboard (dashboard)
            ├── orders_warehouse (sink)
            └── billing_service (application)
```

## Impact Analysis

See what's affected by changes:

```bash
$ streamt lineage --model orders_clean --downstream

orders_clean (topic)
    ├── order_metrics (flink)
    │       └── operations_dashboard (dashboard)
    ├── orders_warehouse (sink)
    └── billing_service (application)

⚠️  3 downstream consumers will be affected
```

## Examples

### Application Exposure

```yaml
- name: fraud_detection_service
  type: application
  description: |
    Real-time fraud detection using ML models.
    Scores transactions within 100ms.
  owner: fraud-team
  url: https://wiki.company.com/fraud-service

  role: consumer
  consumer_group: fraud-detection-cg

  consumes:
    - ref: transactions_enriched

  sla:
    latency_p99_ms: 100
    throughput_per_second: 5000
    availability: 99.99

  contacts:
    - name: Fraud Team
      slack: "#fraud-alerts"
```

### Dashboard Exposure

```yaml
- name: executive_dashboard
  type: dashboard
  description: C-suite real-time business metrics
  owner: bi-team
  url: https://tableau.company.com/exec-dashboard

  role: consumer
  consumer_group: tableau-exec

  consumes:
    - ref: revenue_hourly
    - ref: customer_metrics
    - ref: product_performance

  sla:
    latency_p99_ms: 10000
    availability: 99.5
```

### ML Training Pipeline

```yaml
- name: recommendation_training
  type: ml_training
  description: |
    Trains recommendation models daily.
    Consumes 7 days of interaction data.
  owner: ml-platform

  role: consumer
  consumer_group: ml-training-recommendations

  consumes:
    - ref: user_interactions
    - ref: product_views
    - ref: purchase_history

  sla:
    throughput_per_second: 10000  # High throughput for backfill
```

### ML Inference

```yaml
- name: recommendation_api
  type: ml_inference
  description: Real-time product recommendations
  owner: ml-platform
  url: https://api.company.com/recommendations

  role: consumer
  consumer_group: recommendation-inference

  consumes:
    - ref: user_features
    - ref: product_features

  sla:
    latency_p99_ms: 50
    throughput_per_second: 2000
    availability: 99.9
```

### External API

```yaml
- name: partner_data_feed
  type: api
  description: Partner data syndication API
  owner: partnerships-team

  role: consumer
  consumer_group: partner-feed-cg

  consumes:
    - ref: product_catalog
    - ref: inventory_updates

  sla:
    latency_p99_ms: 1000
    availability: 99.5
    throughput_per_second: 100
```

## File Organization

Organize exposures by domain:

```
exposures/
├── services.yml        # Backend services
├── dashboards.yml      # Reporting dashboards
├── ml.yml              # ML systems
└── external.yml        # External consumers
```

Or by team:

```
exposures/
├── analytics-team.yml
├── fraud-team.yml
├── billing-team.yml
└── partners.yml
```

## Best Practices

### 1. Document All Consumers

Every consumer should have an exposure:

```yaml
# Don't leave mystery consumer groups
- name: unknown_consumer
  description: "Legacy checkout service — migrating to v2 in Q3"
  consumer_group: legacy-consumer-123
```

### 2. Include Contact Information

```yaml
contacts:
  - name: Primary Owner
    email: owner@company.com
    slack: "#team-channel"
  - name: Oncall
    pagerduty: team-oncall
```

### 3. Set Realistic SLAs

```yaml
# Based on actual requirements
sla:
  latency_p99_ms: 500     # Not aspirational, achievable
  availability: 99.9       # Match your infrastructure
```

### 4. Link to Documentation

```yaml
url: https://wiki.company.com/services/fraud-detection
```

### 5. Tag Appropriately

```yaml
tags: [critical, tier-1, pci-compliant]
```

## Validation

With governance rules, validate exposure completeness:

```yaml title="stream_project.yml"
rules:
  exposures:
    require_owner: true
    require_sla: true
    require_contacts: true
```

## Next Steps

- [DAG & Lineage](dag.md) — Visualize data flow
- [Governance Rules](../reference/governance.md) — Enforce exposure requirements
- [CLI Reference](../reference/cli.md) — Lineage commands
