# Architecture - Standalone vs Enhanced

## Core Principle

> **dbt-for-streaming fonctionne sur n'importe quel Kafka, sans dépendance à Conduktor.**
> Conduktor (Proxy, Console) apporte des fonctionnalités bonus.

## Two Modes

### Mode 1: Standalone (any Kafka)

Works with:
- Any Kafka (Confluent, AWS MSK, Aiven, Redpanda, self-hosted)
- Any Flink cluster
- Any Kafka Connect

Capabilities:
| Feature | How it works |
|---------|--------------|
| Sources | Declares existing topics |
| Models (stateless) | Creates **real intermediate topics** |
| Models (stateful) | Deploys Flink jobs via REST API |
| Models (sink) | Deploys Connect connectors via REST API |
| Tests | Runs via Flink/KStreams jobs |
| Security (RBAC) | Generates Kafka ACLs (if enabled) |
| Security (masking) | **Not available** (no Proxy) |
| Lineage | Computed from project, static |

### Mode 2: Enhanced (with Conduktor)

Adds on top of standalone:
| Feature | How it works |
|---------|--------------|
| Models (stateless) | **Virtual topics** via Proxy (no real topic) |
| Security (masking) | Applied at Proxy level |
| Security (RBAC) | Full Conduktor policies + Kafka ACLs |
| Lineage | Runtime lineage (who consumes what, lag, etc.) |
| Monitoring | Integrated with Console |

## Implications for DSL

The `materialized` type is **auto-inferred** from SQL patterns:

```yaml
models:
  # Simple SELECT → inferred as 'topic'
  - name: payments_clean
    sql: SELECT * FROM {{ source("payments_raw") }} WHERE status = 'CAPTURED'
    # materialized: topic (auto-inferred)

    advanced:
      topic:
        partitions: 12
    # ...

  # JOIN → inferred as 'flink'
  - name: payments_enriched
    sql: |
      SELECT p.*, c.risk_band
      FROM {{ ref("payments_clean") }} p
      JOIN {{ ref("customers") }} c ON p.customer_id = c.customer_id
    # materialized: flink (auto-inferred)

    advanced:
      flink:
        parallelism: 4
    # ...

  # Virtual topic (requires Gateway) - must be explicit
  - name: payments_masked
    sql: SELECT payment_id, amount FROM {{ source("payments_raw") }}

    advanced:
      materialized: virtual_topic  # Gateway required (explicit override)
      gateway:
        virtual_topic: true
    # ...
```

If `materialized: virtual_topic` but no Gateway configured → **compilation error** with helpful message.

## Configuration

```yaml
# stream_project.yml

project:
  name: fraud-detection

runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BOOTSTRAP}
    # ...

  schema_registry:           # Optional
    url: ${SR_URL}

  flink:                     # Optional, for stateful models
    rest_url: ${FLINK_REST_URL}

  connect:                   # Optional, for sinks
    rest_url: ${CONNECT_REST_URL}

  conduktor:                 # Optional, enables enhanced features
    proxy_url: ${PROXY_URL}
    console_url: ${CONSOLE_URL}
    api_key: ${CONDUKTOR_API_KEY}
```

## Open Questions

- [ ] Sans Proxy, le masking est-il simplement désactivé? Ou erreur si déclaré?
- [ ] Les tests continus nécessitent Flink. Sans Flink, que faire?
- [ ] Comment détecter automatiquement ce qui est disponible (Proxy, Flink, etc.)?
