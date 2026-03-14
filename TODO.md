# TODO

Unimplemented features and planned work.

## Production Readiness

- [ ] Flink savepoint handling — graceful upgrades without data loss; trigger savepoints on upgrade, restore from savepoint
- [ ] HTTP response validation — check status before `.json()` calls in deployers
- [ ] Input validation — Pydantic validators for URLs, topic names, bootstrap servers

## Core Features

- [ ] Test assertions — `foreign_key`, `max_lag`, `throughput`, `distribution`:
  ```yaml
  # Referential integrity across streams
  - foreign_key:
      column: customer_id
      ref_model: customers
      ref_key: id
      window: "1 HOUR"
      match_rate: 0.99

  # Event time lag monitoring (continuous only)
  - max_lag:
      column: event_timestamp
      max_seconds: 300

  # Message throughput bounds (continuous only)
  - throughput:
      min_per_second: 10
      max_per_second: 1000

  # Value distribution across buckets
  - distribution:
      column: amount
      buckets:
        - { min: 0, max: 100, expected_ratio: 0.4, tolerance: 0.1 }
        - { min: 100, max: 1000, expected_ratio: 0.5, tolerance: 0.1 }
        - { min: 1000, max: 10000, expected_ratio: 0.1, tolerance: 0.05 }
  ```
- [ ] Test failure handlers — `on_failure` actions: alert (Slack/PagerDuty), pause model, route to DLQ, block deployment
- [ ] Global credentials/connections — define Snowflake, S3, etc. once and reference everywhere
- [ ] `streamt build` — generate self-contained deployable artifacts (topics JSON, Flink SQL, connector configs, manifest with checksums) for debugging, auditing, air-gapped deployments
- [ ] `streamt diff` — standalone diff between local and deployed state (currently available as `streamt show --diff`)
- [ ] `streamt status --health` — health checks with configurable thresholds

## Flink Options

`state_backend` is parsed from YAML but not yet applied to Flink jobs (cluster config determines it).

Not yet implemented:

- [ ] Advanced checkpointing — `timeout_ms`, `min_pause_ms`, `max_concurrent`, `mode` (exactly_once/at_least_once), `externalized` (cleanup policy), `unaligned`, `incremental` (RocksDB)
- [ ] State backend advanced — RocksDB tuning (`block_cache_size_mb`, `write_buffer_size_mb`, `predefined_options`), incremental cleanup config
- [ ] Restart strategy — `fixed_delay` (attempts + delay), `failure_rate` (max failures per interval), `exponential_delay` (initial/max delay + multiplier)
- [ ] Resource configuration — task manager memory/CPU/slots, job manager memory/CPU
- [ ] Savepoint management — `savepoint.enabled`, `savepoint.path`, `savepoint.on_upgrade` (trigger_and_restore), `savepoint.on_cancel`
- [ ] Custom watermark expression — `event_time.watermark.strategy: custom` with user-defined SQL expression
- [ ] Kubernetes Flink operator — deploy via K8s CRDs instead of REST API; namespace, service account, image, pod template config
- [ ] Docker cluster type — local Docker deployment
- [ ] Changelog mode configuration — append, upsert, retract

## Governance Rules

- [ ] `max_replication_factor` — maximum replication factor enforcement
- [ ] `forbidden_suffixes` — disallowed topic name suffixes (complement to existing `forbidden_prefixes`)

## Data Governance

- [ ] Security policies — field-level encryption, `allowed_roles`, purpose-based access control, per-consumer column masking
- [ ] Exposure SLOs — `max_end_to_end_latency_ms`, `max_error_rate`, `freshness` contracts
- [ ] Exposure access control — `allowed_roles`, `purpose` metadata on exposures
- [ ] Data residency — region constraints (`region: EU`, `allowed_clusters`, `forbidden_sinks`)
- [ ] Schema versioning — v1/v2 model versions with compatibility checks and migration paths

## Operational

- [ ] Prometheus/OpenTelemetry integration — metrics and alerting
- [ ] CI/CD GitHub Actions templates — automation for deploy pipelines
- [ ] Curated connector library — tested configs for Postgres, Snowflake, S3

## Vision

- [ ] Semantic layer / Streaming API — declarative consumption API ("give me flux X with max 30s delay")
- [ ] Model templates / packages — reusable pipeline patterns (CDC-to-enriched, sessionization)
- [ ] External app support — register blackbox applications (Java, Go) with input/output models for lineage
- [ ] High-level intent mode — "I want X" and streamt builds the entire pipeline
- [ ] KStreams runtime — `materialized: kstreams` for users without Flink
- [ ] RisingWave runtime — streaming SQL database alternative to Flink
- [ ] Materialize runtime — incremental view maintenance

## Deferred

- [ ] VS Code extension
- [ ] Additional streaming substrates (Pulsar, Kinesis)
- [ ] Cloud/SaaS version
